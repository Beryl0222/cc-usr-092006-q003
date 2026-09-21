package settlement

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

func mustEngine(t *testing.T, now time.Time) *Engine {
	t.Helper()
	programs, err := ReadPrograms("data/programs.json")
	if err != nil {
		t.Fatal(err)
	}
	e, err := NewEngine(programs, []byte("acceptance-secret"), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func makeEvent(t *testing.T, id string, typ EventType, at string, payload any) InboundEvent {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, at)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return InboundEvent{ID: id, Type: typ, OccurredAt: ts, Payload: raw}
}

func mustApply(t *testing.T, e *Engine, ev InboundEvent) EventResult {
	t.Helper()
	res := e.Ingest(ev)
	if res.Status != "applied" {
		t.Fatalf("事件 %s 未受理：%s %s", ev.ID, res.Status, res.Reason)
	}
	return res
}

func seedBenefit(t *testing.T, e *Engine, ticketToken, matchID, benefitID, program string, quota Money, at string) {
	t.Helper()
	mustApply(t, e, makeEvent(t, "tk-"+ticketToken, EventTicket, at,
		TicketPayload{TicketToken: ticketToken, EventID: matchID, Status: "issued"}))
	mustApply(t, e, makeEvent(t, "gr-"+benefitID, EventGrant, at,
		GrantPayload{BenefitID: benefitID, ProgramCode: program, TicketToken: ticketToken, Quota: quota}))
}

func mustPosting(t *testing.T, e *Engine, id string) Posting {
	t.Helper()
	p, ok := e.Posting(id)
	if !ok {
		t.Fatalf("分录 %s 不存在", id)
	}
	return p
}

func lineOf(t *testing.T, p Posting, account string) Line {
	t.Helper()
	for _, l := range p.Lines {
		if l.Account == account {
			return l
		}
	}
	t.Fatalf("分录 %s 缺少账户行 %s", p.ID, account)
	return Line{}
}

var testNow = time.Date(2026, 10, 2, 10, 0, 0, 0, time.FixedZone("CST", 8*3600))

// 跨日赛程：商户跨午夜上传的流水按业务发生时间归入正确场次与阶段。
func TestCrossMidnightAttribution(t *testing.T) {
	e := mustEngine(t, testNow)
	seedBenefit(t, e, "T-A1", "match-2026-09-18", "B-A1", "city-stay-a", 100000, "2026-09-17T13:00:00+08:00")

	// 9 月 20 日 01:30（+08:00）的核销属于 9 月 18 日场次的延时阶段，而非 9 月 20 日场次。
	res := mustApply(t, e, makeEvent(t, "rd-A1", EventRedeem, "2026-09-20T01:30:00+08:00",
		RedeemPayload{BenefitID: "B-A1", MerchantID: "M-hotel", Amount: 50000}))
	p := mustPosting(t, e, res.Postings[0])
	if p.MatchID != "match-2026-09-18" || p.ProgramCode != "city-stay-a" || p.Phase != "overtime" {
		t.Fatalf("跨午夜流水归因错误：%+v", p)
	}
	if p.Period != "2026-09" {
		t.Fatalf("账期错误：%s", p.Period)
	}
	if got := lineOf(t, p, "payable:merchant:M-hotel").Credit; got != 10000 {
		t.Fatalf("补贴金额错误：%d", got)
	}
	if got := lineOf(t, p, "receivable:partner:match-2026-09-18").Debit; got != 5000 {
		t.Fatalf("合作方分摊错误：%d", got)
	}
	if got := lineOf(t, p, "expense:organizer:city-stay-a").Debit; got != 5000 {
		t.Fatalf("主办方承担错误：%d", got)
	}
	// city-stay-a 延时策略为 keep：只有一笔正常分录，无冲正。
	if len(res.Postings) != 1 {
		t.Fatalf("延时保留方案不应产生冲正：%v", res.Postings)
	}

	// 同一时刻不属于尚未开窗的 9 月 20 日场次。
	seedBenefit(t, e, "T-B1", "match-2026-09-20", "B-B1", "night-market-b", 100000, "2026-09-20T17:00:00+08:00")
	early := e.Ingest(makeEvent(t, "rd-B0", EventRedeem, "2026-09-20T01:30:00+08:00",
		RedeemPayload{BenefitID: "B-B1", MerchantID: "M-food", Amount: 8000}))
	if early.Status != "rejected" || early.Reason != "outside_window" {
		t.Fatalf("窗口外流水应被拒绝：%+v", early)
	}

	// 9 月 21 日 01:00 的核销归入 9 月 20 日场次的延时阶段；该方案延时策略为 reverse，自动冲正。
	res = mustApply(t, e, makeEvent(t, "rd-B1", EventRedeem, "2026-09-21T01:00:00+08:00",
		RedeemPayload{BenefitID: "B-B1", MerchantID: "M-food", Amount: 8000}))
	if len(res.Postings) != 2 {
		t.Fatalf("延时冲正方案应产生正常+冲正两笔分录：%v", res.Postings)
	}
	std := mustPosting(t, e, res.Postings[0])
	rev := mustPosting(t, e, res.Postings[1])
	if std.MatchID != "match-2026-09-20" || std.Phase != "overtime" {
		t.Fatalf("跨午夜流水归因错误：%+v", std)
	}
	if rev.Kind != "reversal" || rev.Reverses != std.ID || rev.Reason != "post_event_policy" {
		t.Fatalf("延时冲正分录错误：%+v", rev)
	}
	if got := lineOf(t, rev, "payable:merchant:M-food").Debit; got != 800 {
		t.Fatalf("延时冲正金额错误：%d", got)
	}
}

// 部分退款：按退款比例冲正补贴，退清时消除取整尾差。
func TestPartialRefundProratesSubsidy(t *testing.T) {
	e := mustEngine(t, testNow)
	seedBenefit(t, e, "T-A2", "match-2026-09-18", "B-A2", "city-stay-a", 100000, "2026-09-18T10:00:00+08:00")
	res := mustApply(t, e, makeEvent(t, "rd-A2", EventRedeem, "2026-09-18T20:00:00+08:00",
		RedeemPayload{BenefitID: "B-A2", MerchantID: "M-hotel", Amount: 10000}))
	std := mustPosting(t, e, res.Postings[0])
	if got := lineOf(t, std, "payable:merchant:M-hotel").Credit; got != 2000 {
		t.Fatalf("补贴金额错误：%d", got)
	}

	// 退 2500/10000 → 冲正补贴 500，其中合作方 250。
	res = mustApply(t, e, makeEvent(t, "rf-1", EventRefund, "2026-09-19T09:00:00+08:00",
		RefundPayload{RedemptionEventID: "rd-A2", Amount: 2500}))
	rev := mustPosting(t, e, res.Postings[0])
	if rev.Kind != "reversal" || rev.Reverses != std.ID || rev.Reason != "refund" {
		t.Fatalf("退款冲正分录错误：%+v", rev)
	}
	if got := lineOf(t, rev, "payable:merchant:M-hotel").Debit; got != 500 {
		t.Fatalf("部分退款冲正金额错误：%d", got)
	}
	if got := lineOf(t, rev, "receivable:partner:match-2026-09-18").Credit; got != 250 {
		t.Fatalf("部分退款合作方冲正错误：%d", got)
	}

	// 超额退款被拒绝。
	if r := e.Ingest(makeEvent(t, "rf-2", EventRefund, "2026-09-19T10:00:00+08:00",
		RefundPayload{RedemptionEventID: "rd-A2", Amount: 8000})); r.Status != "rejected" || r.Reason != "refund_exceeds" {
		t.Fatalf("超额退款应被拒绝：%+v", r)
	}

	// 退清剩余 7500 → 冲正剩余全部补贴 1500，合计冲正 2000，无尾差。
	res = mustApply(t, e, makeEvent(t, "rf-3", EventRefund, "2026-09-19T11:00:00+08:00",
		RefundPayload{RedemptionEventID: "rd-A2", Amount: 7500}))
	rev = mustPosting(t, e, res.Postings[0])
	if got := lineOf(t, rev, "payable:merchant:M-hotel").Debit; got != 1500 {
		t.Fatalf("退清冲正金额错误：%d", got)
	}
	lines, err := e.LedgerView(Scope{Role: RoleFinance}, "2026-09")
	if err != nil {
		t.Fatal(err)
	}
	var net Money
	for _, l := range lines {
		if l.Account == "payable:merchant:M-hotel" {
			net += l.Credit - l.Debit
		}
	}
	if net != 0 {
		t.Fatalf("退清后商户应付应为零：%d", net)
	}
}

// 并发核销：同一事件并发投递只入账一次；权益额度在并发下不超发。
func TestConcurrentRedemption(t *testing.T) {
	e := mustEngine(t, testNow)
	seedBenefit(t, e, "T-C1", "match-2026-09-18", "B-C1", "city-stay-a", 100000, "2026-09-18T10:00:00+08:00")

	dup := makeEvent(t, "rd-C1", EventRedeem, "2026-09-18T20:00:00+08:00",
		RedeemPayload{BenefitID: "B-C1", MerchantID: "M-hotel", Amount: 5000})
	const n = 16
	results := make([]EventResult, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = e.Ingest(dup)
		}(i)
	}
	wg.Wait()
	var applied, duplicated int
	for _, r := range results {
		switch r.Status {
		case "applied":
			applied++
		case "duplicate":
			duplicated++
		}
	}
	if applied != 1 || duplicated != n-1 {
		t.Fatalf("并发重复投递应只入账一次：applied=%d duplicate=%d", applied, duplicated)
	}

	// 额度 10000，两笔各 6000 并发核销：恰好一笔成功，一笔因额度不足被拒绝。
	seedBenefit(t, e, "T-C2", "match-2026-09-18", "B-C2", "city-stay-a", 10000, "2026-09-18T10:00:00+08:00")
	race := []InboundEvent{
		makeEvent(t, "rd-C2a", EventRedeem, "2026-09-18T21:00:00+08:00", RedeemPayload{BenefitID: "B-C2", MerchantID: "M-a", Amount: 6000}),
		makeEvent(t, "rd-C2b", EventRedeem, "2026-09-18T21:00:00+08:00", RedeemPayload{BenefitID: "B-C2", MerchantID: "M-b", Amount: 6000}),
	}
	raceResults := make([]EventResult, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			raceResults[i] = e.Ingest(race[i])
		}(i)
	}
	wg.Wait()
	var okCount, quotaRejected int
	for _, r := range raceResults {
		if r.Status == "applied" {
			okCount++
		}
		if r.Status == "rejected" && r.Reason == "quota_exceeded" {
			quotaRejected++
		}
	}
	if okCount != 1 || quotaRejected != 1 {
		t.Fatalf("并发核销应恰好一笔入账一笔拒绝：%+v", raceResults)
	}
}

// 退票后核销：退票事件到达后，该票已核销的补贴全额冲正，后续核销被拒绝。
func TestTicketRefundReversesSubsidy(t *testing.T) {
	e := mustEngine(t, testNow)
	seedBenefit(t, e, "T-D1", "match-2026-09-18", "B-D1", "city-stay-a", 100000, "2026-09-18T10:00:00+08:00")
	mustApply(t, e, makeEvent(t, "rd-D1", EventRedeem, "2026-09-18T20:00:00+08:00",
		RedeemPayload{BenefitID: "B-D1", MerchantID: "M-hotel", Amount: 40000}))

	res := mustApply(t, e, makeEvent(t, "tk-D1-rf", EventTicket, "2026-09-19T08:00:00+08:00",
		TicketPayload{TicketToken: "T-D1", EventID: "match-2026-09-18", Status: "refunded"}))
	if len(res.Postings) != 1 {
		t.Fatalf("退票应冲正一笔分录：%v", res.Postings)
	}
	rev := mustPosting(t, e, res.Postings[0])
	if rev.Kind != "reversal" || rev.Reason != "ticket_refunded" {
		t.Fatalf("退票冲正分录错误：%+v", rev)
	}
	if got := lineOf(t, rev, "payable:merchant:M-hotel").Debit; got != 8000 {
		t.Fatalf("退票冲正金额错误：%d", got)
	}

	// 重复投递同一退票事件不产生二次冲正。
	before, err := e.LedgerView(Scope{Role: RoleFinance}, "")
	if err != nil {
		t.Fatal(err)
	}
	dup := e.Ingest(makeEvent(t, "tk-D1-rf", EventTicket, "2026-09-19T08:00:00+08:00",
		TicketPayload{TicketToken: "T-D1", EventID: "match-2026-09-18", Status: "refunded"}))
	if dup.Status != "duplicate" {
		t.Fatalf("重复退票事件应去重：%+v", dup)
	}
	after, _ := e.LedgerView(Scope{Role: RoleFinance}, "")
	if len(before) != len(after) {
		t.Fatalf("重复事件不应产生新分录")
	}

	// 退票后的继续核销被拒绝。
	if r := e.Ingest(makeEvent(t, "rd-D2", EventRedeem, "2026-09-19T09:00:00+08:00",
		RedeemPayload{BenefitID: "B-D1", MerchantID: "M-hotel", Amount: 1000})); r.Status != "rejected" || r.Reason != "ticket_void" {
		t.Fatalf("退票后核销应被拒绝：%+v", r)
	}
}

// 改期与取消按各方案规则冲正或保留。
func TestRescheduleAndCancelPolicies(t *testing.T) {
	e := mustEngine(t, testNow)

	// night-market-b 改期策略为 reverse：改期即冲正。
	seedBenefit(t, e, "T-E1", "match-2026-09-20", "B-E1", "night-market-b", 100000, "2026-09-20T17:00:00+08:00")
	mustApply(t, e, makeEvent(t, "rd-E1", EventRedeem, "2026-09-20T20:00:00+08:00",
		RedeemPayload{BenefitID: "B-E1", MerchantID: "M-food", Amount: 20000}))
	res := mustApply(t, e, makeEvent(t, "tk-E1-rs", EventTicket, "2026-09-20T21:00:00+08:00",
		TicketPayload{TicketToken: "T-E1", EventID: "match-2026-09-20", Status: "rescheduled", NewEventID: "match-2026-09-27"}))
	if len(res.Postings) != 1 {
		t.Fatalf("改期冲正方案应产生一笔冲正：%v", res.Postings)
	}
	if rev := mustPosting(t, e, res.Postings[0]); rev.Reason != "ticket_rescheduled" {
		t.Fatalf("改期冲正原因错误：%+v", rev)
	}

	// city-stay-a 改期策略为 keep：改期保留补贴，但原场次权益不可再核销。
	seedBenefit(t, e, "T-E2", "match-2026-09-18", "B-E2", "city-stay-a", 100000, "2026-09-18T10:00:00+08:00")
	mustApply(t, e, makeEvent(t, "rd-E2", EventRedeem, "2026-09-18T20:00:00+08:00",
		RedeemPayload{BenefitID: "B-E2", MerchantID: "M-hotel", Amount: 10000}))
	res = mustApply(t, e, makeEvent(t, "tk-E2-rs", EventTicket, "2026-09-18T22:00:00+08:00",
		TicketPayload{TicketToken: "T-E2", EventID: "match-2026-09-18", Status: "rescheduled", NewEventID: "match-2026-09-25"}))
	if len(res.Postings) != 0 {
		t.Fatalf("改期保留方案不应产生冲正：%v", res.Postings)
	}
	if r := e.Ingest(makeEvent(t, "rd-E2b", EventRedeem, "2026-09-18T23:00:00+08:00",
		RedeemPayload{BenefitID: "B-E2", MerchantID: "M-hotel", Amount: 1000})); r.Status != "rejected" || r.Reason != "ticket_moved" {
		t.Fatalf("改期后原场次核销应被拒绝：%+v", r)
	}

	// 取消：全额冲正。
	seedBenefit(t, e, "T-E3", "match-2026-09-18", "B-E3", "city-stay-a", 100000, "2026-09-18T10:00:00+08:00")
	mustApply(t, e, makeEvent(t, "rd-E3", EventRedeem, "2026-09-18T20:30:00+08:00",
		RedeemPayload{BenefitID: "B-E3", MerchantID: "M-hotel", Amount: 10000}))
	res = mustApply(t, e, makeEvent(t, "tk-E3-cx", EventTicket, "2026-09-18T23:30:00+08:00",
		TicketPayload{TicketToken: "T-E3", EventID: "match-2026-09-18", Status: "cancelled"}))
	if len(res.Postings) != 1 {
		t.Fatalf("取消应产生一笔冲正：%v", res.Postings)
	}
	if rev := mustPosting(t, e, res.Postings[0]); rev.Reason != "ticket_cancelled" {
		t.Fatalf("取消冲正原因错误：%+v", rev)
	}
}

// 封账后补传：迟到流水以调整分录落入当前账期，已封账快照保持不变且可复算。
func TestCloseSnapshotAndLateUpload(t *testing.T) {
	e := mustEngine(t, testNow)
	seedBenefit(t, e, "T-F1", "match-2026-09-18", "B-F1", "city-stay-a", 100000, "2026-09-18T10:00:00+08:00")
	mustApply(t, e, makeEvent(t, "rd-F1", EventRedeem, "2026-09-18T21:00:00+08:00",
		RedeemPayload{BenefitID: "B-F1", MerchantID: "M-hotel", Amount: 30000}))

	snap, err := e.ClosePeriod("2026-09")
	if err != nil {
		t.Fatal(err)
	}
	if snap.Totals["receivable:partner:match-2026-09-18"] != 3000 ||
		snap.Totals["expense:organizer:city-stay-a"] != 3000 ||
		snap.Totals["payable:merchant:M-hotel"] != -6000 {
		t.Fatalf("快照金额错误：%+v", snap.Totals)
	}
	if _, _, match, err := e.VerifySnapshot("2026-09"); err != nil || !match {
		t.Fatalf("封账后应立即复算一致：match=%v err=%v", match, err)
	}

	// 迟到流水（发生于已封账账期）补传：转为调整分录落入 2026-10。
	res := mustApply(t, e, makeEvent(t, "rd-F2", EventRedeem, "2026-09-19T10:00:00+08:00",
		RedeemPayload{BenefitID: "B-F1", MerchantID: "M-hotel", Amount: 12000}))
	late := mustPosting(t, e, res.Postings[0])
	if late.Kind != "adjustment" || late.Period != "2026-10" {
		t.Fatalf("封账后补传应生成下一账期调整分录：%+v", late)
	}
	if !strings.Contains(late.Reason, "补入已封账账期2026-09") {
		t.Fatalf("调整分录应标注原账期：%s", late.Reason)
	}

	// 已封账快照不被改写，复算仍然一致。
	again, ok := e.Snapshot("2026-09")
	if !ok || again.Hash != snap.Hash {
		t.Fatalf("封账快照被改写：%+v", again)
	}
	if _, _, match, err := e.VerifySnapshot("2026-09"); err != nil || !match {
		t.Fatalf("补传后复算应仍一致：match=%v err=%v", match, err)
	}

	// 重复封账幂等返回同一快照。
	resnap, err := e.ClosePeriod("2026-09")
	if err != nil || resnap.Hash != snap.Hash {
		t.Fatalf("重复封账应幂等：%+v", resnap)
	}
}

// 数据隔离：财务、主办方、商户各自只能看到对应范围。
func TestScopedViews(t *testing.T) {
	e := mustEngine(t, testNow)
	seedBenefit(t, e, "T-G1", "match-2026-09-18", "B-G1", "city-stay-a", 100000, "2026-09-18T10:00:00+08:00")
	mustApply(t, e, makeEvent(t, "rd-G1", EventRedeem, "2026-09-18T20:00:00+08:00",
		RedeemPayload{BenefitID: "B-G1", MerchantID: "M-hotel", Amount: 10000}))
	seedBenefit(t, e, "T-G2", "match-2026-09-20", "B-G2", "night-market-b", 100000, "2026-09-20T17:00:00+08:00")
	mustApply(t, e, makeEvent(t, "rd-G2", EventRedeem, "2026-09-20T20:00:00+08:00",
		RedeemPayload{BenefitID: "B-G2", MerchantID: "M-food", Amount: 20000}))

	all, err := e.LedgerView(Scope{Role: RoleFinance}, "")
	if err != nil || len(all) != 6 {
		t.Fatalf("财务应见全部分录行：%d err=%v", len(all), err)
	}

	hotel, err := e.LedgerView(Scope{Role: RoleMerchant, MerchantID: "M-hotel"}, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range hotel {
		if l.Account != "payable:merchant:M-hotel" {
			t.Fatalf("商户越权看到他人账户行：%+v", l)
		}
	}
	if len(hotel) != 1 {
		t.Fatalf("商户仅应见自身应付款行：%d", len(hotel))
	}

	org, err := e.LedgerView(Scope{Role: RoleOrganizer, MatchID: "match-2026-09-18"}, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range org {
		if l.MatchID != "match-2026-09-18" {
			t.Fatalf("主办方越权看到其他场次：%+v", l)
		}
	}
	if _, err := e.LedgerView(Scope{Role: RoleOrganizer}, ""); err != ErrForbidden {
		t.Fatalf("缺少场次的主办方应被拒绝：%v", err)
	}
	if _, err := e.LedgerView(Scope{Role: "unknown"}, ""); err != ErrForbidden {
		t.Fatalf("未知角色应被拒绝：%v", err)
	}

	rows, err := e.Consumption(Scope{Role: RoleOrganizer, MatchID: "match-2026-09-18"}, "match-2026-09-18")
	if err != nil || len(rows) != 1 || rows[0].Gross != 10000 || rows[0].Subsidy != 2000 {
		t.Fatalf("带动消费聚合错误：%+v err=%v", rows, err)
	}
	if _, err := e.Consumption(Scope{Role: RoleOrganizer, MatchID: "match-2026-09-18"}, "match-2026-09-20"); err != ErrForbidden {
		t.Fatalf("主办方不应查看他人场次：%v", err)
	}
}

// 审计回溯：分录可回溯到脱敏原始事件、规则版本与冲正链。
func TestAuditExplanation(t *testing.T) {
	e := mustEngine(t, testNow)
	seedBenefit(t, e, "T-H1", "match-2026-09-18", "B-H1", "city-stay-a", 100000, "2026-09-18T10:00:00+08:00")
	res := mustApply(t, e, makeEvent(t, "rd-H1", EventRedeem, "2026-09-18T20:00:00+08:00",
		RedeemPayload{BenefitID: "B-H1", MerchantID: "M-hotel", Amount: 10000}))
	stdID := res.Postings[0]
	res = mustApply(t, e, makeEvent(t, "rf-H1", EventRefund, "2026-09-19T09:00:00+08:00",
		RefundPayload{RedemptionEventID: "rd-H1", Amount: 2500}))
	revID := res.Postings[0]

	ex, err := e.Explain(revID)
	if err != nil {
		t.Fatal(err)
	}
	if ex.Posting.RuleVersion != "v2026.09-a" {
		t.Fatalf("审计缺少规则版本：%+v", ex.Posting)
	}
	if len(ex.Chain) != 1 || ex.Chain[0].ID != stdID {
		t.Fatalf("冲正链应包含原分录：%+v", ex.Chain)
	}
	if ex.SourceEvent == nil || ex.SourceEvent.ID != "rf-H1" {
		t.Fatalf("审计缺少原始事件：%+v", ex.SourceEvent)
	}

	back, err := e.Explain(stdID)
	if err != nil {
		t.Fatal(err)
	}
	if len(back.Chain) != 1 || back.Chain[0].ID != revID {
		t.Fatalf("原分录应能查到冲正分录：%+v", back.Chain)
	}

	// 落盘的原始事件已完成身份脱敏：只有伪匿名引用，无明文标识。
	se, ok := e.StoredEvent("gr-B-H1")
	if !ok {
		t.Fatal("原始事件未落盘")
	}
	if strings.Contains(string(se.Payload), "ticket_token") || strings.Contains(string(se.Payload), "T-H1") {
		t.Fatalf("原始事件不应保存明文身份：%s", se.Payload)
	}
	if !strings.Contains(string(se.Payload), "ticket_ref") {
		t.Fatalf("原始事件应保留伪匿名引用：%s", se.Payload)
	}
}

// 封账后只能通过调整分录修正，且调整分录落入未封账账期。
func TestManualAdjustmentAfterClose(t *testing.T) {
	e := mustEngine(t, testNow)
	seedBenefit(t, e, "T-J1", "match-2026-09-18", "B-J1", "city-stay-a", 100000, "2026-09-18T10:00:00+08:00")
	res := mustApply(t, e, makeEvent(t, "rd-J1", EventRedeem, "2026-09-18T20:00:00+08:00",
		RedeemPayload{BenefitID: "B-J1", MerchantID: "M-hotel", Amount: 10000}))
	stdID := res.Postings[0]
	snap, err := e.ClosePeriod("2026-09")
	if err != nil {
		t.Fatal(err)
	}

	// 借贷不平衡被拒绝。
	if _, err := e.AddAdjustment(AdjustmentInput{
		Reason: "不平衡",
		Refs:   []string{stdID},
		Lines:  []Line{{Account: "expense:organizer:city-stay-a", Debit: 100}},
	}); err == nil {
		t.Fatal("借贷不平衡的调整分录应被拒绝")
	}

	// 引用未封账账期分录被拒绝（封账后补传的迟到流水落在未封账的 2026-10）。
	open := mustApply(t, e, makeEvent(t, "rd-J2", EventRedeem, "2026-09-19T10:00:00+08:00",
		RedeemPayload{BenefitID: "B-J1", MerchantID: "M-hotel", Amount: 1000}))
	if p := mustPosting(t, e, open.Postings[0]); p.Period != "2026-10" {
		t.Fatalf("迟到流水应落入未封账账期：%+v", p)
	}
	if _, err := e.AddAdjustment(AdjustmentInput{
		Reason: "引用未封账分录",
		Refs:   []string{open.Postings[0]},
		Lines: []Line{
			{Account: "expense:organizer:city-stay-a", Debit: 100},
			{Account: "payable:merchant:M-hotel", Credit: 100},
		},
	}); err == nil {
		t.Fatal("引用未封账账期分录的调整应被拒绝")
	}

	// 合法调整：补提补贴，落入当前未封账账期 2026-10。
	adj, err := e.AddAdjustment(AdjustmentInput{
		Reason: "商户对账差异补提",
		Refs:   []string{stdID},
		Lines: []Line{
			{Account: "expense:organizer:city-stay-a", Debit: 100},
			{Account: "payable:merchant:M-hotel", Credit: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if adj.Kind != "adjustment" || adj.Period != "2026-10" {
		t.Fatalf("调整分录应落入未封账账期：%+v", adj)
	}

	// 已封账快照不受影响，复算一致。
	if _, _, match, err := e.VerifySnapshot("2026-09"); err != nil || !match {
		t.Fatalf("调整后复算应仍一致：match=%v err=%v", match, err)
	}
	if again, _ := e.Snapshot("2026-09"); again.Hash != snap.Hash {
		t.Fatalf("封账快照被调整分录改写")
	}
}
