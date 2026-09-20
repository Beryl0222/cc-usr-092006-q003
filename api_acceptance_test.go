package settlement

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// apiHarness 封装内存服务与固定时钟。
type apiHarness struct {
	t   *testing.T
	svc *Service
	now time.Time
}

func newAPIHarness(t *testing.T) *apiHarness {
	st := testStore(t)
	svc := NewService(st)
	h := &apiHarness{t: t, svc: svc, now: mustTimeT(t, "2026-09-21T12:00:00+08:00")}
	svc.SetClock(func() time.Time { return h.now })
	return h
}

func testStore(t *testing.T) *Store {
	t.Helper()
	programs, err := ReadPrograms("data/programs.json")
	if err != nil {
		t.Fatal(err)
	}
	matches, err := ReadMatches("data/matches.json")
	if err != nil {
		t.Fatal(err)
	}
	rules, err := ReadRules("data/rules.json")
	if err != nil {
		t.Fatal(err)
	}
	cat, err := BuildCatalog(programs, matches, rules)
	if err != nil {
		t.Fatal(err)
	}
	st, err := NewStore(cat)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func mustTimeT(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return tm
}

func (h *apiHarness) do(method, path, token string, body any) (int, map[string]any) {
	h.t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rdr = bytes.NewReader(raw)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rdr)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.svc.Routes().ServeHTTP(rec, req)
	var out map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			h.t.Fatalf("响应不是 JSON：%s", rec.Body.String())
		}
	}
	return rec.Code, out
}

func (h *apiHarness) ingestToken(token, id, typ string, at time.Time, payload any) (int, map[string]any) {
	h.t.Helper()
	raw, _ := json.Marshal(payload)
	env := map[string]any{"event_id": id, "type": typ, "occurred_at": at.Format(time.RFC3339), "payload": json.RawMessage(raw)}
	return h.do(http.MethodPost, "/v1/events", token, env)
}

func (h *apiHarness) ingestReceived(id, typ string, at, received time.Time, payload any) (int, map[string]any) {
	h.t.Helper()
	raw, _ := json.Marshal(payload)
	env := map[string]any{
		"event_id": id, "type": typ,
		"occurred_at": at.Format(time.RFC3339), "received_at": received.Format(time.RFC3339),
		"payload": json.RawMessage(raw),
	}
	return h.do(http.MethodPost, "/v1/events", "platform", env)
}

func (h *apiHarness) ingest(id, typ string, at time.Time, payload any) map[string]any {
	h.t.Helper()
	code, out := h.ingestToken("platform", id, typ, at, payload)
	if code != http.StatusOK {
		h.t.Fatalf("ingest %s 失败 code=%d body=%v", id, code, out)
	}
	return out
}

func (h *apiHarness) getRedemption(token, id string) (int, RedemptionView) {
	h.t.Helper()
	code, out := h.do(http.MethodGet, "/v1/redemptions/"+id, token, nil)
	var v RedemptionView
	if code == http.StatusOK {
		raw, _ := json.Marshal(out)
		if err := json.Unmarshal(raw, &v); err != nil {
			h.t.Fatal(err)
		}
	}
	return code, v
}

// ---------- 验收场景 1：跨日赛程与跨午夜上传 ----------

func TestAcceptanceCrossMidnightUpload(t *testing.T) {
	h := newAPIHarness(t)
	// 0920 赛事 19:30-21:45；夜市权益窗口 16:00 至次日 03:00。
	consumed := mustTimeT(t, "2026-09-21T00:30:00+08:00") // 跨午夜消费
	uploaded := mustTimeT(t, "2026-09-21T04:30:00+08:00") // 商户清晨才上传（窗口 03:00 + 120min 宽限内）

	h.ingest("t20", EvTicketIssued, consumed, TicketIssued{TicketNo: "TK20", MatchID: "match-2026-09-20", FaceMin: 30000, Currency: "CNY"})
	h.ingest("g20", EvBenefitGranted, consumed, BenefitGranted{GrantID: "GR20", TicketNo: "TK20", ProgramCode: "night-market-b", IssuedAt: consumed, SingleUse: true})
	// night-market-b 不要求实名入场，直接核销 80 元，补贴 15% = 1200 分。
	env := map[string]any{
		"event_id": "r20", "type": EvRedemptionRecorded,
		"occurred_at": consumed.Format(time.RFC3339),
		"payload": map[string]any{
			"redemption_id": "RD20", "merchant_id": "M-NIGHT", "program_code": "night-market-b",
			"grant_id": "GR20", "amount_min": 8000, "currency": "CNY",
		},
	}
	// 直接用原生 map 构造信封，商户在清晨补传凌晨流水。
	env["received_at"] = uploaded.Format(time.RFC3339)
	raw, _ := json.Marshal(env)
	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer merchant:M-NIGHT")
	rec := httptest.NewRecorder()
	h.svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("跨午夜核销应接受：%s", rec.Body.String())
	}

	code, v := h.getRedemption("platform", "RD20")
	if code != 200 {
		t.Fatalf("查询核销失败：%d", code)
	}
	if v.MatchID != "match-2026-09-20" || v.Stage != StagePostMatch || v.SubsidyMin != 1200 {
		t.Fatalf("跨午夜归因错误：%+v", v)
	}
	// 账期按消费时间落在 2026-09，而不是上传时间的 09 月——两者同月，
	// 再验证一笔 10-01 00:30（窗口外）的流水被拒，证明不是"上传日归场次"。
	other := consumed.AddDate(0, 0, 10)
	code, out := h.ingestToken("platform", "r21", EvRedemptionRecorded, other, RedemptionRecorded{
		RedemptionID: "RD21", MerchantID: "M-NIGHT", ProgramCode: "night-market-b",
		GrantID: "GR20", AmountMin: 8000, Currency: "CNY",
	})
	if code != 200 || out["outcome"] != "rejected" {
		t.Fatalf("窗口外流水必须拒绝：%v", out)
	}
}

// ---------- 验收场景 2：部分退款 ----------

func TestAcceptancePartialRefund(t *testing.T) {
	h := newAPIHarness(t)
	at := mustTimeT(t, "2026-09-18T20:00:00+08:00")
	h.ingest("t1", EvTicketIssued, at, TicketIssued{TicketNo: "TK1", MatchID: "match-2026-09-18", FaceMin: 50000, Currency: "CNY"})
	h.ingest("g1", EvBenefitGranted, at, BenefitGranted{GrantID: "GR1", TicketNo: "TK1", ProgramCode: "city-stay-a", IssuedAt: at, SingleUse: true})
	h.ingest("a1", EvAccessVerified, at, AccessVerified{TicketNo: "TK1", MatchID: "match-2026-09-18", VerifiedAt: at})
	h.ingest("r1", EvRedemptionRecorded, at, RedemptionRecorded{
		RedemptionID: "RD1", MerchantID: "M1", ProgramCode: "city-stay-a",
		GrantID: "GR1", AmountMin: 10000, Currency: "CNY",
	})

	// 第一次退 30%，第二次退 20%（累计 50%）。
	r1 := mustTimeT(t, "2026-09-19T09:00:00+08:00")
	r2 := mustTimeT(t, "2026-09-19T15:00:00+08:00")
	h.ingest("rf1", EvTicketRefunded, r1, TicketRefunded{TicketNo: "TK1", RefundMin: 15000, RefundedAt: r1})
	_, v := h.getRedemption("platform", "RD1")
	if v.AutoNetMin != 1400 || v.Status != StatusReversedPartial { // 2000 * 70%
		t.Fatalf("首次部分退票后净额应为 1400：%+v", v)
	}
	h.ingest("rf2", EvTicketRefunded, r2, TicketRefunded{TicketNo: "TK1", RefundMin: 10000, RefundedAt: r2})
	_, v = h.getRedemption("platform", "RD1")
	if v.AutoNetMin != 1000 || v.ReversedMin != 1000 { // 累计 50%
		t.Fatalf("累计退票 50%% 后净额应为 1000：%+v", v)
	}
	// 重复投递同一笔退票（新 event_id 同业务键），不得二次冲正。
	h.ingest("rf2-dup", EvTicketRefunded, r2, TicketRefunded{TicketNo: "TK1", RefundMin: 10000, RefundedAt: r2})
	_, v = h.getRedemption("platform", "RD1")
	if v.AutoNetMin != 1000 {
		t.Fatalf("重复退票不得二次冲正：%+v", v)
	}
}

// ---------- 验收场景 3：并发核销（单次权益互斥） ----------

func TestAcceptanceConcurrentRedemption(t *testing.T) {
	h := newAPIHarness(t)
	at := mustTimeT(t, "2026-09-18T20:00:00+08:00")
	h.ingest("t1", EvTicketIssued, at, TicketIssued{TicketNo: "TK1", MatchID: "match-2026-09-18", FaceMin: 50000, Currency: "CNY"})
	h.ingest("g1", EvBenefitGranted, at, BenefitGranted{GrantID: "GR1", TicketNo: "TK1", ProgramCode: "city-stay-a", IssuedAt: at, SingleUse: true})
	h.ingest("a1", EvAccessVerified, at, AccessVerified{TicketNo: "TK1", MatchID: "match-2026-09-18", VerifiedAt: at})

	// 两个商户同时对同一张单次权益发起核销，并发投递。
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, p := range []struct {
		eid, rid, m string
	}{{"r-a", "RDA", "MA"}, {"r-b", "RDB", "MB"}} {
		wg.Add(1)
		go func(i int, eid, rid, m string) {
			defer wg.Done()
			raw, _ := json.Marshal(map[string]any{
				"event_id": eid, "type": EvRedemptionRecorded, "occurred_at": at.Format(time.RFC3339),
				"payload": map[string]any{
					"redemption_id": rid, "merchant_id": m, "program_code": "city-stay-a",
					"grant_id": "GR1", "amount_min": 10000, "currency": "CNY",
				},
			})
			req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(raw))
			req.Header.Set("Authorization", "Bearer platform")
			rec := httptest.NewRecorder()
			h.svc.Routes().ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				errs[i] = fmt.Errorf("code=%d body=%s", rec.Code, rec.Body.String())
			}
		}(i, p.eid, p.rid, p.m)
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	_, va := h.getRedemption("platform", "RDA")
	_, vb := h.getRedemption("platform", "RDB")
	accepted := 0
	if va.Status == StatusAccepted {
		accepted++
	}
	if vb.Status == StatusAccepted {
		accepted++
	}
	if accepted != 1 {
		t.Fatalf("单次权益只应成功一笔：A=%+v B=%+v", va, vb)
	}
	// 以业务时间+核销 ID 决胜：同一时刻 RDA < RDB，RDA 胜。
	if va.Status != StatusAccepted {
		t.Fatalf("RDA 应按 ID 决胜胜出：%+v", va)
	}
	if !containsStr(vb.Reasons, "single_use_already_consumed") {
		t.Fatalf("RDB 应因单次权益已占用被拒：%+v", vb)
	}
}

// ---------- 补充：幂等（重复事件/重发不得二次补贴） ----------

func TestAcceptanceIdempotency(t *testing.T) {
	h := newAPIHarness(t)
	at := mustTimeT(t, "2026-09-18T20:00:00+08:00")
	h.ingest("t1", EvTicketIssued, at, TicketIssued{TicketNo: "TK1", MatchID: "match-2026-09-18", FaceMin: 50000, Currency: "CNY"})
	h.ingest("g1", EvBenefitGranted, at, BenefitGranted{GrantID: "GR1", TicketNo: "TK1", ProgramCode: "city-stay-a", IssuedAt: at, SingleUse: true})
	h.ingest("a1", EvAccessVerified, at, AccessVerified{TicketNo: "TK1", MatchID: "match-2026-09-18", VerifiedAt: at})
	rec := RedemptionRecorded{RedemptionID: "RD1", MerchantID: "M1", ProgramCode: "city-stay-a",
		GrantID: "GR1", AmountMin: 10000, Currency: "CNY"}
	h.ingest("r1", EvRedemptionRecorded, at, rec)

	// 同一 event_id 重发：幂等返回 duplicate，不报错、不二次入账。
	out := h.ingest("r1", EvRedemptionRecorded, at, rec)
	if out["outcome"] != "duplicate" {
		t.Fatalf("同 event_id 应幂等：%v", out)
	}
	// 网络重试换了 event_id 但业务流水号相同：不得二次补贴。
	out2 := h.ingest("r1-retry", EvRedemptionRecorded, at, rec)
	if out2["outcome"] != "duplicate" {
		t.Fatalf("同 redemption_id 重发应幂等去重：%v", out2)
	}
	_, totals := h.doRawGetTotals("platform")
	var payable int64
	for _, row := range totals {
		if row.Dimension == "merchant" && row.Key == "M1" {
			payable += row.PayableMin
		}
	}
	if payable != 2000 {
		t.Fatalf("重复核销不得二次补贴，应付应仍为 2000：%d", payable)
	}
}

func containsStr(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// ---------- 验收场景 4：封账后补传只能走调整分录 ----------

func TestAcceptanceLateAfterClose(t *testing.T) {
	h := newAPIHarness(t)
	at := mustTimeT(t, "2026-09-18T20:00:00+08:00")
	h.ingest("t1", EvTicketIssued, at, TicketIssued{TicketNo: "TK1", MatchID: "match-2026-09-18", FaceMin: 50000, Currency: "CNY"})
	h.ingest("g1", EvBenefitGranted, at, BenefitGranted{GrantID: "GR1", TicketNo: "TK1", ProgramCode: "city-stay-a", IssuedAt: at, SingleUse: true})
	h.ingest("a1", EvAccessVerified, at, AccessVerified{TicketNo: "TK1", MatchID: "match-2026-09-18", VerifiedAt: at})

	// 财务先封 2026-09 账期。
	h.now = mustTimeT(t, "2026-10-05T10:00:00+08:00")
	code, out := h.do(http.MethodPost, "/v1/periods/2026-09/close", "finance", nil)
	if code != http.StatusOK {
		t.Fatalf("封账失败：%d %v", code, out)
	}

	// 封账后商户补传 9 月赛中消费。
	late := mustTimeT(t, "2026-09-18T21:00:00+08:00")
	res := h.ingest("r-late", EvRedemptionRecorded, late, RedemptionRecorded{
		RedemptionID: "RDLATE", MerchantID: "M1", ProgramCode: "city-stay-a",
		GrantID: "GR1", AmountMin: 10000, Currency: "CNY",
	})
	if blocked, _ := res["blocked"].(bool); !blocked {
		t.Fatalf("封账后补传必须挂起：%+v", res)
	}
	// 9 月快照金额不被改变。
	verify, _ := h.do(http.MethodGet, "/v1/periods/2026-09/verify", "finance", nil)
	if verify != http.StatusOK {
		t.Fatalf("快照校验失败：%d", verify)
	}
	// 普通事件无法再写入已封账账期——这里补贴已被挂起，尝试直接调整到 9 月应被拒绝。
	badAdj := AdjustmentPosted{
		AdjustmentID: "adj-bad", TargetPeriod: "2026-09", Reason: "x", PostedAt: h.now,
		Lines: []AdjustmentLine{
			{Account: AccountAROrganizer, CounterpartyID: "org-main", DC: "D", AmountMin: 1},
			{Account: AccountAPMerchant, CounterpartyID: "M1", DC: "C", AmountMin: 1},
		},
	}
	code, _ = h.do(http.MethodPost, "/v1/adjustments", "finance", badAdj)
	if code != http.StatusConflict {
		t.Fatalf("向已封账账期调整应 409：%d", code)
	}

	// 财务在 10 月开放账期做调整分录，金额必须恰好承接挂起的 2000 分。
	adj := AdjustmentPosted{
		AdjustmentID: "adj-1", OriginEventID: "r-late", TargetPeriod: "2026-10",
		Reason: "补提 9 月赛事封账后补传补贴", PostedAt: h.now,
		Lines: []AdjustmentLine{
			{Account: AccountAROrganizer, CounterpartyID: "org-main", MatchID: "match-2026-09-18", ProgramCode: "city-stay-a", DC: "D", AmountMin: 2000},
			{Account: AccountAPMerchant, CounterpartyID: "M1", MatchID: "match-2026-09-18", ProgramCode: "city-stay-a", DC: "C", AmountMin: 2000},
		},
	}
	code, out = h.do(http.MethodPost, "/v1/adjustments", "finance", adj)
	if code != http.StatusOK {
		t.Fatalf("调整分录入账失败：%d %v", code, out)
	}
	_, v := h.getRedemption("platform", "RDLATE")
	if v.Status != StatusAdjusted || v.AdjustedMin != 2000 {
		t.Fatalf("挂起应被调整承接：%+v", v)
	}
	// 金额不匹配的调整必须被拒绝。
	wrong := adj
	wrong.AdjustmentID = "adj-2"
	wrong.Lines[0].AmountMin = 1999
	wrong.Lines[1].AmountMin = 1999
	code, _ = h.do(http.MethodPost, "/v1/adjustments", "finance", wrong)
	if code != http.StatusBadRequest {
		t.Fatalf("金额与挂起不符的调整必须拒绝：%d", code)
	}
}

// ---------- 补充：封账后冲正（9 月消费 10 月退票） ----------

func TestAcceptanceReversalAfterClose(t *testing.T) {
	h := newAPIHarness(t)
	at := mustTimeT(t, "2026-09-18T20:00:00+08:00")
	h.ingest("t1", EvTicketIssued, at, TicketIssued{TicketNo: "TK1", MatchID: "match-2026-09-18", FaceMin: 50000, Currency: "CNY"})
	h.ingest("g1", EvBenefitGranted, at, BenefitGranted{GrantID: "GR1", TicketNo: "TK1", ProgramCode: "city-stay-a", IssuedAt: at, SingleUse: true})
	h.ingest("a1", EvAccessVerified, at, AccessVerified{TicketNo: "TK1", MatchID: "match-2026-09-18", VerifiedAt: at})
	h.ingest("r1", EvRedemptionRecorded, at, RedemptionRecorded{
		RedemptionID: "RD1", MerchantID: "M1", ProgramCode: "city-stay-a",
		GrantID: "GR1", AmountMin: 10000, Currency: "CNY",
	})
	h.now = mustTimeT(t, "2026-10-01T09:00:00+08:00")
	if code, _ := h.do(http.MethodPost, "/v1/periods/2026-09/close", "finance", nil); code != 200 {
		t.Fatal("封账失败")
	}
	// 10 月全额退票：冲正 2000 分应挂起，9 月快照不变。
	rAt := mustTimeT(t, "2026-10-03T11:00:00+08:00")
	h.ingest("rf1", EvTicketRefunded, rAt, TicketRefunded{TicketNo: "TK1", RefundMin: 50000, RefundedAt: rAt})
	_, v := h.getRedemption("platform", "RD1")
	if v.BlockedMin != -2000 || v.Status != StatusBlocked {
		t.Fatalf("封账后冲正应挂起 -2000：%+v", v)
	}
	code, out := h.do(http.MethodGet, "/v1/periods/2026-09/verify", "finance", nil)
	if code != 200 {
		t.Fatalf("快照校验失败：%v", out)
	}
	// 在 10 月账做反向调整分录承接冲正。
	adj := AdjustmentPosted{
		AdjustmentID: "adj-r1", OriginEventID: "rf1", TargetPeriod: "2026-10",
		Reason: "9 月补贴对应门票 10 月退票，冲回应付/应收", PostedAt: rAt,
		Lines: []AdjustmentLine{
			{Account: AccountAPMerchant, CounterpartyID: "M1", MatchID: "match-2026-09-18", ProgramCode: "city-stay-a", DC: "D", AmountMin: 2000},
			{Account: AccountAROrganizer, CounterpartyID: "org-main", MatchID: "match-2026-09-18", ProgramCode: "city-stay-a", DC: "C", AmountMin: 2000},
		},
	}
	if code, out = h.do(http.MethodPost, "/v1/adjustments", "finance", adj); code != 200 {
		t.Fatalf("冲正调整失败：%d %v", code, out)
	}
}

// ---------- 补充：事件顺序无关性（迟到事件重放结果一致） ----------

func TestAcceptanceOrderIndependence(t *testing.T) {
	build := func(reverse bool) []RedemptionView {
		st := testStore(t)
		at := mustTimeT(t, "2026-09-18T20:00:00+08:00")
		rAt := mustTimeT(t, "2026-09-19T10:00:00+08:00")
		evs := []*Envelope{
			env("e-t", EvTicketIssued, at, TicketIssued{TicketNo: "TK1", MatchID: "match-2026-09-18", FaceMin: 50000, Currency: "CNY"}),
			env("e-g", EvBenefitGranted, at, BenefitGranted{GrantID: "GR1", TicketNo: "TK1", ProgramCode: "city-stay-a", IssuedAt: at, SingleUse: true}),
			env("e-a", EvAccessVerified, at, AccessVerified{TicketNo: "TK1", MatchID: "match-2026-09-18", VerifiedAt: at}),
			env("e-r", EvRedemptionRecorded, at, RedemptionRecorded{RedemptionID: "RD1", MerchantID: "M1", ProgramCode: "city-stay-a", GrantID: "GR1", AmountMin: 10000, Currency: "CNY"}),
			env("e-rf", EvTicketRefunded, rAt, TicketRefunded{TicketNo: "TK1", RefundMin: 25000, RefundedAt: rAt}),
		}
		if reverse {
			for i, j := 0, len(evs)-1; i < j; i, j = i+1, j-1 {
				evs[i], evs[j] = evs[j], evs[i]
			}
		}
		for _, e := range evs {
			if _, err := st.Ingest(e); err != nil {
				t.Fatal(err)
			}
		}
		return st.ListRedemptions(Scope{Role: RolePlatform})
	}
	a := build(false)
	b := build(true)
	if len(a) != 1 || len(b) != 1 || a[0].AutoNetMin != b[0].AutoNetMin || a[0].AutoNetMin != 1000 {
		t.Fatalf("正序/乱序摄入结果必须一致：%+v vs %+v", a, b)
	}
}

func env(id, typ string, at time.Time, payload any) *Envelope {
	raw, _ := json.Marshal(payload)
	return &Envelope{EventID: id, Type: typ, OccurredAt: at, Payload: raw}
}

// ---------- 补充：角色数据范围隔离 ----------

func TestAcceptanceScopeIsolation(t *testing.T) {
	h := newAPIHarness(t)
	at := mustTimeT(t, "2026-09-18T20:00:00+08:00")
	h.ingest("t1", EvTicketIssued, at, TicketIssued{TicketNo: "TK1", MatchID: "match-2026-09-18", FaceMin: 50000, Currency: "CNY"})
	h.ingest("g1", EvBenefitGranted, at, BenefitGranted{GrantID: "GR1", TicketNo: "TK1", ProgramCode: "city-stay-a", IssuedAt: at, SingleUse: true})
	h.ingest("a1", EvAccessVerified, at, AccessVerified{TicketNo: "TK1", MatchID: "match-2026-09-18", VerifiedAt: at})
	h.ingest("r1", EvRedemptionRecorded, at, RedemptionRecorded{
		RedemptionID: "RD1", MerchantID: "M1", ProgramCode: "city-stay-a",
		GrantID: "GR1", AmountMin: 10000, Currency: "CNY",
	})

	// 无令牌 401。
	if code, _ := h.do(http.MethodGet, "/v1/redemptions", "", nil); code != http.StatusUnauthorized {
		t.Fatalf("无令牌应 401：%d", code)
	}
	// 商户 M2 看不到 M1 的核销。
	code, out := h.do(http.MethodGet, "/v1/redemptions/RD1", "merchant:M2", nil)
	if code != http.StatusForbidden {
		t.Fatalf("跨商户访问应 403：%d %v", code, out)
	}
	code, out = h.do(http.MethodGet, "/v1/redemptions", "merchant:M2", nil)
	if code != 200 {
		t.Fatalf("列表查询应 200：%d", code)
	}
	if list, _ := out["redemptions"].([]any); len(list) != 0 {
		t.Fatalf("M2 列表必须为空：%v", list)
	}
	// 商户 M1 看不到主办方维度数据。
	_, totals := h.doRawGetTotals("merchant:M1")
	for _, row := range totals {
		if row.Dimension == "organizer" {
			t.Fatalf("商户不得看到主办方维度：%+v", row)
		}
	}
	// 主办方看不到商户身份，商户维度被抹掉。
	code, out = h.do(http.MethodGet, "/v1/redemptions/RD1", "organizer:org-main", nil)
	if code != 200 || out["merchant_id"] != "" {
		t.Fatalf("主办方视图必须遮蔽商户标识：%d %v", code, out)
	}
	// 主办方不能封账。
	if code, _ = h.do(http.MethodPost, "/v1/periods/2026-09/close", "organizer:org-main", nil); code != http.StatusForbidden {
		t.Fatalf("主办方封账应 403：%d", code)
	}
	// 载荷夹带身份字段必须拒绝。
	raw, _ := json.Marshal(map[string]any{
		"event_id": "evil", "type": EvRedemptionRecorded, "occurred_at": at.Format(time.RFC3339),
		"payload": map[string]any{
			"redemption_id": "RDEVIL", "merchant_id": "M1", "program_code": "city-stay-a",
			"grant_id": "GR1", "amount_min": 10000, "currency": "CNY", "customer_name": "张三",
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer platform")
	rec := httptest.NewRecorder()
	h.svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("夹带身份字段应 422：%d %s", rec.Code, rec.Body.String())
	}
}

func (h *apiHarness) doRawGetTotals(token string) (int, []Total) {
	code, out := h.do(http.MethodGet, "/v1/periods/2026-09/totals", token, nil)
	var res struct {
		Totals []Total `json:"totals"`
	}
	raw, _ := json.Marshal(out)
	_ = json.Unmarshal(raw, &res)
	return code, res.Totals
}

// ---------- 补充：快照复算与审计链 ----------

func TestAcceptanceSnapshotReproducible(t *testing.T) {
	h := newAPIHarness(t)
	at := mustTimeT(t, "2026-09-18T20:00:00+08:00")
	h.ingest("t1", EvTicketIssued, at, TicketIssued{TicketNo: "TK1", MatchID: "match-2026-09-18", FaceMin: 50000, Currency: "CNY"})
	h.ingest("g1", EvBenefitGranted, at, BenefitGranted{GrantID: "GR1", TicketNo: "TK1", ProgramCode: "city-stay-a", IssuedAt: at, SingleUse: true})
	h.ingest("a1", EvAccessVerified, at, AccessVerified{TicketNo: "TK1", MatchID: "match-2026-09-18", VerifiedAt: at})
	h.ingest("r1", EvRedemptionRecorded, at, RedemptionRecorded{
		RedemptionID: "RD1", MerchantID: "M1", ProgramCode: "city-stay-a",
		GrantID: "GR1", AmountMin: 10000, Currency: "CNY",
	})
	h.now = mustTimeT(t, "2026-10-01T00:00:00+08:00")
	code, out := h.do(http.MethodPost, "/v1/periods/2026-09/close", "finance", nil)
	if code != 200 {
		t.Fatalf("封账失败：%v", out)
	}
	code, out = h.do(http.MethodGet, "/v1/periods/2026-09/verify", "finance", nil)
	if code != 200 || out["matches"] != true {
		t.Fatalf("快照复算应一致：%d %v", code, out)
	}
	// 审计链：原始事件、规则版本、凭证齐全。
	code, out = h.do(http.MethodGet, "/v1/redemptions/RD1/audit", "finance", nil)
	if code != 200 {
		t.Fatalf("审计查询失败：%d", code)
	}
	if out["rule_version"] != "v2026.09" {
		t.Fatalf("审计链必须带规则版本：%v", out["rule_version"])
	}
	evs, _ := out["events"].([]any)
	if len(evs) < 4 {
		t.Fatalf("审计链至少包含发票/发权益/入场/核销：%d", len(evs))
	}
	vouch, _ := out["vouchers"].([]any)
	if len(vouch) != 1 {
		t.Fatalf("审计链应有 1 条补贴凭证：%d", len(vouch))
	}
	// 重复封账必须拒绝。
	if code, _ = h.do(http.MethodPost, "/v1/periods/2026-09/close", "finance", nil); code != http.StatusConflict {
		t.Fatalf("重复封账应 409：%d", code)
	}
}

// ---------- 补充：迟到宽限与封账的优先级 ----------

func TestAcceptanceLateGraceBoundary(t *testing.T) {
	at := mustTimeT(t, "2026-09-18T20:00:00+08:00")
	within := mustTimeT(t, "2026-09-20T03:00:00+08:00") // 窗口 09-20 02:00 + 120min 宽限内
	beyond := mustTimeT(t, "2026-09-20T05:00:00+08:00") // 超过宽限

	// 开放账期内：宽限内接受，超宽限拒绝。
	h := newAPIHarness(t)
	h.ingest("t1", EvTicketIssued, at, TicketIssued{TicketNo: "TK1", MatchID: "match-2026-09-18", FaceMin: 50000, Currency: "CNY"})
	h.ingest("g1", EvBenefitGranted, at, BenefitGranted{GrantID: "GR1", TicketNo: "TK1", ProgramCode: "city-stay-a", IssuedAt: at, SingleUse: true})
	h.ingest("a1", EvAccessVerified, at, AccessVerified{TicketNo: "TK1", MatchID: "match-2026-09-18", VerifiedAt: at})
	code, out := h.ingestReceived("r-ok", EvRedemptionRecorded, at, within,
		RedemptionRecorded{RedemptionID: "ROK", MerchantID: "M1", ProgramCode: "city-stay-a", GrantID: "GR1", AmountMin: 10000, Currency: "CNY"})
	if code != 200 || out["outcome"] != "accepted" {
		t.Fatalf("宽限内应接受：%v", out)
	}
	code, out = h.ingestReceived("r-late", EvRedemptionRecorded, at, beyond,
		RedemptionRecorded{RedemptionID: "RLATEOPEN", MerchantID: "M1", ProgramCode: "city-stay-a", GrantID: "GR1", AmountMin: 10000, Currency: "CNY"})
	if code != 200 || out["outcome"] != "rejected" {
		t.Fatalf("开放账期超宽限应拒绝：%v", out)
	}

	// 已封账：即使超宽限也必须挂起，交给财务调整（不能让封账后补传无处可去）。
	h2 := newAPIHarness(t)
	h2.ingest("t1", EvTicketIssued, at, TicketIssued{TicketNo: "TK1", MatchID: "match-2026-09-18", FaceMin: 50000, Currency: "CNY"})
	h2.ingest("g1", EvBenefitGranted, at, BenefitGranted{GrantID: "GR1", TicketNo: "TK1", ProgramCode: "city-stay-a", IssuedAt: at, SingleUse: true})
	h2.ingest("a1", EvAccessVerified, at, AccessVerified{TicketNo: "TK1", MatchID: "match-2026-09-18", VerifiedAt: at})
	h2.now = mustTimeT(t, "2026-10-05T10:00:00+08:00")
	if code, _ := h2.do(http.MethodPost, "/v1/periods/2026-09/close", "finance", nil); code != 200 {
		t.Fatal("封账失败")
	}
	code, out = h2.ingestReceived("r-late2", EvRedemptionRecorded, at, beyond,
		RedemptionRecorded{RedemptionID: "RLATECLOSED", MerchantID: "M1", ProgramCode: "city-stay-a", GrantID: "GR1", AmountMin: 10000, Currency: "CNY"})
	if code != 200 || out["outcome"] != "accepted" || out["blocked"] != true {
		t.Fatalf("封账后超宽限补传也应挂起：%v", out)
	}
	_, v := h2.getRedemption("platform", "RLATECLOSED")
	if v.Status != StatusBlocked || v.BlockedMin != 2000 {
		t.Fatalf("应挂起 2000：%+v", v)
	}
}

// ---------- 补充：改期保留与赛后延时 ----------

func TestAcceptanceRescheduleAndPostMatch(t *testing.T) {
	h := newAPIHarness(t)
	// 赛中消费，之后赛事官方改期：已发生消费保留（冲正不应出现）。
	at := mustTimeT(t, "2026-09-18T20:00:00+08:00")
	h.ingest("t1", EvTicketIssued, at, TicketIssued{TicketNo: "TK1", MatchID: "match-2026-09-18", FaceMin: 50000, Currency: "CNY"})
	h.ingest("g1", EvBenefitGranted, at, BenefitGranted{GrantID: "GR1", TicketNo: "TK1", ProgramCode: "city-stay-a", IssuedAt: at, SingleUse: true})
	h.ingest("a1", EvAccessVerified, at, AccessVerified{TicketNo: "TK1", MatchID: "match-2026-09-18", VerifiedAt: at})
	h.ingest("r1", EvRedemptionRecorded, at, RedemptionRecorded{
		RedemptionID: "RD1", MerchantID: "M1", ProgramCode: "city-stay-a",
		GrantID: "GR1", AmountMin: 10000, Currency: "CNY",
	})
	rsAt := mustTimeT(t, "2026-09-19T08:00:00+08:00")
	h.ingest("mr1", EvMatchRescheduled, rsAt, MatchRescheduled{
		MatchID:     "match-2026-09-18",
		NewStartsAt: mustTimeT(t, "2026-09-25T19:30:00+08:00"),
		NewEndsAt:   mustTimeT(t, "2026-09-25T22:00:00+08:00"),
		At:          rsAt,
	})
	_, v := h.getRedemption("platform", "RD1")
	if v.AutoNetMin != 2000 || v.Status != StatusRetained {
		t.Fatalf("改期前消费应保留：%+v", v)
	}

	// 赛后延时消费（22:30，窗口内到次日 02:00），按规则保留补贴，阶段为 post_match。
	post := mustTimeT(t, "2026-09-18T22:30:00+08:00")
	h.ingest("t2", EvTicketIssued, post, TicketIssued{TicketNo: "TK2", MatchID: "match-2026-09-18", FaceMin: 50000, Currency: "CNY"})
	h.ingest("g2", EvBenefitGranted, post, BenefitGranted{GrantID: "GR2", TicketNo: "TK2", ProgramCode: "city-stay-a", IssuedAt: post, SingleUse: true})
	h.ingest("a2", EvAccessVerified, mustTimeT(t, "2026-09-18T21:50:00+08:00"), AccessVerified{TicketNo: "TK2", MatchID: "match-2026-09-18", VerifiedAt: mustTimeT(t, "2026-09-18T21:50:00+08:00")})
	h.ingest("r2", EvRedemptionRecorded, post, RedemptionRecorded{
		RedemptionID: "RD2", MerchantID: "M1", ProgramCode: "city-stay-a",
		GrantID: "GR2", AmountMin: 10000, Currency: "CNY",
	})
	_, v2 := h.getRedemption("platform", "RD2")
	if v2.Stage != StagePostMatch || v2.SubsidyMin != 2000 || v2.Status != StatusRetained {
		t.Fatalf("赛后延时消费应保留并标记 post_match：%+v", v2)
	}
}

// ---------- 补充：赛事取消冲正 ----------

func TestAcceptanceMatchCancel(t *testing.T) {
	h := newAPIHarness(t)
	at := mustTimeT(t, "2026-09-18T20:00:00+08:00")
	h.ingest("t1", EvTicketIssued, at, TicketIssued{TicketNo: "TK1", MatchID: "match-2026-09-18", FaceMin: 50000, Currency: "CNY"})
	h.ingest("g1", EvBenefitGranted, at, BenefitGranted{GrantID: "GR1", TicketNo: "TK1", ProgramCode: "city-stay-a", IssuedAt: at, SingleUse: true})
	h.ingest("a1", EvAccessVerified, at, AccessVerified{TicketNo: "TK1", MatchID: "match-2026-09-18", VerifiedAt: at})
	h.ingest("r1", EvRedemptionRecorded, at, RedemptionRecorded{
		RedemptionID: "RD1", MerchantID: "M1", ProgramCode: "city-stay-a",
		GrantID: "GR1", AmountMin: 10000, Currency: "CNY",
	})
	cAt := mustTimeT(t, "2026-09-18T23:00:00+08:00")
	h.ingest("c1", EvMatchCancelled, cAt, MatchCancelled{MatchID: "match-2026-09-18", CancelledAt: cAt})
	_, v := h.getRedemption("platform", "RD1")
	if v.AutoNetMin != 0 || v.Status != StatusReversedFull {
		t.Fatalf("赛事取消应全额冲正：%+v", v)
	}
}
