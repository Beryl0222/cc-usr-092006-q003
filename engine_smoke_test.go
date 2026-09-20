package settlement

import (
	"testing"
	"time"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return tm
}

func ingest(t *testing.T, st *Store, id, typ string, at time.Time, payload any) *IngestResult {
	t.Helper()
	return ingestReceived(t, st, id, typ, at, time.Time{}, payload)
}

func ingestReceived(t *testing.T, st *Store, id, typ string, at, received time.Time, payload any) *IngestResult {
	t.Helper()
	raw := mustJSON(t, payload)
	res, err := st.Ingest(&Envelope{EventID: id, Type: typ, OccurredAt: at, ReceivedAt: received, Payload: raw})
	if err != nil {
		t.Fatalf("ingest %s: %v", id, err)
	}
	return res
}

func TestSmokeBasicAttribution(t *testing.T) {
	st := testStore(t)
	// 发票 + 发权益 + 入场 + 赛中核销 100 元（10000 分），补贴 20%
	at := mustTime(t, "2026-09-18T20:00:00+08:00")
	ingest(t, st, "t1", EvTicketIssued, at, TicketIssued{TicketNo: "TK1", MatchID: "match-2026-09-18", FaceMin: 50000, Currency: "CNY"})
	ingest(t, st, "g1", EvBenefitGranted, at, BenefitGranted{GrantID: "GR1", TicketNo: "TK1", ProgramCode: "city-stay-a", IssuedAt: at, SingleUse: true})
	ingest(t, st, "a1", EvAccessVerified, at, AccessVerified{TicketNo: "TK1", MatchID: "match-2026-09-18", VerifiedAt: at})
	res := ingest(t, st, "r1", EvRedemptionRecorded, at, RedemptionRecorded{
		RedemptionID: "RD1", MerchantID: "M1", ProgramCode: "city-stay-a",
		GrantID: "GR1", AmountMin: 10000, Currency: "CNY",
	})
	if res.Outcome != "accepted" || res.Blocked {
		t.Fatalf("核销应被接受：%+v", res)
	}
	v, _ := st.Redemption("RD1", Scope{Role: RolePlatform})
	if v.SubsidyMin != 2000 || v.Stage != StageInMatch || v.MatchID != "match-2026-09-18" {
		t.Fatalf("归因结果错误：%+v", v)
	}
}

func TestSmokePartialRefund(t *testing.T) {
	st := testStore(t)
	at := mustTime(t, "2026-09-18T20:00:00+08:00")
	ingest(t, st, "t1", EvTicketIssued, at, TicketIssued{TicketNo: "TK1", MatchID: "match-2026-09-18", FaceMin: 50000, Currency: "CNY"})
	ingest(t, st, "g1", EvBenefitGranted, at, BenefitGranted{GrantID: "GR1", TicketNo: "TK1", ProgramCode: "city-stay-a", IssuedAt: at, SingleUse: true})
	ingest(t, st, "a1", EvAccessVerified, at, AccessVerified{TicketNo: "TK1", MatchID: "match-2026-09-18", VerifiedAt: at})
	ingest(t, st, "r1", EvRedemptionRecorded, at, RedemptionRecorded{
		RedemptionID: "RD1", MerchantID: "M1", ProgramCode: "city-stay-a",
		GrantID: "GR1", AmountMin: 10000, Currency: "CNY",
	})
	// 赛后部分退票 50%
	rAt := mustTime(t, "2026-09-19T10:00:00+08:00")
	ingest(t, st, "rf1", EvTicketRefunded, rAt, TicketRefunded{TicketNo: "TK1", RefundMin: 25000, RefundedAt: rAt})
	v, _ := st.Redemption("RD1", Scope{Role: RolePlatform})
	if v.Status != StatusReversedPartial || v.AutoNetMin != 1000 {
		t.Fatalf("部分退票后应剩 50%% 补贴：%+v", v)
	}
}

func TestSmokeRefundBeforeRedeem(t *testing.T) {
	st := testStore(t)
	at := mustTime(t, "2026-09-18T20:00:00+08:00")
	ingest(t, st, "t1", EvTicketIssued, at, TicketIssued{TicketNo: "TK1", MatchID: "match-2026-09-18", FaceMin: 50000, Currency: "CNY"})
	ingest(t, st, "g1", EvBenefitGranted, at, BenefitGranted{GrantID: "GR1", TicketNo: "TK1", ProgramCode: "city-stay-a", IssuedAt: at, SingleUse: true})
	ingest(t, st, "a1", EvAccessVerified, at, AccessVerified{TicketNo: "TK1", MatchID: "match-2026-09-18", VerifiedAt: at})
	rAt := mustTime(t, "2026-09-18T19:00:00+08:00")
	ingest(t, st, "rf0", EvTicketRefunded, rAt, TicketRefunded{TicketNo: "TK1", RefundMin: 50000, RefundedAt: rAt})
	res := ingest(t, st, "r1", EvRedemptionRecorded, at, RedemptionRecorded{
		RedemptionID: "RD1", MerchantID: "M1", ProgramCode: "city-stay-a",
		GrantID: "GR1", AmountMin: 10000, Currency: "CNY",
	})
	if res.Outcome != "rejected" {
		t.Fatalf("退票后核销应拒绝：%+v", res)
	}
}
