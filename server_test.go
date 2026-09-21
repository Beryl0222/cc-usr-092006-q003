package settlement

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func newTestServer(t *testing.T) (*httptest.Server, *Engine) {
	t.Helper()
	e := mustEngine(t, testNow)
	srv := httptest.NewServer(NewHandler(e))
	t.Cleanup(srv.Close)
	return srv, e
}

func doJSON(t *testing.T, method, url string, headers map[string]string, body any) (int, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, url, &buf)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("响应不是合法 JSON：%v", err)
	}
	return resp.StatusCode, out
}

func financeHeaders() map[string]string { return map[string]string{"X-Actor-Role": "finance"} }

// 接口验收主流程：批量接入 → 角色隔离查询 → 封账 → 封账后补传 → 复算与审计。
func TestHTTPAcceptanceFlow(t *testing.T) {
	srv, _ := newTestServer(t)

	batch := map[string]any{"events": []map[string]any{
		{"id": "tk-1", "type": "ticket", "occurred_at": "2026-09-18T10:00:00+08:00",
			"payload": map[string]any{"ticket_token": "T-1", "event_id": "match-2026-09-18", "status": "issued"}},
		{"id": "en-1", "type": "entry", "occurred_at": "2026-09-18T18:30:00+08:00",
			"payload": map[string]any{"ticket_token": "T-1", "event_id": "match-2026-09-18"}},
		{"id": "gr-1", "type": "benefit_grant", "occurred_at": "2026-09-18T10:05:00+08:00",
			"payload": map[string]any{"benefit_id": "B-1", "program_code": "city-stay-a", "ticket_token": "T-1", "quota": 100000}},
		{"id": "rd-1", "type": "redemption", "occurred_at": "2026-09-18T20:00:00+08:00",
			"payload": map[string]any{"benefit_id": "B-1", "merchant_id": "M-hotel", "amount": 50000}},
	}}

	code, out := doJSON(t, "POST", srv.URL+"/v1/events", nil, batch)
	if code != http.StatusOK {
		t.Fatalf("事件接入失败：%d %v", code, out)
	}
	for _, r := range out["results"].([]any) {
		if r.(map[string]any)["status"] != "applied" {
			t.Fatalf("事件未受理：%v", r)
		}
	}

	// 重复投递整批事件：全部去重，不产生二次补贴。
	code, out = doJSON(t, "POST", srv.URL+"/v1/events", nil, batch)
	if code != http.StatusOK {
		t.Fatalf("重复投递失败：%d", code)
	}
	for _, r := range out["results"].([]any) {
		if r.(map[string]any)["status"] != "duplicate" {
			t.Fatalf("重复事件应去重：%v", r)
		}
	}

	// 未声明角色 → 拒绝。
	if code, _ := doJSON(t, "GET", srv.URL+"/v1/ledger", nil, nil); code != http.StatusForbidden {
		t.Fatalf("未授权访问应被拒绝：%d", code)
	}

	// 商户仅见自身应付款行。
	code, out = doJSON(t, "GET", srv.URL+"/v1/ledger", map[string]string{
		"X-Actor-Role": "merchant", "X-Actor-Merchant": "M-hotel"}, nil)
	if code != http.StatusOK {
		t.Fatalf("商户查询失败：%d", code)
	}
	for _, l := range out["lines"].([]any) {
		if l.(map[string]any)["account"] != "payable:merchant:M-hotel" {
			t.Fatalf("商户越权看到他人账户行：%v", l)
		}
	}

	// 主办方查看自有场次带动消费；查看他人场次被拒绝。
	code, out = doJSON(t, "GET", srv.URL+"/v1/consumption?match_id=match-2026-09-18", map[string]string{
		"X-Actor-Role": "organizer", "X-Actor-Match": "match-2026-09-18"}, nil)
	if code != http.StatusOK {
		t.Fatalf("主办方查询失败：%d %v", code, out)
	}
	rows := out["rows"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["gross"].(float64) != 50000 {
		t.Fatalf("带动消费数据错误：%v", rows)
	}
	if code, _ := doJSON(t, "GET", srv.URL+"/v1/consumption?match_id=match-2026-09-20", map[string]string{
		"X-Actor-Role": "organizer", "X-Actor-Match": "match-2026-09-18"}, nil); code != http.StatusForbidden {
		t.Fatalf("主办方不应查看他人场次：%d", code)
	}

	// 主办方无权封账；财务封账成功。
	if code, _ := doJSON(t, "POST", srv.URL+"/v1/periods/2026-09/close", map[string]string{
		"X-Actor-Role": "organizer", "X-Actor-Match": "match-2026-09-18"}, nil); code != http.StatusForbidden {
		t.Fatalf("主办方不应能封账：%d", code)
	}
	code, out = doJSON(t, "POST", srv.URL+"/v1/periods/2026-09/close", financeHeaders(), nil)
	if code != http.StatusOK || out["hash"] == "" {
		t.Fatalf("封账失败：%d %v", code, out)
	}

	// 封账后补传迟到流水 → 调整分录落入 2026-10。
	code, out = doJSON(t, "POST", srv.URL+"/v1/events", nil, map[string]any{"events": []map[string]any{
		{"id": "rd-late", "type": "redemption", "occurred_at": "2026-09-19T10:00:00+08:00",
			"payload": map[string]any{"benefit_id": "B-1", "merchant_id": "M-hotel", "amount": 12000}},
	}})
	if code != http.StatusOK {
		t.Fatalf("补传失败：%d", code)
	}
	late := out["results"].([]any)[0].(map[string]any)
	if late["status"] != "applied" {
		t.Fatalf("补传未受理：%v", late)
	}
	latePostingID := late["postings"].([]any)[0].(string)

	// 审计回溯该笔分录：调整分录、规则版本、原始事件齐全。
	code, out = doJSON(t, "GET", srv.URL+"/v1/audit/postings/"+latePostingID, financeHeaders(), nil)
	if code != http.StatusOK {
		t.Fatalf("审计查询失败：%d", code)
	}
	posting := out["posting"].(map[string]any)
	if posting["kind"] != "adjustment" || posting["period"] != "2026-10" || posting["rule_version"] != "v2026.09-a" {
		t.Fatalf("审计分录信息错误：%v", posting)
	}
	if out["source_event"].(map[string]any)["id"] != "rd-late" {
		t.Fatalf("审计缺少原始事件：%v", out)
	}

	// 复算与封账快照一致。
	code, out = doJSON(t, "POST", srv.URL+"/v1/periods/2026-09/verify", financeHeaders(), nil)
	if code != http.StatusOK || out["match"] != true {
		t.Fatalf("复算应一致：%d %v", code, out)
	}
	if code, out = doJSON(t, "GET", srv.URL+"/v1/periods/2026-09/snapshot", financeHeaders(), nil); code != http.StatusOK || out["postings"].(float64) != 1 {
		t.Fatalf("快照查询失败：%d %v", code, out)
	}
}

// 接口级并发核销：同一事件并发 POST 只入账一次。
func TestHTTPConcurrentRedemption(t *testing.T) {
	srv, _ := newTestServer(t)
	doJSON(t, "POST", srv.URL+"/v1/events", nil, map[string]any{"events": []map[string]any{
		{"id": "tk-c", "type": "ticket", "occurred_at": "2026-09-18T10:00:00+08:00",
			"payload": map[string]any{"ticket_token": "T-C", "event_id": "match-2026-09-18", "status": "issued"}},
		{"id": "gr-c", "type": "benefit_grant", "occurred_at": "2026-09-18T10:05:00+08:00",
			"payload": map[string]any{"benefit_id": "B-C", "program_code": "city-stay-a", "ticket_token": "T-C", "quota": 100000}},
	}})

	body := map[string]any{"events": []map[string]any{
		{"id": "rd-c", "type": "redemption", "occurred_at": "2026-09-18T20:00:00+08:00",
			"payload": map[string]any{"benefit_id": "B-C", "merchant_id": "M-hotel", "amount": 5000}},
	}}
	const n = 8
	statuses := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			code, out := doJSON(t, "POST", srv.URL+"/v1/events", nil, body)
			if code != http.StatusOK {
				statuses[i] = fmt.Sprintf("http-%d", code)
				return
			}
			statuses[i] = out["results"].([]any)[0].(map[string]any)["status"].(string)
		}(i)
	}
	wg.Wait()
	var applied, duplicated int
	for _, s := range statuses {
		switch s {
		case "applied":
			applied++
		case "duplicate":
			duplicated++
		}
	}
	if applied != 1 || duplicated != n-1 {
		t.Fatalf("并发核销应只入账一次：%v", statuses)
	}
}
