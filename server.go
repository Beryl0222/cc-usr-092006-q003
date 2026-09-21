package settlement

import (
	"encoding/json"
	"errors"
	"net/http"
)

// NewHandler 暴露清算引擎的 HTTP 接口。调用方角色通过请求头声明：
// X-Actor-Role（finance|organizer|merchant）、X-Actor-Merchant、X-Actor-Match。
// 生产部署时应在网关侧完成身份认证后再注入这些头。
func NewHandler(e *Engine) http.Handler {
	mux := http.NewServeMux()

	// 统一事件接入（可批量）。
	mux.HandleFunc("POST /v1/events", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Events []InboundEvent `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "请求体无效")
			return
		}
		type item struct {
			ID       string   `json:"id"`
			Status   string   `json:"status"`
			Reason   string   `json:"reason,omitempty"`
			Postings []string `json:"postings,omitempty"`
		}
		resp := struct {
			Results []item `json:"results"`
		}{Results: []item{}}
		for _, ev := range req.Events {
			res := e.Ingest(ev)
			resp.Results = append(resp.Results, item{
				ID: ev.ID, Status: res.Status, Reason: res.Reason, Postings: res.Postings,
			})
		}
		writeJSON(w, http.StatusOK, resp)
	})

	// 账簿查询，按角色隔离数据范围。
	mux.HandleFunc("GET /v1/ledger", func(w http.ResponseWriter, r *http.Request) {
		lines, err := e.LedgerView(scopeOf(r), r.URL.Query().Get("period"))
		if err != nil {
			writeScopeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"lines": lines})
	})

	// 带动消费聚合，供主办方与城市合作伙伴核对。
	mux.HandleFunc("GET /v1/consumption", func(w http.ResponseWriter, r *http.Request) {
		matchID := r.URL.Query().Get("match_id")
		if matchID == "" {
			writeError(w, http.StatusBadRequest, "缺少 match_id")
			return
		}
		rows, err := e.Consumption(scopeOf(r), matchID)
		if err != nil {
			writeScopeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"rows": rows})
	})

	// 封账指定账期并生成快照（仅财务）。
	mux.HandleFunc("POST /v1/periods/{period}/close", func(w http.ResponseWriter, r *http.Request) {
		if !requireRole(w, r, RoleFinance) {
			return
		}
		snap, err := e.ClosePeriod(r.PathValue("period"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, snap)
	})

	// 查询账期快照（仅财务）。
	mux.HandleFunc("GET /v1/periods/{period}/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if !requireRole(w, r, RoleFinance) {
			return
		}
		snap, ok := e.Snapshot(r.PathValue("period"))
		if !ok {
			writeError(w, http.StatusNotFound, "账期尚未封账")
			return
		}
		writeJSON(w, http.StatusOK, snap)
	})

	// 复算账期并与封账快照比对（仅财务）。
	mux.HandleFunc("POST /v1/periods/{period}/verify", func(w http.ResponseWriter, r *http.Request) {
		if !requireRole(w, r, RoleFinance) {
			return
		}
		stored, actual, match, err := e.VerifySnapshot(r.PathValue("period"))
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"match": match, "stored": stored, "actual": actual,
		})
	})

	// 封账后调整分录（仅财务）。
	mux.HandleFunc("POST /v1/adjustments", func(w http.ResponseWriter, r *http.Request) {
		if !requireRole(w, r, RoleFinance) {
			return
		}
		var in AdjustmentInput
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeError(w, http.StatusBadRequest, "请求体无效")
			return
		}
		p, err := e.AddAdjustment(in)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, p)
	})

	// 审计回溯：分录 → 原始事件 → 规则版本 → 冲正/调整链（仅财务）。
	mux.HandleFunc("GET /v1/audit/postings/{id}", func(w http.ResponseWriter, r *http.Request) {
		if !requireRole(w, r, RoleFinance) {
			return
		}
		ex, err := e.Explain(r.PathValue("id"))
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, ex)
	})

	return mux
}

func scopeOf(r *http.Request) Scope {
	return Scope{
		Role:       r.Header.Get("X-Actor-Role"),
		MerchantID: r.Header.Get("X-Actor-Merchant"),
		MatchID:    r.Header.Get("X-Actor-Match"),
	}
}

func requireRole(w http.ResponseWriter, r *http.Request, roles ...string) bool {
	role := r.Header.Get("X-Actor-Role")
	for _, x := range roles {
		if x == role {
			return true
		}
	}
	writeError(w, http.StatusForbidden, "无权访问该数据范围")
	return false
}

func writeScopeError(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrForbidden) {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	writeError(w, http.StatusBadRequest, err.Error())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
