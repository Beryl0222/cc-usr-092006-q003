package settlement

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

// Service 在内存事件账本之上提供 HTTP 接口与角色数据范围隔离。
type Service struct {
	store *Store
	now   func() time.Time
}

func NewService(store *Store) *Service {
	return &Service{store: store, now: time.Now}
}

// SetClock 注入服务端时钟，测试中可固定为确定时间。
func (svc *Service) SetClock(f func() time.Time) {
	svc.now = f
}

// token 约定：Bearer platform | finance | organizer:<id> | merchant:<id>。
// 生产实现应替换为令牌表/网关鉴权；这里以无状态约定便于验收。
func parseToken(token string) (Scope, bool) {
	token = strings.TrimSpace(token)
	if token == "" {
		return Scope{}, false
	}
	parts := strings.SplitN(token, ":", 2)
	sc := Scope{Role: parts[0]}
	if len(parts) == 2 {
		sc.Identity = parts[1]
	}
	switch sc.Role {
	case RolePlatform, RoleFinance:
		return sc, true
	case RoleOrganizer, RoleMerchant:
		return sc, sc.Identity != ""
	}
	return Scope{}, false
}

func (svc *Service) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", svc.handleIngest)
	mux.HandleFunc("GET /v1/redemptions", svc.handleListRedemptions)
	mux.HandleFunc("GET /v1/redemptions/{id}", svc.handleGetRedemption)
	mux.HandleFunc("GET /v1/redemptions/{id}/audit", svc.handleAudit)
	mux.HandleFunc("GET /v1/periods", svc.handleSnapshots)
	mux.HandleFunc("GET /v1/periods/{period}/totals", svc.handleTotals)
	mux.HandleFunc("POST /v1/periods/{period}/close", svc.handleClose)
	mux.HandleFunc("GET /v1/periods/{period}/verify", svc.handleVerify)
	mux.HandleFunc("POST /v1/adjustments", svc.handleAdjustment)
	mux.HandleFunc("GET /v1/blocked", svc.handleBlocked)
	mux.HandleFunc("GET /v1/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return loggingRecovery(mux)
}

func (svc *Service) scope(r *http.Request) (Scope, bool) {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return Scope{}, false
	}
	return parseToken(strings.TrimPrefix(h, "Bearer "))
}

func (svc *Service) handleIngest(w http.ResponseWriter, r *http.Request) {
	sc, ok := svc.scope(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "缺少或非法的鉴权令牌")
		return
	}
	var env Envelope
	if err := decodeBody(r, &env); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !canIngest(sc, env.Type) {
		writeError(w, http.StatusForbidden, "该角色无权投递此类事件")
		return
	}
	// 商户只能上报自己门店的核销。
	if sc.Role == RoleMerchant {
		var p RedemptionRecorded
		if err := json.Unmarshal(env.Payload, &p); err == nil && p.MerchantID != sc.Identity {
			writeError(w, http.StatusForbidden, "商户只能上报本门店流水")
			return
		}
	}
	// 只有商户侧流水以服务端接收时间判定迟到上传；
	// 平台内部事件默认以业务发生时间为接收时间，回填历史不受宽限影响。
	if sc.Role == RoleMerchant && env.ReceivedAt.IsZero() {
		env.ReceivedAt = svc.now()
	}
	res, err := svc.store.Ingest(&env)
	if err != nil {
		writeIngestError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func canIngest(sc Scope, typ string) bool {
	switch sc.Role {
	case RolePlatform:
		return true
	case RoleMerchant:
		return typ == EvRedemptionRecorded || typ == EvRedemptionRefunded
	default:
		return false
	}
}

func writeIngestError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrIdentityField):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, ErrClosed):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, ErrInvalid), errors.Is(err, ErrUnknownType):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func (svc *Service) handleListRedemptions(w http.ResponseWriter, r *http.Request) {
	sc, ok := svc.scope(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "缺少或非法的鉴权令牌")
		return
	}
	rs := svc.store.ListRedemptions(sc)
	if rs == nil {
		rs = []RedemptionView{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"redemptions": rs})
}

func (svc *Service) handleGetRedemption(w http.ResponseWriter, r *http.Request) {
	sc, ok := svc.scope(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "缺少或非法的鉴权令牌")
		return
	}
	v, err := svc.store.Redemption(r.PathValue("id"), sc)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (svc *Service) handleAudit(w http.ResponseWriter, r *http.Request) {
	sc, ok := svc.scope(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "缺少或非法的鉴权令牌")
		return
	}
	chain, err := svc.store.Audit(r.PathValue("id"), sc)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, chain)
}

func (svc *Service) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	if _, ok := svc.scope(r); !ok {
		writeError(w, http.StatusUnauthorized, "缺少或非法的鉴权令牌")
		return
	}
	ps := svc.store.Snapshots()
	if ps == nil {
		ps = []PeriodClosed{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"periods": ps})
}

func (svc *Service) handleTotals(w http.ResponseWriter, r *http.Request) {
	sc, ok := svc.scope(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "缺少或非法的鉴权令牌")
		return
	}
	period := r.PathValue("period")
	totals := svc.store.Totals(period, sc)
	if totals == nil {
		totals = []Total{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"period": period, "totals": totals})
}

func (svc *Service) handleClose(w http.ResponseWriter, r *http.Request) {
	sc, ok := svc.scope(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "缺少或非法的鉴权令牌")
		return
	}
	if sc.Role != RoleFinance {
		writeError(w, http.StatusForbidden, "只有财务可以封账")
		return
	}
	period := r.PathValue("period")
	pc, err := svc.store.ClosePeriod(period, svc.now())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pc)
}

func (svc *Service) handleVerify(w http.ResponseWriter, r *http.Request) {
	sc, ok := svc.scope(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "缺少或非法的鉴权令牌")
		return
	}
	if sc.Role != RoleFinance && sc.Role != RolePlatform {
		writeError(w, http.StatusForbidden, "只有财务与平台可以复算快照")
		return
	}
	res, err := svc.store.VerifySnapshot(r.PathValue("period"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (svc *Service) handleAdjustment(w http.ResponseWriter, r *http.Request) {
	sc, ok := svc.scope(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "缺少或非法的鉴权令牌")
		return
	}
	if sc.Role != RoleFinance {
		writeError(w, http.StatusForbidden, "只有财务可以登记调整分录")
		return
	}
	var p AdjustmentPosted
	if err := decodeBody(r, &p); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	p.CreatedBy = RoleFinance
	res, err := svc.store.PostAdjustment(p)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (svc *Service) handleBlocked(w http.ResponseWriter, r *http.Request) {
	sc, ok := svc.scope(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "缺少或非法的鉴权令牌")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"blocked": nonNilBlocked(svc.store.Blocked(sc))})
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrForbidden):
		writeError(w, http.StatusForbidden, err.Error())
	case errors.Is(err, ErrClosed):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, ErrInvalid):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		// 查询类"不存在"也按 404 处理。
		writeError(w, http.StatusNotFound, err.Error())
	}
}

func decodeBody(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func nonNilBlocked(bs []BlockedEffect) []BlockedEffect {
	if bs == nil {
		return []BlockedEffect{}
	}
	return bs
}

// loggingRecovery 是最小化的 panic 恢复中间件，避免单条请求拖垮进程。
func loggingRecovery(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				writeError(w, http.StatusInternalServerError, "内部错误")
			}
		}()
		h.ServeHTTP(w, r)
	})
}
