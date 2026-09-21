package settlement

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// 数据访问角色：财务、主办方、商户三方数据范围彼此隔离。
const (
	RoleFinance   = "finance"
	RoleOrganizer = "organizer"
	RoleMerchant  = "merchant"
)

var (
	// ErrForbidden 表示调用方超出其数据范围。
	ErrForbidden      = errors.New("无权访问该数据范围")
	errUnknownPosting = errors.New("分录不存在")
)

type ticketState struct {
	Ref     string
	MatchID string
	Status  string // issued | refunded | cancelled
	Entered bool
}

type benefitState struct {
	ID          string
	ProgramCode string
	TicketRef   string
	Quota       Money
	Used        Money
}

type redemptionState struct {
	EventID         string
	BenefitID       string
	TicketRef       string
	MatchID         string
	ProgramCode     string
	Phase           string
	MerchantID      string
	Amount          Money
	Subsidy         Money
	Partner         Money
	PostingID       string
	Refunded        Money
	Reversed        Money
	ReversedPartner Money
}

// Engine 统一接收各类事件并维护归因台账，所有状态变更均在单锁下完成，并发安全。
type Engine struct {
	mu       sync.Mutex
	programs map[string]Program
	secret   []byte
	now      func() time.Time

	results  map[string]EventResult
	stored   map[string]StoredEvent
	tickets  map[string]*ticketState
	benefits map[string]*benefitState
	redems   map[string]*redemptionState
	postings []*Posting
	closed   map[string]bool
	snaps    map[string]Snapshot
	seq      int
}

// NewEngine 构建清算引擎。secret 用于身份脱敏；now 可注入以便测试，nil 时使用系统时间。
func NewEngine(programs []Program, secret []byte, now func() time.Time) (*Engine, error) {
	if len(secret) == 0 {
		return nil, errors.New("缺少脱敏密钥")
	}
	if now == nil {
		now = time.Now
	}
	e := &Engine{
		programs: map[string]Program{},
		secret:   secret,
		now:      now,
		results:  map[string]EventResult{},
		stored:   map[string]StoredEvent{},
		tickets:  map[string]*ticketState{},
		benefits: map[string]*benefitState{},
		redems:   map[string]*redemptionState{},
		closed:   map[string]bool{},
		snaps:    map[string]Snapshot{},
	}
	for _, p := range programs {
		if err := validateProgram(p); err != nil {
			return nil, err
		}
		if _, dup := e.programs[p.Code]; dup {
			return nil, fmt.Errorf("权益方案 %s 重复", p.Code)
		}
		e.programs[p.Code] = p
	}
	return e, nil
}

// ref 将外部身份标识脱敏为伪匿名引用（HMAC-SHA256），原始标识永不落盘。
func (e *Engine) ref(kind, token string) string {
	h := hmac.New(sha256.New, e.secret)
	h.Write([]byte(kind + ":" + token))
	return hex.EncodeToString(h.Sum(nil))
}

// Ingest 统一接收单个事件，按幂等键去重后分发处理。
func (e *Engine) Ingest(ev InboundEvent) EventResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	if ev.ID == "" {
		return EventResult{Status: "rejected", Reason: "missing_event_id"}
	}
	if prev, ok := e.results[ev.ID]; ok {
		dup := prev
		dup.Status = "duplicate"
		return dup
	}
	sanitized, err := e.sanitize(ev)
	if err != nil {
		return EventResult{Status: "rejected", Reason: "invalid_payload"}
	}
	var res EventResult
	switch ev.Type {
	case EventTicket:
		res = e.applyTicket(ev)
	case EventEntry:
		res = e.applyEntry(ev)
	case EventGrant:
		res = e.applyGrant(ev)
	case EventRedeem:
		res = e.applyRedemption(ev)
	case EventRefund:
		res = e.applyRefund(ev)
	case EventSettle:
		res = e.applySettlement(ev)
	default:
		res = EventResult{Status: "rejected", Reason: "unknown_event_type"}
	}
	e.results[ev.ID] = res
	e.stored[ev.ID] = StoredEvent{ID: ev.ID, Type: ev.Type, OccurredAt: ev.OccurredAt, Payload: sanitized}
	return res
}

// sanitize 将载荷中的明文身份标识替换为伪匿名引用后再落盘。
func (e *Engine) sanitize(ev InboundEvent) (json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(ev.Payload, &m); err != nil {
		return nil, err
	}
	if tok, ok := m["ticket_token"]; ok {
		var token string
		if err := json.Unmarshal(tok, &token); err != nil || token == "" {
			return nil, errors.New("ticket_token 无效")
		}
		delete(m, "ticket_token")
		ref, _ := json.Marshal(e.ref("ticket", token))
		m["ticket_ref"] = ref
	}
	return json.Marshal(m)
}

// placeLocked 为分录分配 ID 并落账。目标账期已封账时，分录转为调整分录，
// 落入当前未封账账期，已封账账期的数据保持不变。
func (e *Engine) placeLocked(p *Posting, occurredAt time.Time, loc *time.Location) {
	period := periodOf(occurredAt, loc)
	if e.closed[period] {
		if p.Kind != "adjustment" {
			p.Kind = "adjustment"
			p.Reason = strings.TrimSpace(p.Reason + " 补入已封账账期" + period)
		}
		period = periodOf(e.now(), loc)
		for e.closed[period] {
			period = nextPeriod(period)
		}
	}
	e.seq++
	p.ID = fmt.Sprintf("P%06d", e.seq)
	p.Period = period
	e.postings = append(e.postings, p)
}

func (e *Engine) findPostingLocked(id string) *Posting {
	for _, p := range e.postings {
		if p.ID == id {
			return p
		}
	}
	return nil
}

func (e *Engine) applyTicket(ev InboundEvent) EventResult {
	var p TicketPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return EventResult{Status: "rejected", Reason: "invalid_payload"}
	}
	if p.TicketToken == "" {
		return EventResult{Status: "rejected", Reason: "missing_ticket"}
	}
	ref := e.ref("ticket", p.TicketToken)
	t, ok := e.tickets[ref]
	switch p.Status {
	case "issued":
		if !ok {
			e.tickets[ref] = &ticketState{Ref: ref, MatchID: p.EventID, Status: "issued"}
		}
		return EventResult{Status: "applied"}
	case "refunded", "cancelled":
		if !ok {
			return EventResult{Status: "rejected", Reason: "unknown_ticket"}
		}
		if t.Status == "refunded" || t.Status == "cancelled" {
			return EventResult{Status: "applied", Reason: "already_void"}
		}
		t.Status = p.Status
		// 退票/取消后，该票在对应场次下已核销的补贴全额冲正。
		ids := e.reverseTicketLocked(t, t.MatchID, ev.ID, "ticket_"+p.Status, ev.OccurredAt, nil)
		return EventResult{Status: "applied", Postings: ids}
	case "rescheduled":
		if !ok {
			return EventResult{Status: "rejected", Reason: "unknown_ticket"}
		}
		if p.NewEventID == "" {
			return EventResult{Status: "rejected", Reason: "missing_new_event"}
		}
		old := t.MatchID
		t.MatchID = p.NewEventID
		// 改期是否冲正由各方案规则决定。
		ids := e.reverseTicketLocked(t, old, ev.ID, "ticket_rescheduled", ev.OccurredAt,
			func(prog Program) bool { return prog.Rules.OnReschedule == "reverse" })
		return EventResult{Status: "applied", Postings: ids}
	default:
		return EventResult{Status: "rejected", Reason: "invalid_status"}
	}
}

func (e *Engine) applyEntry(ev InboundEvent) EventResult {
	var p EntryPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return EventResult{Status: "rejected", Reason: "invalid_payload"}
	}
	if p.TicketToken == "" {
		return EventResult{Status: "rejected", Reason: "missing_ticket"}
	}
	ref := e.ref("ticket", p.TicketToken)
	t, ok := e.tickets[ref]
	if !ok {
		// 入场事件可能先于出票事件到达，补建票据档案。
		t = &ticketState{Ref: ref, MatchID: p.EventID, Status: "issued"}
		e.tickets[ref] = t
	}
	if t.Status != "issued" {
		return EventResult{Status: "rejected", Reason: "ticket_void"}
	}
	t.Entered = true
	return EventResult{Status: "applied"}
}

func (e *Engine) applyGrant(ev InboundEvent) EventResult {
	var p GrantPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return EventResult{Status: "rejected", Reason: "invalid_payload"}
	}
	if p.BenefitID == "" || p.TicketToken == "" || p.Quota <= 0 {
		return EventResult{Status: "rejected", Reason: "invalid_grant"}
	}
	if _, ok := e.programs[p.ProgramCode]; !ok {
		return EventResult{Status: "rejected", Reason: "unknown_program"}
	}
	if _, dup := e.benefits[p.BenefitID]; dup {
		return EventResult{Status: "rejected", Reason: "duplicate_benefit"}
	}
	e.benefits[p.BenefitID] = &benefitState{
		ID:          p.BenefitID,
		ProgramCode: p.ProgramCode,
		TicketRef:   e.ref("ticket", p.TicketToken),
		Quota:       p.Quota,
	}
	return EventResult{Status: "applied"}
}

func (e *Engine) applyRedemption(ev InboundEvent) EventResult {
	var p RedeemPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return EventResult{Status: "rejected", Reason: "invalid_payload"}
	}
	if p.Amount <= 0 || p.MerchantID == "" {
		return EventResult{Status: "rejected", Reason: "invalid_amount"}
	}
	b, ok := e.benefits[p.BenefitID]
	if !ok {
		return EventResult{Status: "rejected", Reason: "unknown_benefit"}
	}
	prog := e.programs[b.ProgramCode]
	if b.Used+p.Amount > b.Quota {
		return EventResult{Status: "rejected", Reason: "quota_exceeded"}
	}
	if t, ok := e.tickets[b.TicketRef]; ok {
		if t.Status == "refunded" || t.Status == "cancelled" {
			return EventResult{Status: "rejected", Reason: "ticket_void"}
		}
		if t.MatchID != prog.EventID {
			return EventResult{Status: "rejected", Reason: "ticket_moved"}
		}
	}
	// 按业务发生时间在方案时区下归因阶段，跨午夜流水归入正确场次。
	phase := prog.phaseAt(ev.OccurredAt)
	if phase == nil {
		return EventResult{Status: "rejected", Reason: "outside_window"}
	}
	subsidy := bpsOf(p.Amount, prog.Rules.SubsidyBps)
	partner := bpsOf(subsidy, prog.Rules.PartnerShareBps)
	posting := &Posting{
		Kind:        "standard",
		MatchID:     prog.EventID,
		ProgramCode: prog.Code,
		Phase:       phase.Name,
		MerchantID:  p.MerchantID,
		SourceEvent: ev.ID,
		RuleVersion: prog.Rules.Version,
		Gross:       p.Amount,
		Subsidy:     subsidy,
		Lines: []Line{
			{Account: "receivable:partner:" + prog.EventID, Debit: partner},
			{Account: "expense:organizer:" + prog.Code, Debit: subsidy - partner},
			{Account: "payable:merchant:" + p.MerchantID, Credit: subsidy},
		},
	}
	e.placeLocked(posting, ev.OccurredAt, prog.location())
	ids := []string{posting.ID}
	r := &redemptionState{
		EventID: ev.ID, BenefitID: b.ID, TicketRef: b.TicketRef,
		MatchID: prog.EventID, ProgramCode: prog.Code, Phase: phase.Name,
		MerchantID: p.MerchantID, Amount: p.Amount, Subsidy: subsidy,
		Partner: partner, PostingID: posting.ID,
	}
	e.redems[ev.ID] = r
	b.Used += p.Amount
	// 赛后延时消费按方案规则处理：reverse 时立即全额冲正，保留审计轨迹。
	if phase.PostEvent && prog.Rules.PostEventPolicy == "reverse" {
		ids = append(ids, e.reverseRedemptionLocked(r, ev.ID, "post_event_policy",
			subsidy, partner, p.Amount, ev.OccurredAt))
		r.Reversed = subsidy
		r.ReversedPartner = partner
		r.Refunded = r.Amount
	}
	return EventResult{Status: "applied", Postings: ids}
}

func (e *Engine) applyRefund(ev InboundEvent) EventResult {
	var p RefundPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return EventResult{Status: "rejected", Reason: "invalid_payload"}
	}
	r, ok := e.redems[p.RedemptionEventID]
	if !ok {
		return EventResult{Status: "rejected", Reason: "unknown_redemption"}
	}
	if p.Amount <= 0 || r.Refunded+p.Amount > r.Amount {
		return EventResult{Status: "rejected", Reason: "refund_exceeds"}
	}
	full := r.Refunded+p.Amount == r.Amount
	var rev, partnerRev Money
	if full {
		// 全额退清时冲正剩余全部补贴，消除多次部分退款的取整尾差。
		rev = r.Subsidy - r.Reversed
		partnerRev = r.Partner - r.ReversedPartner
	} else {
		rev = Money(int64(r.Subsidy) * int64(p.Amount) / int64(r.Amount))
		partnerRev = Money(int64(r.Partner) * int64(p.Amount) / int64(r.Amount))
	}
	r.Refunded += p.Amount
	r.Reversed += rev
	r.ReversedPartner += partnerRev
	if rev == 0 {
		return EventResult{Status: "applied"}
	}
	id := e.reverseRedemptionLocked(r, ev.ID, "refund", rev, partnerRev, p.Amount, ev.OccurredAt)
	return EventResult{Status: "applied", Postings: []string{id}}
}

func (e *Engine) applySettlement(ev InboundEvent) EventResult {
	var p SettlePayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return EventResult{Status: "rejected", Reason: "invalid_payload"}
	}
	if _, err := time.Parse("2006-01", p.Period); err != nil {
		return EventResult{Status: "rejected", Reason: "invalid_period"}
	}
	e.closePeriodLocked(p.Period)
	return EventResult{Status: "applied"}
}

// reverseRedemptionLocked 生成一笔冲正分录（rev/partnerRev/gross 为正数，记账时取反）。
func (e *Engine) reverseRedemptionLocked(r *redemptionState, sourceEventID, reason string,
	rev, partnerRev, gross Money, occurredAt time.Time) string {
	prog := e.programs[r.ProgramCode]
	p := &Posting{
		Kind:        "reversal",
		MatchID:     r.MatchID,
		ProgramCode: r.ProgramCode,
		Phase:       r.Phase,
		MerchantID:  r.MerchantID,
		SourceEvent: sourceEventID,
		Reverses:    r.PostingID,
		RuleVersion: prog.Rules.Version,
		Reason:      reason,
		Gross:       -gross,
		Subsidy:     -rev,
		Lines: []Line{
			{Account: "receivable:partner:" + r.MatchID, Credit: partnerRev},
			{Account: "expense:organizer:" + r.ProgramCode, Credit: rev - partnerRev},
			{Account: "payable:merchant:" + r.MerchantID, Debit: rev},
		},
	}
	e.placeLocked(p, occurredAt, prog.location())
	return p.ID
}

// reverseTicketLocked 冲正某张票在指定场次下全部未冲正的核销补贴；onlyIf 可按方案规则过滤。
func (e *Engine) reverseTicketLocked(t *ticketState, matchID, sourceEventID, reason string,
	occurredAt time.Time, onlyIf func(Program) bool) []string {
	var targets []*redemptionState
	for _, r := range e.redems {
		if r.TicketRef == t.Ref && r.MatchID == matchID {
			targets = append(targets, r)
		}
	}
	// 固定处理顺序，保证分录编号确定。
	sort.Slice(targets, func(i, j int) bool { return targets[i].EventID < targets[j].EventID })
	var ids []string
	for _, r := range targets {
		prog := e.programs[r.ProgramCode]
		if onlyIf != nil && !onlyIf(prog) {
			continue
		}
		remaining := r.Subsidy - r.Reversed
		if remaining <= 0 {
			continue
		}
		gross := r.Amount - r.Refunded
		ids = append(ids, e.reverseRedemptionLocked(r, sourceEventID, reason,
			remaining, r.Partner-r.ReversedPartner, gross, occurredAt))
		r.Reversed = r.Subsidy
		r.ReversedPartner = r.Partner
		r.Refunded = r.Amount
	}
	return ids
}

func (e *Engine) closePeriodLocked(period string) Snapshot {
	if s, ok := e.snaps[period]; ok {
		return s
	}
	s := computeSnapshot(e.postings, period)
	e.closed[period] = true
	e.snaps[period] = s
	return s
}

// ClosePeriod 封账指定账期并生成快照；重复封账幂等返回既有快照。
func (e *Engine) ClosePeriod(period string) (Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := time.Parse("2006-01", period); err != nil {
		return Snapshot{}, fmt.Errorf("账期格式无效")
	}
	return e.closePeriodLocked(period), nil
}

// Snapshot 返回已封账账期的快照。
func (e *Engine) Snapshot(period string) (Snapshot, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.snaps[period]
	return s, ok
}

// VerifySnapshot 依据当前台账复算指定账期，校验与封账快照是否一致。
func (e *Engine) VerifySnapshot(period string) (stored, actual Snapshot, match bool, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	stored, ok := e.snaps[period]
	if !ok {
		return Snapshot{}, Snapshot{}, false, fmt.Errorf("账期 %s 尚未封账", period)
	}
	actual = computeSnapshot(e.postings, period)
	return stored, actual, stored.Hash == actual.Hash && stored.Postings == actual.Postings, nil
}

// AdjustmentInput 是封账后修正已封账账期的调整分录请求。
type AdjustmentInput struct {
	Reason string   `json:"reason"`
	Refs   []string `json:"refs"`  // 被修正的分录，必须属于已封账账期
	Lines  []Line   `json:"lines"` // 借贷必须平衡
}

// AddAdjustment 以调整分录修正已封账账期，分录落入当前未封账账期。
func (e *Engine) AddAdjustment(in AdjustmentInput) (*Posting, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(in.Lines) == 0 {
		return nil, errors.New("调整分录缺少记账行")
	}
	var dr, cr Money
	for _, l := range in.Lines {
		if l.Account == "" || (l.Debit == 0) == (l.Credit == 0) {
			return nil, errors.New("记账行无效")
		}
		dr += l.Debit
		cr += l.Credit
	}
	if dr != cr {
		return nil, errors.New("调整分录借贷不平衡")
	}
	loc := time.UTC
	for _, ref := range in.Refs {
		p := e.findPostingLocked(ref)
		if p == nil {
			return nil, errUnknownPosting
		}
		if !e.closed[p.Period] {
			return nil, fmt.Errorf("账期 %s 未封账，可直接补传事件", p.Period)
		}
		if prog, ok := e.programs[p.ProgramCode]; ok {
			loc = prog.location()
		}
	}
	p := &Posting{
		Kind:        "adjustment",
		Reason:      in.Reason,
		Refs:        in.Refs,
		Lines:       in.Lines,
		SourceEvent: "manual",
	}
	e.placeLocked(p, e.now(), loc)
	return p, nil
}

// Posting 按 ID 查询分录。
func (e *Engine) Posting(id string) (Posting, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.findPostingLocked(id)
	if p == nil {
		return Posting{}, false
	}
	return *p, true
}

// StoredEvent 按事件 ID 查询脱敏后的原始事件。
func (e *Engine) StoredEvent(id string) (StoredEvent, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	se, ok := e.stored[id]
	return se, ok
}
