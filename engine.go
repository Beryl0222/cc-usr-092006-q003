package settlement

import (
	"bytes"
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

// 账户：应收（主办方）与应付（商户）。
const (
	AccountAROrganizer = "AR_ORGANIZER"
	AccountAPMerchant  = "AP_MERCHANT"
)

// 角色常量。
const (
	RolePlatform  = "platform"
	RoleFinance   = "finance"
	RoleOrganizer = "organizer"
	RoleMerchant  = "merchant"
)

var (
	ErrClosed        = errors.New("账期已封账，只能通过调整分录修正")
	ErrDuplicate     = errors.New("事件重复")
	ErrInvalid       = errors.New("事件校验失败")
	ErrUnknownType   = errors.New("未知事件类型")
	ErrIdentityField = errors.New("载荷包含不允许采集的身份字段")
	ErrForbidden     = errors.New("无权访问该数据范围")
)

// Catalog 是相对静态的基础资料。
type Catalog struct {
	Programs map[string]Program
	Matches  map[string]Match
	Rules    []RuleSet // 已按生效时间排序
}

func BuildCatalog(programs []Program, matches []Match, rules []RuleSet) (*Catalog, error) {
	if len(rules) == 0 {
		return nil, fmt.Errorf("%w：缺少规则版本", ErrInvalid)
	}
	c := &Catalog{Programs: map[string]Program{}, Matches: map[string]Match{}, Rules: append([]RuleSet(nil), rules...)}
	for _, m := range matches {
		c.Matches[m.EventID] = m
	}
	for _, p := range programs {
		c.Programs[p.Code] = p
		if _, ok := c.Matches[p.EventID]; !ok {
			return nil, fmt.Errorf("%w：方案 %s 引用了未知赛事 %s", ErrInvalid, p.Code, p.EventID)
		}
	}
	sort.Slice(c.Rules, func(i, j int) bool { return c.Rules[i].EffectiveAt.Before(c.Rules[j].EffectiveAt) })
	return c, nil
}

// RuleAt 返回业务时间 t 适用的规则版本。
func (c *Catalog) RuleAt(t time.Time) RuleSet {
	chosen := c.Rules[0]
	for _, r := range c.Rules {
		if !r.EffectiveAt.After(t) {
			chosen = r
		}
	}
	return chosen
}

// Voucher 是账本凭证。补贴/冲正各展开为两条 Posting；
// 调整分录使用财务显式给出的多行，借贷必须平衡。
type Voucher struct {
	Seq          int64     `json:"seq"`
	Period       string    `json:"period"`
	Kind         string    `json:"kind"`
	EventID      string    `json:"event_id"`
	RedemptionID string    `json:"redemption_id,omitempty"`
	MatchID      string    `json:"match_id,omitempty"`
	Stage        string    `json:"stage,omitempty"`
	ProgramCode  string    `json:"program_code,omitempty"`
	MerchantID   string    `json:"merchant_id,omitempty"`
	OrganizerID  string    `json:"organizer_id,omitempty"`
	GrossMin     int64     `json:"gross_min"`  // 有符号消费本金
	AmountMin    int64     `json:"amount_min"` // 有符号补贴净额
	Reason       string    `json:"reason,omitempty"`
	RuleVersion  string    `json:"rule_version,omitempty"`
	PostedAt     time.Time `json:"posted_at"`
	Lines        []Posting `json:"lines"`
}

// BlockedEffect 记录因账期封账而无法自动入账的业务影响，等待财务调整分录承接。
type BlockedEffect struct {
	OriginEventID string    `json:"origin_event_id"`
	RedemptionID  string    `json:"redemption_id,omitempty"`
	NaturalPeriod string    `json:"natural_period"`
	MatchID       string    `json:"match_id,omitempty"`
	ProgramCode   string    `json:"program_code,omitempty"`
	MerchantID    string    `json:"merchant_id,omitempty"`
	OrganizerID   string    `json:"organizer_id,omitempty"`
	AmountMin     int64     `json:"amount_min"` // 有符号：补记补贴为正，冲回为负
	GrossMin      int64     `json:"gross_min"`
	Kind          string    `json:"kind"`
	Reason        string    `json:"reason"`
	DetectedAt    time.Time `json:"detected_at"`
	AdjustmentID  string    `json:"adjustment_id,omitempty"`
}

type IngestResult struct {
	EventID string   `json:"event_id"`
	Seq     int64    `json:"seq"`
	Outcome string   `json:"outcome"` // accepted | duplicate | rejected
	Reasons []string `json:"reasons,omitempty"`
	Blocked bool     `json:"blocked,omitempty"`
}

// Store 是内存事件仓库与账本。所有派生状态均可由事件日志确定性重放。
type Store struct {
	mu      sync.Mutex
	catalog *Catalog
	loc     *time.Location

	seq    int64
	events []*Envelope

	facts    map[string]*redemptionFact
	vouchers []*Voucher
	blocked  []*BlockedEffect
	views    map[string]*RedemptionView
	closes   map[string]*PeriodClosed
}

func NewStore(catalog *Catalog) (*Store, error) {
	tz := catalog.Rules[0].AccountTimezone
	loc, err := time.LoadLocation(tz)
	if err != nil {
		if !(strings.HasPrefix(tz, "+") || strings.HasPrefix(tz, "-")) {
			return nil, err
		}
		var sign, h, m int
		sign = 1
		if strings.HasPrefix(tz, "-") {
			sign = -1
		}
		if _, err := fmt.Sscanf(tz[1:], "%d:%d", &h, &m); err != nil {
			return nil, err
		}
		loc = time.FixedZone(tz, sign*(h*3600+m*60))
	}
	return &Store{
		catalog: catalog, loc: loc,
		facts:  map[string]*redemptionFact{},
		views:  map[string]*RedemptionView{},
		closes: map[string]*PeriodClosed{},
	}, nil
}

// ---------- 第一遍：业务时间投影（与事件到达顺序无关） ----------

type ticketState struct {
	exists     bool
	face       int64
	matchID    string
	refunds    []TicketRefunded
	reschedule *TicketRescheduled
}

type grantState struct {
	g         BenefitGranted
	revokedAt *time.Time
}

type matchState struct {
	cancelledAt *time.Time
	reschedules []MatchRescheduled
}

type rImpact struct {
	triggerEventID string
	at             time.Time
	amount         int64 // 本次新增冲正额（正数）
	gross          int64 // 对应的消费本金冲减（有符号）
	reason         string
}

type redemptionFact struct {
	rec           RedemptionRecorded
	originEventID string
	ticketNo      string
	occurred      time.Time
	uploaded      time.Time
	accepted      bool
	late          bool // 上传晚于窗口+宽限；是否入账由自然账期是否封账决定
	status        string
	reasons       []string
	subsidy       int64
	matchID       string
	stage         string
	ruleVer       string
	reschedKeep   bool
	impacts       []rImpact
}

func (s *Store) project() {
	tickets := map[string]*ticketState{}
	grants := map[string]*grantState{}
	access := map[string][]AccessVerified{}
	matches := map[string]*matchState{}
	var reds []RedemptionRecorded
	redEnv := map[string]*Envelope{}
	rRefunds := map[string][]RedemptionRefunded{}
	// 业务键去重：迟到重传的同一笔退款（不同 event_id、相同票/流水+时间+金额）
	// 不能造成二次冲正。
	ticketRefundSeen := map[string]bool{}
	redRefundSeen := map[string]bool{}

	// 第一遍：建立实体，保证退款等"迟到事件"与其引用的发票/权益以何种顺序到达都不丢。
	for _, e := range s.events {
		switch e.Type {
		case EvTicketIssued:
			var p TicketIssued
			_ = json.Unmarshal(e.Payload, &p)
			t := tickets[p.TicketNo]
			if t == nil {
				t = &ticketState{}
				tickets[p.TicketNo] = t
			}
			t.exists = true
			t.face, t.matchID = p.FaceMin, p.MatchID
		case EvBenefitGranted:
			var p BenefitGranted
			_ = json.Unmarshal(e.Payload, &p)
			grants[p.GrantID] = &grantState{g: p}
		case EvRedemptionRecorded:
			var p RedemptionRecorded
			_ = json.Unmarshal(e.Payload, &p)
			if _, dup := redEnv[p.RedemptionID]; !dup {
				reds = append(reds, p)
				redEnv[p.RedemptionID] = e
			}
		}
	}

	// 第二遍：应用生命周期事件。
	for _, e := range s.events {
		switch e.Type {
		case EvTicketIssued, EvBenefitGranted, EvRedemptionRecorded:
			continue
		case EvTicketRefunded:
			var p TicketRefunded
			_ = json.Unmarshal(e.Payload, &p)
			key := fmt.Sprintf("%s|%d|%d", p.TicketNo, p.RefundedAt.UnixNano(), p.RefundMin)
			if ticketRefundSeen[key] {
				continue
			}
			ticketRefundSeen[key] = true
			t := tickets[p.TicketNo]
			if t == nil {
				t = &ticketState{}
				tickets[p.TicketNo] = t
			}
			t.refunds = append(t.refunds, p)
		case EvTicketRescheduled:
			var p TicketRescheduled
			_ = json.Unmarshal(e.Payload, &p)
			t := tickets[p.TicketNo]
			if t == nil {
				t = &ticketState{}
				tickets[p.TicketNo] = t
			}
			cp := p
			t.reschedule = &cp
			t.matchID = p.ToMatchID
		case EvMatchRescheduled:
			var p MatchRescheduled
			_ = json.Unmarshal(e.Payload, &p)
			m := matches[p.MatchID]
			if m == nil {
				m = &matchState{}
				matches[p.MatchID] = m
			}
			m.reschedules = append(m.reschedules, p)
		case EvMatchCancelled:
			var p MatchCancelled
			_ = json.Unmarshal(e.Payload, &p)
			m := matches[p.MatchID]
			if m == nil {
				m = &matchState{}
				matches[p.MatchID] = m
			}
			at := p.CancelledAt
			m.cancelledAt = &at
		case EvAccessVerified:
			var p AccessVerified
			_ = json.Unmarshal(e.Payload, &p)
			access[p.TicketNo] = append(access[p.TicketNo], p)
		case EvBenefitRevoked:
			var p BenefitRevoked
			_ = json.Unmarshal(e.Payload, &p)
			if g := grants[p.GrantID]; g != nil {
				at := p.At
				g.revokedAt = &at
			}
		case EvRedemptionRefunded:
			var p RedemptionRefunded
			_ = json.Unmarshal(e.Payload, &p)
			key := fmt.Sprintf("%s|%d|%d", p.RedemptionID, p.RefundedAt.UnixNano(), p.RefundMin)
			if redRefundSeen[key] {
				continue
			}
			redRefundSeen[key] = true
			rRefunds[p.RedemptionID] = append(rRefunds[p.RedemptionID], p)
		}
	}

	// 单次权益竞争：按业务时间排序，同一时刻以核销 ID 兜底，保证与摄入顺序无关。
	sort.Slice(reds, func(i, j int) bool {
		ti, tj := redEnv[reds[i].RedemptionID].OccurredAt, redEnv[reds[j].RedemptionID].OccurredAt
		if ti.Equal(tj) {
			return reds[i].RedemptionID < reds[j].RedemptionID
		}
		return ti.Before(tj)
	})

	s.facts = map[string]*redemptionFact{}
	consumed := map[string]string{} // grantID -> 最先消费的核销 ID

	for _, r := range reds {
		env := redEnv[r.RedemptionID]
		f := s.evaluate(r, env.OccurredAt, env.ReceivedAt, tickets, grants, access, matches, consumed)
		f.originEventID = env.EventID
		s.facts[r.RedemptionID] = f
		if f.accepted {
			if gid := f.grantID(); gid != "" {
				if _, taken := consumed[gid]; !taken {
					consumed[gid] = r.RedemptionID
				}
			}
			s.buildImpacts(f, rRefunds[r.RedemptionID], tickets, matches)
		}
	}
}

func (f *redemptionFact) grantID() string {
	if f.rec.GrantID != "" {
		return f.rec.GrantID
	}
	return ""
}

func findGrant(r RedemptionRecorded, grants map[string]*grantState) (*grantState, string) {
	if r.GrantID != "" {
		return grants[r.GrantID], r.GrantID
	}
	for id, g := range grants {
		if g.g.TicketNo == r.TicketNo && g.g.ProgramCode == r.ProgramCode {
			return g, id
		}
	}
	return nil, ""
}

func (s *Store) evaluate(r RedemptionRecorded, at, received time.Time,
	tickets map[string]*ticketState, grants map[string]*grantState,
	access map[string][]AccessVerified, matches map[string]*matchState,
	consumed map[string]string) *redemptionFact {

	f := &redemptionFact{rec: r, ticketNo: r.TicketNo, occurred: at, uploaded: received, status: StatusRejected}
	rule := s.catalog.RuleAt(at)
	f.ruleVer = rule.Version

	program, ok := s.catalog.Programs[r.ProgramCode]
	if !ok {
		f.reasons = append(f.reasons, "unknown_program")
		return f
	}
	match, ok := s.catalog.Matches[program.EventID]
	if !ok {
		f.reasons = append(f.reasons, "unknown_match")
		return f
	}
	f.matchID = match.EventID

	if r.AmountMin <= 0 || r.Currency != program.Currency {
		f.reasons = append(f.reasons, "amount_or_currency_invalid")
		return f
	}
	// 跨午夜归因：只看消费时间落在哪个方案窗口，
	// 商户在午夜之后上传不会把流水算到次日场次。
	if at.Before(program.StartsAt) || at.After(program.EndsAt) {
		f.reasons = append(f.reasons, "outside_program_window")
		return f
	}
	// 迟到宽限：上传晚于窗口结束 + 宽限分钟先标记迟到。
	// 是否拒绝取决于入账时自然账期是否已封账：封账则挂起走调整，开放账期才直接拒绝。
	if !received.IsZero() && received.After(program.EndsAt.Add(time.Duration(rule.LateUploadGraceMinutes)*time.Minute)) {
		f.late = true
		f.reasons = append(f.reasons, "late_upload")
	}

	grant, gid := findGrant(r, grants)
	if grant == nil {
		f.reasons = append(f.reasons, "benefit_not_granted")
		return f
	}
	f.ticketNo = grant.g.TicketNo
	if grant.g.ProgramCode != r.ProgramCode {
		f.reasons = append(f.reasons, "grant_program_mismatch")
		return f
	}
	ticket := tickets[grant.g.TicketNo]
	if ticket == nil || !ticket.exists {
		f.reasons = append(f.reasons, "ticket_missing")
		return f
	}
	if r.TicketNo != "" && r.TicketNo != grant.g.TicketNo {
		f.reasons = append(f.reasons, "ticket_grant_mismatch")
		return f
	}
	if !grant.g.IssuedAt.IsZero() && at.Before(grant.g.IssuedAt) {
		f.reasons = append(f.reasons, "redemption_before_grant")
		return f
	}
	if grant.revokedAt != nil && !at.Before(*grant.revokedAt) {
		f.reasons = append(f.reasons, "benefit_revoked")
		return f
	}
	// 退票后继续核销：消费前已全额退票则直接失效；部分退票按未退比例缩放补贴。
	refundedBefore := int64(0)
	for _, rf := range ticket.refunds {
		if !rf.RefundedAt.After(at) {
			refundedBefore += rf.RefundMin
		}
	}
	if ticket.face > 0 && refundedBefore >= ticket.face {
		f.reasons = append(f.reasons, "ticket_refunded")
		return f
	}
	// 改期保留：票转到新场次后，旧场次权益不再可核销；改期前的核销不受影响。
	if ticket.reschedule != nil && !at.Before(ticket.reschedule.At) && ticket.matchID != program.EventID {
		f.reasons = append(f.reasons, "ticket_rescheduled_to_other_event")
		return f
	}
	if program.EntryRequired {
		verified := false
		for _, av := range access[grant.g.TicketNo] {
			if av.MatchID != program.EventID {
				continue
			}
			if !av.VerifiedAt.After(at.Add(time.Duration(rule.EntryGraceMinutes) * time.Minute)) {
				verified = true
				break
			}
		}
		if !verified {
			f.reasons = append(f.reasons, "entry_not_verified")
			return f
		}
	}
	if grant.g.SingleUse {
		if winner, taken := consumed[gid]; taken && winner != r.RedemptionID {
			f.reasons = append(f.reasons, "single_use_already_consumed")
			return f
		}
	}
	// 赛事在消费前取消：不补贴。
	if ms := matches[match.EventID]; ms != nil && ms.cancelledAt != nil && !at.Before(*ms.cancelledAt) {
		f.reasons = append(f.reasons, "match_cancelled")
		return f
	}

	// 活动阶段：以消费时点有效的赛程（含官方改期）判定。
	start, end := match.StartsAt, match.EndsAt
	if ms := matches[match.EventID]; ms != nil {
		for _, rs := range ms.reschedules {
			if !rs.At.After(at) {
				start, end = rs.NewStartsAt, rs.NewEndsAt
			}
		}
	}
	switch {
	case at.Before(start):
		f.stage = StagePreMatch
	case at.After(end):
		f.stage = StagePostMatch
	default:
		f.stage = StageInMatch
	}
	if f.stage == StagePostMatch && !rule.PostMatchRetention {
		f.reasons = append(f.reasons, "post_match_not_retained")
		return f
	}

	subsidyFull := r.AmountMin * int64(program.Subsidy.BPS) / 10000
	if program.Subsidy.CapMin > 0 && subsidyFull > program.Subsidy.CapMin {
		subsidyFull = program.Subsidy.CapMin
	}
	// 消费时点已部分退票：补贴只覆盖未退部分，已退部分从未入账，也不产生冲正凭证。
	subsidy := subsidyFull
	if ticket.face > 0 && refundedBefore > 0 {
		subsidy = roundRat(subsidyFull*(ticket.face-refundedBefore), ticket.face)
	}
	f.accepted = true
	f.subsidy = subsidy
	f.status = StatusAccepted
	if f.stage == StagePostMatch {
		f.status = StatusRetained
		f.reasons = append(f.reasons, "post_match_retained")
	}
	if ticket.reschedule != nil && at.Before(ticket.reschedule.At) {
		f.reschedKeep = true
		f.reasons = append(f.reasons, "reschedule_retained")
	}
	// 赛事在消费之后官方改期：已发生消费按规则保留，不因改期冲正。
	if ms := matches[f.matchID]; ms != nil {
		for _, rs := range ms.reschedules {
			if rs.At.After(at) {
				f.reschedKeep = true
				f.reasons = append(f.reasons, "match_reschedule_retained")
				break
			}
		}
	}
	return f
}

// buildImpacts 预计算核销在其之后所有冲正事件产生的累计冲正额。
// 商户退款与退票两路独立，按剩余比例连乘：
// 累计应冲正 = S - S*(1-rM/A)*(1-rT/F)，赛事取消则冲至全额。
func (s *Store) buildImpacts(f *redemptionFact, merchantRefunds []RedemptionRefunded,
	tickets map[string]*ticketState, matches map[string]*matchState) {

	r := f.rec
	ticket := tickets[f.ticketNo]
	S, A, F := f.subsidy, r.AmountMin, int64(0)
	if ticket != nil {
		F = ticket.face
	}

	type trig struct {
		eventID string
		at      time.Time
		rM, rT  int64
		cancel  bool
	}
	var trigs []trig
	for _, mr := range merchantRefunds {
		if !mr.RefundedAt.After(f.occurred) {
			continue
		}
		trigs = append(trigs, trig{
			eventID: findEventID(s.events, EvRedemptionRefunded, mr.RedemptionID, mr.RefundedAt, mr.RefundMin),
			at:      mr.RefundedAt, rM: mr.RefundMin,
		})
	}
	if ticket != nil {
		for _, tr := range ticket.refunds {
			if tr.RefundedAt.After(f.occurred) {
				trigs = append(trigs, trig{
					eventID: findEventID(s.events, EvTicketRefunded, tr.TicketNo, tr.RefundedAt, tr.RefundMin),
					at:      tr.RefundedAt, rT: tr.RefundMin,
				})
			}
		}
	}
	if ms := matches[f.matchID]; ms != nil && ms.cancelledAt != nil && ms.cancelledAt.After(f.occurred) {
		trigs = append(trigs, trig{eventID: findCancelEventID(s.events, f.matchID), at: *ms.cancelledAt, cancel: true})
	}
	sort.Slice(trigs, func(i, j int) bool {
		if trigs[i].at.Equal(trigs[j].at) {
			return trigs[i].eventID < trigs[j].eventID
		}
		return trigs[i].at.Before(trigs[j].at)
	})

	cumM, cumT, prev, cumGross := int64(0), int64(0), int64(0), int64(0)
	for _, t := range trigs {
		if t.cancel {
			if S > prev {
				delta := S - prev
				// 取消冲至全额：本金冲减到整笔消费，消除分步四舍五入的残差。
				grossLeft := A - cumGross
				f.impacts = append(f.impacts, rImpact{
					triggerEventID: t.eventID, at: t.at, amount: delta,
					gross: -grossLeft, reason: "match_cancelled",
				})
				cumGross = A
				prev = S
			}
			continue
		}
		cumM = min64(cumM+t.rM, A)
		cumT = min64(cumT+t.rT, F)
		var target int64
		if F > 0 {
			num := A*cumT + F*cumM - cumM*cumT
			target = roundRat(S*num, A*F)
		} else {
			target = roundRat(S*cumM, A)
		}
		target = min64(target, S)
		if target > prev {
			reason := "merchant_refund"
			if t.rT > 0 {
				reason = "ticket_refund"
			}
			delta := target - prev
			grossDelta := prorataGross(A, S, delta)
			cumGross += grossDelta
			f.impacts = append(f.impacts, rImpact{
				triggerEventID: t.eventID, at: t.at, amount: delta,
				gross: -grossDelta, reason: reason,
			})
			prev = target
		}
	}
}

// prorataGross 把补贴冲正额按比例换算为消费本金冲减，保证快照可精确复算。
func prorataGross(A, S, delta int64) int64 {
	if S <= 0 {
		return 0
	}
	return roundRat(A*delta, S)
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func roundRat(num, den int64) int64 {
	if den == 0 {
		return 0
	}
	q := num / den
	rem := num % den
	if rem < 0 {
		rem = -rem
	}
	if rem*2 >= den {
		q++
	}
	return q
}

func findEventID(evs []*Envelope, typ, key string, at time.Time, amt int64) string {
	for _, e := range evs {
		if e.Type != typ {
			continue
		}
		switch typ {
		case EvRedemptionRefunded:
			var p RedemptionRefunded
			_ = json.Unmarshal(e.Payload, &p)
			if p.RedemptionID == key && p.RefundedAt.Equal(at) && p.RefundMin == amt {
				return e.EventID
			}
		case EvTicketRefunded:
			var p TicketRefunded
			_ = json.Unmarshal(e.Payload, &p)
			if p.TicketNo == key && p.RefundedAt.Equal(at) && p.RefundMin == amt {
				return e.EventID
			}
		}
	}
	return ""
}

func findCancelEventID(evs []*Envelope, matchID string) string {
	for _, e := range evs {
		if e.Type != EvMatchCancelled {
			continue
		}
		var p MatchCancelled
		_ = json.Unmarshal(e.Payload, &p)
		if p.MatchID == matchID {
			return e.EventID
		}
	}
	return ""
}

// ---------- 第二遍：按日志顺序入账（封账冻结） ----------

func (s *Store) replay() error {
	s.vouchers = nil
	s.blocked = nil
	s.views = map[string]*RedemptionView{}
	s.closes = map[string]*PeriodClosed{}

	closed := map[string]bool{}
	seenRedemptions := map[string]bool{}
	emittedImpacts := map[string]bool{} // redemptionID|triggerEventID 去重
	var prevHash string
	emitDue := func(f *redemptionFact, maxSeq int64, detectedAt time.Time) {
		if !s.subsidyLive(f.rec.RedemptionID) {
			return // 补贴未实际入账（如开放账期迟到被拒），不产生冲正
		}
		for _, im := range f.impacts {
			key := f.rec.RedemptionID + "|" + im.triggerEventID
			if emittedImpacts[key] || seqOf(s.events, im.triggerEventID) > maxSeq {
				continue
			}
			emittedImpacts[key] = true
			s.emitImpact(f, im, closed, detectedAt)
		}
	}
	for _, e := range s.events {
		switch e.Type {
		case EvRedemptionRecorded:
			var p RedemptionRecorded
			if err := decodeStrict(e.Payload, &p); err != nil {
				return err
			}
			// 同一 redemption_id 的重复事件只入账一次（幂等）。
			if !seenRedemptions[p.RedemptionID] {
				seenRedemptions[p.RedemptionID] = true
				if f := s.facts[p.RedemptionID]; f != nil && f.accepted {
					period := s.periodOf(f.occurred)
					switch {
					case f.late && closed[period]:
						// 封账后补传：即便超宽限也挂起，只能由财务调整分录入账。
						s.emitSubsidy(e, f, closed)
					case f.late:
						// 开放账期内超过迟到宽限：拒绝，不补贴。
					default:
						s.emitSubsidy(e, f, closed)
					}
					// 回填式冲正（退款事件先于迟到核销到达）随核销一并补发。
					emitDue(f, e.Seq, e.ReceivedAt)
				}
			}
			s.buildView(p.RedemptionID)
		case EvRedemptionRefunded:
			var p RedemptionRefunded
			if err := decodeStrict(e.Payload, &p); err != nil {
				return err
			}
			if f := s.facts[p.RedemptionID]; f != nil {
				emitDue(f, e.Seq, e.ReceivedAt)
				s.buildView(p.RedemptionID)
			}
		case EvTicketRefunded:
			var p TicketRefunded
			if err := decodeStrict(e.Payload, &p); err != nil {
				return err
			}
			for _, f := range s.factsByTicket(p.TicketNo) {
				emitDue(f, e.Seq, e.ReceivedAt)
				s.buildView(f.rec.RedemptionID)
			}
		case EvMatchCancelled:
			var p MatchCancelled
			if err := decodeStrict(e.Payload, &p); err != nil {
				return err
			}
			for _, f := range s.factsByMatch(p.MatchID) {
				emitDue(f, e.Seq, e.ReceivedAt)
				s.buildView(f.rec.RedemptionID)
			}
		case EvPeriodClosed:
			var p PeriodClosed
			if err := decodeStrict(e.Payload, &p); err != nil {
				return err
			}
			totals := s.computeTotals(p.Period, e.Seq)
			count := 0
			for _, v := range s.vouchers {
				if v.Period == p.Period && v.Seq <= e.Seq {
					count++
				}
			}
			hash := snapshotHash(prevHash, p.Period, e.Seq, totals)
			if p.ClosedSeq != e.Seq || p.EntryCount != count || !totalsEqual(p.Totals, totals) || p.Hash != hash {
				return fmt.Errorf("账期 %s 快照复算不一致", p.Period)
			}
			cp := p
			s.closes[p.Period] = &cp
			closed[p.Period] = true
			prevHash = hash
		case EvAdjustmentPosted:
			var p AdjustmentPosted
			if err := decodeStrict(e.Payload, &p); err != nil {
				return err
			}
			if closed[p.TargetPeriod] {
				return fmt.Errorf("%w：调整 %s 目标账期 %s", ErrClosed, p.AdjustmentID, p.TargetPeriod)
			}
			lines := makePostingLines(p.Lines)
			d, c, err := sumAdjustmentLines(p.Lines)
			if err != nil {
				return err
			}
			if err := validateAdjustment(p, d, c); err != nil {
				return err
			}
			if err := s.validateAgainstBlocked(p); err != nil {
				return err
			}
			s.vouchers = append(s.vouchers, &Voucher{
				Seq: e.Seq, Period: p.TargetPeriod, Kind: KindAdjust, EventID: e.EventID,
				Reason: p.Reason, RuleVersion: s.catalog.RuleAt(p.PostedAt).Version, PostedAt: p.PostedAt,
				Lines: lines,
			})
			for _, b := range s.blocked {
				if b.AdjustmentID == "" && p.OriginEventID != "" && b.OriginEventID == p.OriginEventID {
					b.AdjustmentID = p.AdjustmentID
				}
			}
			for _, f := range s.facts {
				if s.blockedTouches(f.rec.RedemptionID, p.OriginEventID) {
					s.buildView(f.rec.RedemptionID)
				}
			}
		}
	}
	return nil
}

func validateAdjustment(p AdjustmentPosted, d, c int64) error {
	if p.AdjustmentID == "" || p.Reason == "" || p.TargetPeriod == "" || len(p.Lines) < 2 {
		return fmt.Errorf("%w：调整分录缺少必填字段", ErrInvalid)
	}
	if d != c {
		return fmt.Errorf("%w：调整分录借贷不平衡（借 %d / 贷 %d）", ErrInvalid, d, c)
	}
	return nil
}

func sumAdjustmentLines(ls []AdjustmentLine) (d, c int64, err error) {
	for _, l := range ls {
		if l.AmountMin <= 0 {
			return 0, 0, fmt.Errorf("%w：调整分录金额必须为正", ErrInvalid)
		}
		switch l.DC {
		case "D":
			d += l.AmountMin
		case "C":
			c += l.AmountMin
		default:
			return 0, 0, fmt.Errorf("%w：借贷标记必须为 D/C", ErrInvalid)
		}
		if l.Account != AccountAROrganizer && l.Account != AccountAPMerchant {
			return 0, 0, fmt.Errorf("%w：未知账户 %s", ErrInvalid, l.Account)
		}
	}
	return d, c, nil
}

// signedAdjustmentTotals 返回调整分录入账后对 AP/AR 两侧的有符号增量。
func signedAdjustmentTotals(lines []Posting) (ap, ar int64) {
	for _, l := range lines {
		v := l.AmountMin
		if l.DC == "D" {
			v = -v
		}
		switch l.Account {
		case AccountAPMerchant:
			ap += v // C 贷应付为正
		case AccountAROrganizer:
			ar += -v // D 借应收为正
		}
	}
	return ap, ar
}

// validateAgainstBlocked 校验调整分录金额恰好承接其引用的挂起影响，
// 防止封账后通过调整多补或少冲。
func (s *Store) validateAgainstBlocked(p AdjustmentPosted) error {
	if p.OriginEventID == "" {
		return nil
	}
	var want int64
	matched := false
	for _, b := range s.blocked {
		if b.OriginEventID == p.OriginEventID && b.AdjustmentID == "" {
			want += b.AmountMin
			matched = true
		}
	}
	if !matched {
		return fmt.Errorf("%w：原始事件 %s 没有待调整的封账影响", ErrInvalid, p.OriginEventID)
	}
	ap, ar := signedAdjustmentTotals(makePostingLines(p.Lines))
	if ap != want || ar != want {
		return fmt.Errorf("%w：调整金额与挂起影响不一致（挂起 %d，应收侧 %d，应付侧 %d）", ErrInvalid, want, ar, ap)
	}
	return nil
}

func makePostingLines(ls []AdjustmentLine) []Posting {
	out := make([]Posting, 0, len(ls))
	for _, l := range ls {
		out = append(out, Posting{Account: l.Account, CounterpartyID: l.CounterpartyID,
			MatchID: l.MatchID, ProgramCode: l.ProgramCode, DC: l.DC, AmountMin: l.AmountMin})
	}
	return out
}

func (s *Store) blockedTouches(redemptionID, originEventID string) bool {
	if originEventID == "" {
		return false
	}
	for _, b := range s.blocked {
		if b.RedemptionID == redemptionID && b.OriginEventID == originEventID {
			return true
		}
	}
	return false
}

func (s *Store) emitSubsidy(e *Envelope, f *redemptionFact, closed map[string]bool) {
	r := f.rec
	match := s.catalog.Matches[f.matchID]
	period := s.periodOf(f.occurred)
	lines := []Posting{
		{Account: AccountAROrganizer, CounterpartyID: match.OrganizerID, DC: "D", AmountMin: f.subsidy,
			MatchID: f.matchID, ProgramCode: r.ProgramCode},
		{Account: AccountAPMerchant, CounterpartyID: r.MerchantID, DC: "C", AmountMin: f.subsidy,
			MatchID: f.matchID, ProgramCode: r.ProgramCode},
	}
	v := &Voucher{
		Seq: e.Seq, Period: period, Kind: KindSubsidy, EventID: e.EventID,
		RedemptionID: r.RedemptionID, MatchID: f.matchID, Stage: f.stage,
		ProgramCode: r.ProgramCode, MerchantID: r.MerchantID, OrganizerID: match.OrganizerID,
		GrossMin: r.AmountMin, AmountMin: f.subsidy, RuleVersion: f.ruleVer,
		Reason: strings.Join(f.reasons, ","), PostedAt: f.occurred, Lines: lines,
	}
	if closed[period] {
		s.blocked = append(s.blocked, &BlockedEffect{
			OriginEventID: e.EventID, RedemptionID: r.RedemptionID, NaturalPeriod: period,
			MatchID: f.matchID, ProgramCode: r.ProgramCode, MerchantID: r.MerchantID,
			OrganizerID: match.OrganizerID, AmountMin: f.subsidy, GrossMin: r.AmountMin,
			Kind: KindSubsidy, Reason: "封账后补传，需调整分录入账", DetectedAt: e.ReceivedAt,
		})
		return
	}
	s.vouchers = append(s.vouchers, v)
}

func (s *Store) subsidyLive(redemptionID string) bool {
	for _, v := range s.vouchers {
		if v.RedemptionID == redemptionID && v.Kind == KindSubsidy {
			return true
		}
	}
	for _, b := range s.blocked {
		if b.RedemptionID == redemptionID && b.Kind == KindSubsidy {
			return true
		}
	}
	return false
}

func (s *Store) emitImpact(f *redemptionFact, im rImpact, closed map[string]bool, detectedAt time.Time) {
	match := s.catalog.Matches[f.matchID]
	seq := seqOf(s.events, im.triggerEventID)
	// 冲正的自然账期是原消费所在账期：9 月账未封则当期红冲，
	// 已封账则挂起，由财务在当前开放账期做调整分录。
	period := s.periodOf(f.occurred)
	lines := []Posting{
		{Account: AccountAPMerchant, CounterpartyID: f.rec.MerchantID, DC: "D", AmountMin: im.amount,
			MatchID: f.matchID, ProgramCode: f.rec.ProgramCode},
		{Account: AccountAROrganizer, CounterpartyID: match.OrganizerID, DC: "C", AmountMin: im.amount,
			MatchID: f.matchID, ProgramCode: f.rec.ProgramCode},
	}
	v := &Voucher{
		Seq: seq, Period: period, Kind: KindReversal, EventID: im.triggerEventID,
		RedemptionID: f.rec.RedemptionID, MatchID: f.matchID, Stage: f.stage,
		ProgramCode: f.rec.ProgramCode, MerchantID: f.rec.MerchantID, OrganizerID: match.OrganizerID,
		GrossMin: im.gross, AmountMin: -im.amount, Reason: im.reason, RuleVersion: f.ruleVer,
		PostedAt: im.at, Lines: lines,
	}
	if closed[period] {
		s.blocked = append(s.blocked, &BlockedEffect{
			OriginEventID: im.triggerEventID, RedemptionID: f.rec.RedemptionID, NaturalPeriod: period,
			MatchID: f.matchID, ProgramCode: f.rec.ProgramCode, MerchantID: f.rec.MerchantID,
			OrganizerID: match.OrganizerID, AmountMin: -im.amount, GrossMin: im.gross,
			Kind: KindReversal, Reason: "封账后冲正，需调整分录修正", DetectedAt: detectedAt,
		})
		return
	}
	s.vouchers = append(s.vouchers, v)
}

func seqOf(evs []*Envelope, eventID string) int64 {
	for _, e := range evs {
		if e.EventID == eventID {
			return e.Seq
		}
	}
	return 0
}

func (s *Store) factsByTicket(ticketNo string) []*redemptionFact {
	var out []*redemptionFact
	for _, f := range s.facts {
		if f.ticketNo == ticketNo {
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].rec.RedemptionID < out[j].rec.RedemptionID })
	return out
}

func (s *Store) factsByMatch(matchID string) []*redemptionFact {
	var out []*redemptionFact
	for _, f := range s.facts {
		if f.matchID == matchID {
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].rec.RedemptionID < out[j].rec.RedemptionID })
	return out
}

func (s *Store) periodOf(t time.Time) string {
	return t.In(s.loc).Format("2006-01")
}

// ---------- 视图 ----------

func (s *Store) buildView(redemptionID string) {
	f := s.facts[redemptionID]
	if f == nil {
		return
	}
	r := f.rec
	view := &RedemptionView{
		RedemptionID: r.RedemptionID, MerchantID: r.MerchantID, ProgramCode: r.ProgramCode,
		MatchID: f.matchID, Stage: f.stage, ConsumedAt: f.occurred, UploadedAt: f.uploaded,
		AmountMin: r.AmountMin, Currency: r.Currency, SubsidyMin: f.subsidy,
		Status: f.status, Reasons: append([]string(nil), f.reasons...), RuleVersion: f.ruleVer,
	}
	if m, ok := s.catalog.Matches[f.matchID]; ok {
		view.EventName = m.Name
	}
	if f.accepted {
		for _, v := range s.vouchers {
			switch {
			case v.RedemptionID != redemptionID:
			case v.Kind == KindSubsidy:
				view.AutoNetMin += v.AmountMin
			case v.Kind == KindReversal:
				view.AutoNetMin += v.AmountMin
				view.ReversedMin += -v.AmountMin
			}
		}
		for _, b := range s.blocked {
			if b.RedemptionID == redemptionID {
				view.BlockedMin += b.AmountMin
				if b.AdjustmentID != "" {
					view.AdjustedMin += b.AmountMin
				}
			}
		}
		// 开放账期内超迟到宽限：补贴未入账也未挂起，结论是拒绝。
		if f.late && view.AutoNetMin == 0 && view.BlockedMin == 0 {
			view.Status = StatusRejected
			view.SubsidyMin = 0
			view.Reasons = []string{"late_upload_beyond_grace"}
			view.EventIDs = s.eventChain(redemptionID)
			s.views[redemptionID] = view
			return
		}
		switch {
		case view.AdjustedMin != 0 && view.BlockedMin == view.AdjustedMin:
			view.Status = StatusAdjusted
		case view.BlockedMin != 0:
			view.Status = StatusBlocked
		case view.AutoNetMin == 0:
			view.Status = StatusReversedFull
		case view.AutoNetMin < f.subsidy:
			view.Status = StatusReversedPartial
		case f.reschedKeep:
			view.Status = StatusRetained
		}
	}
	view.EventIDs = s.eventChain(redemptionID)
	s.views[redemptionID] = view
}

func (s *Store) eventChain(redemptionID string) []string {
	ids := map[string]bool{}
	add := func(prefix string, b []byte) {
		var probe struct {
			RedemptionID string `json:"redemption_id"`
			TicketNo     string `json:"ticket_no"`
			GrantID      string `json:"grant_id"`
			MatchID      string `json:"match_id"`
		}
		_ = json.Unmarshal(b, &probe)
		_ = prefix
	}
	_ = add
	f := s.facts[redemptionID]
	for _, e := range s.events {
		matched := false
		switch e.Type {
		case EvRedemptionRecorded, EvRedemptionRefunded:
			var p struct {
				RedemptionID string `json:"redemption_id"`
			}
			_ = json.Unmarshal(e.Payload, &p)
			matched = p.RedemptionID == redemptionID
		case EvTicketRefunded:
			var p TicketRefunded
			_ = json.Unmarshal(e.Payload, &p)
			matched = f != nil && p.TicketNo == f.ticketNo
		case EvBenefitGranted, EvAccessVerified:
			var p struct {
				TicketNo string `json:"ticket_no"`
				GrantID  string `json:"grant_id"`
			}
			_ = json.Unmarshal(e.Payload, &p)
			matched = f != nil && (p.TicketNo == f.ticketNo || (f.rec.GrantID != "" && p.GrantID == f.rec.GrantID))
		case EvTicketRescheduled:
			var p TicketRescheduled
			_ = json.Unmarshal(e.Payload, &p)
			matched = f != nil && p.TicketNo == f.ticketNo
		case EvMatchCancelled:
			var p MatchCancelled
			_ = json.Unmarshal(e.Payload, &p)
			matched = f != nil && f.accepted && p.MatchID == f.matchID
		case EvAdjustmentPosted:
			var p AdjustmentPosted
			_ = json.Unmarshal(e.Payload, &p)
			for _, b := range s.blocked {
				if b.RedemptionID == redemptionID && b.OriginEventID == p.OriginEventID {
					matched = true
					break
				}
			}
		}
		if matched {
			ids[e.EventID] = true
		}
	}
	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// ---------- 汇总与快照 ----------

type aggKey struct {
	dim, key string
}

func (s *Store) computeTotals(period string, maxSeq int64) []Total {
	agg := map[aggKey]*Total{}
	get := func(k aggKey) *Total {
		if k.key == "" {
			return nil
		}
		t := agg[k]
		if t == nil {
			t = &Total{Dimension: k.dim, Key: k.key}
			agg[k] = t
		}
		return t
	}
	add := func(dim, key string, recv, payable, gross int64) {
		if t := get(aggKey{dim, key}); t != nil {
			t.ReceivableMin += recv
			t.PayableMin += payable
			t.GrossMin += gross
		}
	}

	for _, v := range s.vouchers {
		if v.Period != period || v.Seq > maxSeq {
			continue
		}
		if v.Kind == KindAdjust {
			// 调整分录按行归属：AP 行进商户应付，AR 行进主办方应收；
			// 携带的 match/program 同步归集，账期维度不留 stage。
			for _, l := range v.Lines {
				signed := l.AmountMin
				if (l.Account == AccountAROrganizer && l.DC == "C") ||
					(l.Account == AccountAPMerchant && l.DC == "D") {
					signed = -signed
				}
				switch l.Account {
				case AccountAROrganizer:
					add("organizer", l.CounterpartyID, signed, 0, 0)
					add("match", l.MatchID, signed, 0, 0)
					add("program", l.ProgramCode, signed, 0, 0)
				case AccountAPMerchant:
					add("merchant", l.CounterpartyID, 0, signed, 0)
					add("match", l.MatchID, 0, signed, 0)
					add("program", l.ProgramCode, 0, signed, 0)
				}
			}
			continue
		}
		// 补贴/冲正凭证：有符号金额在应收与应付两侧一致。
		amt := v.AmountMin
		add("merchant", v.MerchantID, 0, amt, v.GrossMin)
		add("organizer", v.OrganizerID, amt, 0, 0)
		add("match", v.MatchID, amt, amt, v.GrossMin)
		add("program", v.ProgramCode, amt, amt, v.GrossMin)
		add("stage", v.Stage, amt, amt, v.GrossMin)
	}
	out := make([]Total, 0, len(agg))
	for _, t := range agg {
		out = append(out, *t)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Dimension != out[j].Dimension {
			return out[i].Dimension < out[j].Dimension
		}
		return out[i].Key < out[j].Key
	})
	return out
}

func totalsEqual(a, b []Total) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func snapshotHash(prev, period string, seq int64, totals []Total) string {
	canonical, _ := json.Marshal(struct {
		PrevHash string  `json:"prev_hash"`
		Period   string  `json:"period"`
		Seq      int64   `json:"seq"`
		Totals   []Total `json:"totals"`
	}{prev, period, seq, totals})
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

// ---------- 对外命令 ----------

func (s *Store) Ingest(env *Envelope) (*IngestResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if env.EventID == "" || env.Type == "" || env.OccurredAt.IsZero() {
		return nil, fmt.Errorf("%w：缺少 event_id/type/occurred_at", ErrInvalid)
	}
	if containsIdentityField(env.Payload) {
		return nil, ErrIdentityField
	}
	if _, dup := s.eventByID(env.EventID); dup {
		return &IngestResult{EventID: env.EventID, Outcome: "duplicate"}, nil
	}
	if err := validatePayload(env); err != nil {
		return nil, err
	}
	// 网络重试更换 event_id 但业务流水号相同：按重发幂等处理，不得二次补贴。
	if env.Type == EvRedemptionRecorded {
		var p RedemptionRecorded
		_ = json.Unmarshal(env.Payload, &p)
		if s.hasRedemption(p.RedemptionID) {
			return &IngestResult{EventID: env.EventID, Outcome: "duplicate"}, nil
		}
	}

	snap := s.snapshotEvents()
	s.seq++
	env.Seq = s.seq
	if env.ReceivedAt.IsZero() {
		env.ReceivedAt = env.OccurredAt
	}
	s.events = append(s.events, env)

	s.project()
	if err := s.replay(); err != nil {
		s.restoreEvents(snap)
		s.project()
		_ = s.replay()
		return nil, err
	}

	res := &IngestResult{EventID: env.EventID, Seq: s.seq, Outcome: "accepted"}
	if env.Type == EvRedemptionRecorded {
		var p RedemptionRecorded
		_ = json.Unmarshal(env.Payload, &p)
		if f := s.facts[p.RedemptionID]; f != nil {
			if !f.accepted {
				res.Outcome = "rejected"
				res.Reasons = f.reasons
			} else if s.isBlocked(env.EventID) {
				res.Blocked = true
				res.Reasons = []string{"natural_period_closed_pending_adjustment"}
			} else if f.late && !s.subsidyLive(p.RedemptionID) {
				res.Outcome = "rejected"
				res.Reasons = []string{"late_upload_beyond_grace"}
			}
		}
	}
	return res, nil
}

func (s *Store) isBlocked(originEventID string) bool {
	for _, b := range s.blocked {
		if b.OriginEventID == originEventID && b.AdjustmentID == "" {
			return true
		}
	}
	return false
}

type eventsSnap struct {
	seq    int64
	events []*Envelope
}

func (s *Store) snapshotEvents() eventsSnap {
	cp := make([]*Envelope, len(s.events))
	copy(cp, s.events)
	return eventsSnap{s.seq, cp}
}

func (s *Store) restoreEvents(snap eventsSnap) {
	s.seq = snap.seq
	s.events = snap.events
}

func (s *Store) eventByID(id string) (*Envelope, bool) {
	for _, e := range s.events {
		if e.EventID == id {
			return e, true
		}
	}
	return nil, false
}

func (s *Store) hasRedemption(redemptionID string) bool {
	for _, e := range s.events {
		if e.Type != EvRedemptionRecorded {
			continue
		}
		var p RedemptionRecorded
		_ = json.Unmarshal(e.Payload, &p)
		if p.RedemptionID == redemptionID {
			return true
		}
	}
	return false
}

func validatePayload(e *Envelope) error {
	var v any
	switch e.Type {
	case EvTicketIssued:
		v = &TicketIssued{}
	case EvTicketRefunded:
		v = &TicketRefunded{}
	case EvTicketRescheduled:
		v = &TicketRescheduled{}
	case EvMatchRescheduled:
		v = &MatchRescheduled{}
	case EvMatchCancelled:
		v = &MatchCancelled{}
	case EvAccessVerified:
		v = &AccessVerified{}
	case EvBenefitGranted:
		v = &BenefitGranted{}
	case EvBenefitRevoked:
		v = &BenefitRevoked{}
	case EvRedemptionRecorded:
		v = &RedemptionRecorded{}
	case EvRedemptionRefunded:
		v = &RedemptionRefunded{}
	case EvPeriodClosed, EvAdjustmentPosted:
		return fmt.Errorf("%w：封账与调整必须走专用命令", ErrInvalid)
	default:
		return fmt.Errorf("%w：%s", ErrUnknownType, e.Type)
	}
	if err := decodeStrict(e.Payload, v); err != nil {
		return fmt.Errorf("%w：%v", ErrInvalid, err)
	}
	return nil
}

// decodeStrict 拒绝载荷中的未知字段，避免夹带系统不接受的信息。
func decodeStrict(raw json.RawMessage, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}

// ClosePeriod 由财务封账：服务端重算金额，生成带哈希链的账期快照事件。
func (s *Store) ClosePeriod(period string, closedAt time.Time) (*PeriodClosed, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.closes[period]; ok {
		return nil, fmt.Errorf("%w：%s", ErrClosed, period)
	}

	s.seq++
	seq := s.seq
	var prevHash string
	for _, e := range s.events {
		if e.Type == EvPeriodClosed {
			var p PeriodClosed
			_ = json.Unmarshal(e.Payload, &p)
			prevHash = p.Hash
		}
	}
	count := 0
	for _, v := range s.vouchers {
		if v.Period == period && v.Seq <= seq {
			count++
		}
	}
	totals := s.computeTotals(period, seq)
	rule := s.catalog.RuleAt(closedAt)
	hash := snapshotHash(prevHash, period, seq, totals)
	pc := &PeriodClosed{
		Period: period, ClosedAt: closedAt, RuleVersion: rule.Version, ClosedSeq: seq,
		EntryCount: count, PrevHash: prevHash, Hash: hash, Totals: totals,
	}
	raw, _ := json.Marshal(pc)
	s.events = append(s.events, &Envelope{
		EventID: fmt.Sprintf("close-%s-%d", period, seq), Type: EvPeriodClosed,
		OccurredAt: closedAt, ReceivedAt: closedAt, Seq: seq, Payload: raw,
	})
	s.project()
	if err := s.replay(); err != nil {
		return nil, err
	}
	return pc, nil
}

// PostAdjustment 由财务在开放账期中登记调整分录，可引用封账后挂起的原始事件。
func (s *Store) PostAdjustment(p AdjustmentPosted) (*IngestResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.AdjustmentID == "" {
		return nil, fmt.Errorf("%w：缺少 adjustment_id", ErrInvalid)
	}
	if _, dup := s.eventByID(p.AdjustmentID); dup {
		return &IngestResult{EventID: p.AdjustmentID, Outcome: "duplicate"}, nil
	}
	if _, closed := s.closes[p.TargetPeriod]; closed {
		return nil, fmt.Errorf("%w：目标账期 %s 已封账", ErrClosed, p.TargetPeriod)
	}
	if p.PostedAt.IsZero() {
		p.PostedAt = time.Now()
	}
	d, c, errSum := sumAdjustmentLines(p.Lines)
	if errSum != nil {
		return nil, errSum
	}
	if err := validateAdjustment(p, d, c); err != nil {
		return nil, err
	}
	if err := s.validateAgainstBlocked(p); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	snap := s.snapshotEvents()
	s.seq++
	s.events = append(s.events, &Envelope{
		EventID: p.AdjustmentID, Type: EvAdjustmentPosted, OccurredAt: p.PostedAt,
		ReceivedAt: p.PostedAt, Seq: s.seq, Payload: raw,
	})
	s.project()
	if err := s.replay(); err != nil {
		s.restoreEvents(snap)
		s.project()
		_ = s.replay()
		return nil, err
	}
	return &IngestResult{EventID: p.AdjustmentID, Seq: s.seq, Outcome: "accepted"}, nil
}

// ---------- 查询 ----------

// Scope 表示调用方的数据可见范围。
type Scope struct {
	Role     string
	Identity string // organizer / merchant 的具体主体 ID
}

func (s *Store) Redemption(id string, scope Scope) (*RedemptionView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.views[id]
	if v == nil {
		return nil, fmt.Errorf("核销不存在：%s", id)
	}
	if !s.canRead(v, scope) {
		return nil, ErrForbidden
	}
	masked := *v
	s.mask(&masked, scope)
	return &masked, nil
}

func (s *Store) organizerOfMatch(matchID string) string {
	if m, ok := s.catalog.Matches[matchID]; ok {
		return m.OrganizerID
	}
	return ""
}

func (s *Store) canRead(v *RedemptionView, scope Scope) bool {
	switch scope.Role {
	case RoleMerchant:
		return scope.Identity == v.MerchantID
	case RoleOrganizer:
		return scope.Identity == s.organizerOfView(v)
	default:
		return true
	}
}

func (s *Store) organizerOfView(v *RedemptionView) string {
	return s.organizerOfMatch(v.MatchID)
}

// mask 按角色抹除不该跨范围看到的对手方标识。
func (s *Store) mask(v *RedemptionView, scope Scope) {
	if scope.Role == RoleOrganizer {
		v.MerchantID = ""
	}
}

func (s *Store) ListRedemptions(scope Scope) []RedemptionView {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []RedemptionView
	for _, v := range s.views {
		if !s.canRead(v, scope) {
			continue
		}
		cp := *v
		s.mask(&cp, scope)
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RedemptionID < out[j].RedemptionID })
	return out
}

func (s *Store) Totals(period string, scope Scope) []Total {
	s.mu.Lock()
	defer s.mu.Unlock()
	maxSeq := int64(1 << 62)
	if c, ok := s.closes[period]; ok {
		maxSeq = c.ClosedSeq
	}
	var out []Total
	for _, t := range s.computeTotals(period, maxSeq) {
		switch scope.Role {
		case RoleMerchant:
			if !(t.Dimension == "merchant" && t.Key == scope.Identity) {
				continue
			}
		case RoleOrganizer:
			switch t.Dimension {
			case "organizer":
				if t.Key != scope.Identity {
					continue
				}
			case "merchant":
				continue // 商户维度不对主办方暴露
			}
		}
		out = append(out, t)
	}
	return out
}

func (s *Store) Blocked(scope Scope) []BlockedEffect {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []BlockedEffect
	for _, b := range s.blocked {
		switch scope.Role {
		case RoleMerchant:
			if b.MerchantID != scope.Identity {
				continue
			}
		case RoleOrganizer:
			if b.OrganizerID != scope.Identity {
				continue
			}
		}
		cp := *b
		if scope.Role == RoleOrganizer {
			cp.MerchantID = ""
		}
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].OriginEventID == out[j].OriginEventID {
			return out[i].RedemptionID < out[j].RedemptionID
		}
		return out[i].OriginEventID < out[j].OriginEventID
	})
	return out
}

func (s *Store) Snapshots() []PeriodClosed {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []PeriodClosed
	for _, c := range s.closes {
		out = append(out, *c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Period < out[j].Period })
	return out
}

// VerifySnapshot 从当前事件日志重新推导账期金额并与快照比对。
func (s *Store) VerifySnapshot(period string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pc, ok := s.closes[period]
	if !ok {
		return nil, fmt.Errorf("账期未封账：%s", period)
	}
	totals := s.computeTotals(period, pc.ClosedSeq)
	count := 0
	for _, v := range s.vouchers {
		if v.Period == period && v.Seq <= pc.ClosedSeq {
			count++
		}
	}
	hash := snapshotHash(pc.PrevHash, period, pc.ClosedSeq, totals)
	return map[string]any{
		"period":           period,
		"snapshot_totals":  pc.Totals,
		"recomputed":       totals,
		"snapshot_count":   pc.EntryCount,
		"recomputed_count": count,
		"snapshot_hash":    pc.Hash,
		"recomputed_hash":  hash,
		"matches":          totalsEqual(pc.Totals, totals) && count == pc.EntryCount && hash == pc.Hash,
	}, nil
}

// Audit 返回一笔应收应付的完整解释链：原始事件 → 规则版本 → 凭证/挂起 → 调整。
func (s *Store) Audit(redemptionID string, scope Scope) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	view := s.views[redemptionID]
	if view == nil {
		return nil, fmt.Errorf("核销不存在：%s", redemptionID)
	}
	if !s.canRead(view, scope) {
		return nil, ErrForbidden
	}
	f := s.facts[redemptionID]
	var vouchers []Voucher
	var blocked []BlockedEffect
	for _, v := range s.vouchers {
		if v.RedemptionID == redemptionID {
			cp := *v
			if scope.Role == RoleOrganizer {
				cp.MerchantID = ""
				for i := range cp.Lines {
					if cp.Lines[i].Account == AccountAPMerchant {
						cp.Lines[i].CounterpartyID = ""
					}
				}
			}
			vouchers = append(vouchers, cp)
		}
	}
	for _, b := range s.blocked {
		if b.RedemptionID == redemptionID {
			cp := *b
			if scope.Role == RoleOrganizer {
				cp.MerchantID = ""
			}
			blocked = append(blocked, cp)
		}
	}
	sort.Slice(vouchers, func(i, j int) bool { return vouchers[i].Seq < vouchers[j].Seq })
	events := s.rawEventsFor(redemptionID)
	if scope.Role == RoleOrganizer {
		for i := range events {
			events[i] = maskEvent(events[i])
		}
	}
	return map[string]any{
		"redemption":   view,
		"events":       events,
		"vouchers":     vouchers,
		"blocked":      blocked,
		"rule_version": f.ruleVer,
		"rule":         s.catalog.RuleAt(f.occurred),
	}, nil
}

// maskEvent 对主办方隐藏商户标识，载荷中的票号属于假名标识，可保留。
func maskEvent(e Envelope) Envelope {
	var probe map[string]any
	if err := json.Unmarshal(e.Payload, &probe); err != nil {
		return e
	}
	for _, k := range []string{"merchant_id"} {
		if _, ok := probe[k]; ok {
			probe[k] = ""
		}
	}
	raw, _ := json.Marshal(probe)
	cp := e
	cp.Payload = raw
	return cp
}

func (s *Store) rawEventsFor(redemptionID string) []Envelope {
	f := s.facts[redemptionID]
	want := map[string]bool{}
	for _, e := range s.events {
		matched := false
		switch e.Type {
		case EvRedemptionRecorded, EvRedemptionRefunded:
			var p struct {
				RedemptionID string `json:"redemption_id"`
			}
			_ = json.Unmarshal(e.Payload, &p)
			matched = p.RedemptionID == redemptionID
		case EvTicketRefunded, EvTicketRescheduled, EvTicketIssued:
			var p struct {
				TicketNo string `json:"ticket_no"`
			}
			_ = json.Unmarshal(e.Payload, &p)
			matched = f != nil && p.TicketNo == f.ticketNo
		case EvBenefitGranted, EvAccessVerified:
			var p struct {
				TicketNo string `json:"ticket_no"`
				GrantID  string `json:"grant_id"`
			}
			_ = json.Unmarshal(e.Payload, &p)
			matched = f != nil && (p.TicketNo == f.ticketNo || (f.rec.GrantID != "" && p.GrantID == f.rec.GrantID))
		case EvMatchCancelled:
			var p MatchCancelled
			_ = json.Unmarshal(e.Payload, &p)
			matched = f != nil && f.accepted && p.MatchID == f.matchID
		case EvAdjustmentPosted:
			var p AdjustmentPosted
			_ = json.Unmarshal(e.Payload, &p)
			for _, b := range s.blocked {
				if b.RedemptionID == redemptionID && b.OriginEventID == p.OriginEventID {
					matched = true
					break
				}
			}
		}
		if matched {
			want[e.EventID] = true
		}
	}
	var out []Envelope
	for _, e := range s.events {
		if want[e.EventID] {
			out = append(out, *e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}
