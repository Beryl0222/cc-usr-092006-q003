package settlement

import (
	"encoding/json"
	"strings"
	"time"
)

// 事件类型：原始事件只追加、不可修改。封账与调整同样以事件形式入日志，
// 使全量状态可由事件日志确定性重放。
const (
	EvTicketIssued       = "ticket.issued"
	EvTicketRefunded     = "ticket.refunded"
	EvTicketRescheduled  = "ticket.rescheduled"
	EvMatchRescheduled   = "match.rescheduled"
	EvMatchCancelled     = "match.cancelled"
	EvAccessVerified     = "access.verified"
	EvBenefitGranted     = "benefit.granted"
	EvBenefitRevoked     = "benefit.revoked"
	EvRedemptionRecorded = "redemption.recorded"
	EvRedemptionRefunded = "redemption.refunded"
	EvPeriodClosed       = "settlement.period_closed"
	EvAdjustmentPosted   = "adjustment.posted"
)

// Envelope 是统一事件入口的信封。ReceivedAt 由服务端在接收时加盖，
// 用于判断跨午夜迟到上传；OccurredAt 是业务发生时间，归因一律以它为准。
type Envelope struct {
	EventID    string          `json:"event_id"`
	Type       string          `json:"type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload"`

	ReceivedAt time.Time `json:"received_at,omitempty"`
	Seq        int64     `json:"-"`
}

// 金额一律使用整数最小单位（如分），杜绝浮点误差。

type TicketIssued struct {
	TicketNo string `json:"ticket_no"`
	MatchID  string `json:"match_id"`
	FaceMin  int64  `json:"face_min"`
	Currency string `json:"currency"`
}

// TicketRefunded 的 RefundMin 是本次退款增量，支持部分退款的多次事件。
type TicketRefunded struct {
	TicketNo   string    `json:"ticket_no"`
	RefundMin  int64     `json:"refund_min"`
	RefundedAt time.Time `json:"refunded_at"`
}

type TicketRescheduled struct {
	TicketNo    string    `json:"ticket_no"`
	FromMatchID string    `json:"from_match_id"`
	ToMatchID   string    `json:"to_match_id"`
	At          time.Time `json:"at"`
}

type MatchRescheduled struct {
	MatchID     string    `json:"match_id"`
	NewStartsAt time.Time `json:"new_starts_at"`
	NewEndsAt   time.Time `json:"new_ends_at"`
	At          time.Time `json:"at"`
}

type MatchCancelled struct {
	MatchID     string    `json:"match_id"`
	CancelledAt time.Time `json:"cancelled_at"`
}

// AccessVerified 只保存票号这一假名标识与入场时间，不保存姓名、证件号等身份信息。
type AccessVerified struct {
	TicketNo   string    `json:"ticket_no"`
	MatchID    string    `json:"match_id"`
	VerifiedAt time.Time `json:"verified_at"`
	Gate       string    `json:"gate,omitempty"`
}

type BenefitGranted struct {
	GrantID     string    `json:"grant_id"`
	TicketNo    string    `json:"ticket_no"`
	ProgramCode string    `json:"program_code"`
	IssuedAt    time.Time `json:"issued_at"`
	SingleUse   bool      `json:"single_use"`
}

type BenefitRevoked struct {
	GrantID string    `json:"grant_id"`
	At      time.Time `json:"at"`
	Reason  string    `json:"reason,omitempty"`
}

// RedemptionRecorded 是商户核销/流水事件。OccurredAt 即消费时间，
// 商户跨午夜上传不影响按消费时间的场次归属。
type RedemptionRecorded struct {
	RedemptionID string `json:"redemption_id"`
	MerchantID   string `json:"merchant_id"`
	ProgramCode  string `json:"program_code"`
	GrantID      string `json:"grant_id,omitempty"`
	TicketNo     string `json:"ticket_no,omitempty"`
	AmountMin    int64  `json:"amount_min"`
	Currency     string `json:"currency"`
}

type RedemptionRefunded struct {
	RedemptionID string    `json:"redemption_id"`
	RefundMin    int64     `json:"refund_min"`
	RefundedAt   time.Time `json:"refunded_at"`
}

// AdjustmentLine 是调整分录的单行；借方合计必须等于贷方合计。
type AdjustmentLine struct {
	Account        string `json:"account"` // AR_ORGANIZER / AP_MERCHANT
	CounterpartyID string `json:"counterparty_id"`
	MatchID        string `json:"match_id,omitempty"`
	ProgramCode    string `json:"program_code,omitempty"`
	DC             string `json:"dc"` // D 借 / C 贷
	AmountMin      int64  `json:"amount_min"`
}

type AdjustmentPosted struct {
	AdjustmentID  string           `json:"adjustment_id"`
	OriginEventID string           `json:"origin_event_id,omitempty"`
	OriginPeriod  string           `json:"origin_period,omitempty"`
	TargetPeriod  string           `json:"target_period"`
	Reason        string           `json:"reason"`
	CreatedBy     string           `json:"created_by"`
	PostedAt      time.Time        `json:"posted_at"`
	Lines         []AdjustmentLine `json:"lines"`
}

type PeriodClosed struct {
	Period      string    `json:"period"`
	ClosedAt    time.Time `json:"closed_at"`
	RuleVersion string    `json:"rule_version"`
	ClosedSeq   int64     `json:"closed_seq"`
	EntryCount  int       `json:"entry_count"`
	PrevHash    string    `json:"prev_hash,omitempty"`
	Hash        string    `json:"hash"`
	Totals      []Total   `json:"totals"`
}

type Total struct {
	Dimension     string `json:"dimension"` // merchant / organizer / match / program / stage
	Key           string `json:"key"`
	GrossMin      int64  `json:"gross_min"`
	ReceivableMin int64  `json:"receivable_min"`
	PayableMin    int64  `json:"payable_min"`
}

const (
	StagePreMatch  = "pre_match"
	StageInMatch   = "in_match"
	StagePostMatch = "post_match"
)

const (
	KindSubsidy  = "subsidy"
	KindReversal = "reversal"
	KindAdjust   = "adjustment"
)

// Posting 是凭证行。补贴凭证展开为两行（借应收/贷应付），
// 冲正方向相反；调整分录由财务显式给出多行且必须借贷平衡。
type Posting struct {
	Account        string `json:"account"`
	CounterpartyID string `json:"counterparty_id"`
	MatchID        string `json:"match_id,omitempty"`
	ProgramCode    string `json:"program_code,omitempty"`
	DC             string `json:"dc"` // D 借 / C 贷
	AmountMin      int64  `json:"amount_min"`
}

// RedemptionView 用于审计与查询：每笔核销的归因结论与计算轨迹。
type RedemptionView struct {
	RedemptionID string    `json:"redemption_id"`
	MerchantID   string    `json:"merchant_id"`
	ProgramCode  string    `json:"program_code"`
	MatchID      string    `json:"match_id"`
	EventName    string    `json:"event_name,omitempty"`
	Stage        string    `json:"stage"`
	ConsumedAt   time.Time `json:"consumed_at"`
	UploadedAt   time.Time `json:"uploaded_at"`
	AmountMin    int64     `json:"amount_min"`
	Currency     string    `json:"currency"`
	SubsidyMin   int64     `json:"subsidy_min"`  // 初次补贴
	ReversedMin  int64     `json:"reversed_min"` // 已入账冲正（正数）
	AutoNetMin   int64     `json:"auto_net_min"` // 自动凭证净额 = 补贴 - 已入账冲正
	BlockedMin   int64     `json:"blocked_min"`  // 封账挂起的有符号影响（正补贴/负冲正）
	AdjustedMin  int64     `json:"adjusted_min"` // 已由调整分录承接的挂起金额（有符号）
	Status       string    `json:"status"`
	Reasons      []string  `json:"reasons,omitempty"`
	RuleVersion  string    `json:"rule_version"`
	EventIDs     []string  `json:"event_ids"`
}

const (
	StatusAccepted        = "accepted"
	StatusRejected        = "rejected"
	StatusReversedPartial = "reversed_partial"
	StatusReversedFull    = "reversed_full"
	StatusRetained        = "retained" // 改期/赛后延时按规则保留
	StatusBlocked         = "blocked_pending_adjustment"
	StatusAdjusted        = "adjusted"
)

var identityFieldHints = []string{"name", "id_number", "id_no", "idcard", "phone", "mobile", "passport"}

// containsIdentityField 检查原始载荷是否夹带多余身份字段。
func containsIdentityField(raw json.RawMessage) bool {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return false
	}
	for k := range probe {
		lk := strings.ToLower(k)
		for _, h := range identityFieldHints {
			if strings.Contains(lk, h) {
				return true
			}
		}
	}
	return false
}
