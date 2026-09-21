package settlement

import (
	"encoding/json"
	"time"
)

// EventType 是统一接入的事件类型。
type EventType string

const (
	EventTicket EventType = "ticket"        // 票务状态（出票/退票/改期/取消）
	EventEntry  EventType = "entry"         // 实名入场
	EventGrant  EventType = "benefit_grant" // 城市权益发放
	EventRedeem EventType = "redemption"    // 商户核销（含跨午夜迟到流水）
	EventRefund EventType = "refund"        // 商户退款（支持部分退款）
	EventSettle EventType = "settlement"    // 结算（封账指定账期）
)

// InboundEvent 是统一事件信封。ID 为幂等键，重复投递不会产生二次补贴。
type InboundEvent struct {
	ID         string          `json:"id"`
	Type       EventType       `json:"type"`
	OccurredAt time.Time       `json:"occurred_at"` // 业务发生时间，携带明确时区偏移
	Payload    json.RawMessage `json:"payload"`
}

// StoredEvent 是脱敏后落盘的原始事件，供审计回溯；不保存任何明文身份标识。
type StoredEvent struct {
	ID         string          `json:"id"`
	Type       EventType       `json:"type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload"`
}

// EventResult 是单个事件的处理结果。
type EventResult struct {
	Status   string   `json:"status"` // applied | duplicate | rejected
	Reason   string   `json:"reason,omitempty"`
	Postings []string `json:"postings,omitempty"`
}

// TicketPayload 票务状态。Status: issued | refunded | rescheduled | cancelled。
type TicketPayload struct {
	TicketToken string `json:"ticket_token"`
	EventID     string `json:"event_id"`
	Status      string `json:"status"`
	NewEventID  string `json:"new_event_id,omitempty"` // 改期后的目标场次
}

// EntryPayload 实名入场。
type EntryPayload struct {
	TicketToken string `json:"ticket_token"`
	EventID     string `json:"event_id"`
}

// GrantPayload 城市权益发放。Quota 为权益额度（分）。
type GrantPayload struct {
	BenefitID   string `json:"benefit_id"`
	ProgramCode string `json:"program_code"`
	TicketToken string `json:"ticket_token"`
	Quota       Money  `json:"quota"`
}

// RedeemPayload 商户核销流水。Amount 为消费金额（分）。
type RedeemPayload struct {
	BenefitID  string `json:"benefit_id"`
	MerchantID string `json:"merchant_id"`
	Amount     Money  `json:"amount"`
}

// RefundPayload 商户退款，Amount 小于原核销金额时按比例冲正补贴。
type RefundPayload struct {
	RedemptionEventID string `json:"redemption_event_id"`
	Amount            Money  `json:"amount"`
}

// SettlePayload 结算事件，封账指定账期（YYYY-MM）。
type SettlePayload struct {
	Period string `json:"period"`
}
