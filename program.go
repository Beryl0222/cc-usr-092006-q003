package settlement

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Phase 描述合作方案内的一个活动阶段，窗口均携带明确时区偏移。
// PostEvent 标记赛后延时阶段，该阶段内的消费按方案规则决定保留或冲正。
type Phase struct {
	Name      string    `json:"name"`
	StartsAt  time.Time `json:"starts_at"`
	EndsAt    time.Time `json:"ends_at"`
	PostEvent bool      `json:"post_event,omitempty"`
}

// Rules 记录方案当前生效的清算规则版本，每笔分录都会引用该版本以便审计复算。
type Rules struct {
	Version         string `json:"version"`
	SubsidyBps      int    `json:"subsidy_bps"`       // 补贴比例（基点，万分之一）
	PartnerShareBps int    `json:"partner_share_bps"` // 城市合作伙伴承担补贴的比例（基点）
	PostEventPolicy string `json:"post_event_policy"` // keep：延时消费补贴保留；reverse：自动冲正
	OnReschedule    string `json:"on_reschedule"`     // keep：改期后保留；reverse：改期即冲正
}

// Program 描述一项赛事城市权益合作方案及其有效窗口。
type Program struct {
	Code     string    `json:"code"`
	EventID  string    `json:"event_id"`
	StartsAt time.Time `json:"starts_at"`
	EndsAt   time.Time `json:"ends_at"`
	Currency string    `json:"currency"`
	Phases   []Phase   `json:"phases"`
	Rules    Rules     `json:"rules"`
}

func (p Program) location() *time.Location { return p.StartsAt.Location() }

// phaseAt 返回业务时间 t 所属的活动阶段；不在方案窗口内时返回 nil。
// 比较统一在方案自带时区下进行，跨午夜的流水据此归入正确场次。
func (p Program) phaseAt(t time.Time) *Phase {
	local := t.In(p.location())
	if local.Before(p.StartsAt) || !local.Before(p.EndsAt) {
		return nil
	}
	for i := range p.Phases {
		ph := p.Phases[i]
		if !local.Before(ph.StartsAt) && local.Before(ph.EndsAt) {
			return &ph
		}
	}
	if len(p.Phases) == 0 {
		return &Phase{Name: "", StartsAt: p.StartsAt, EndsAt: p.EndsAt}
	}
	return nil
}

func validateProgram(p Program) error {
	if p.Code == "" || p.EventID == "" || !p.EndsAt.After(p.StartsAt) || p.Currency == "" {
		return fmt.Errorf("权益方案资料无效")
	}
	if p.Rules.Version == "" ||
		p.Rules.SubsidyBps < 0 || p.Rules.SubsidyBps > 10000 ||
		p.Rules.PartnerShareBps < 0 || p.Rules.PartnerShareBps > 10000 {
		return fmt.Errorf("权益方案 %s 规则资料无效", p.Code)
	}
	switch p.Rules.PostEventPolicy {
	case "keep", "reverse":
	default:
		return fmt.Errorf("权益方案 %s 延时消费策略无效", p.Code)
	}
	switch p.Rules.OnReschedule {
	case "keep", "reverse":
	default:
		return fmt.Errorf("权益方案 %s 改期策略无效", p.Code)
	}
	last := p.StartsAt
	for _, ph := range p.Phases {
		if ph.Name == "" || !ph.EndsAt.After(ph.StartsAt) ||
			ph.StartsAt.Before(p.StartsAt) || ph.EndsAt.After(p.EndsAt) || ph.StartsAt.Before(last) {
			return fmt.Errorf("权益方案 %s 阶段窗口无效", p.Code)
		}
		last = ph.EndsAt
	}
	return nil
}

func ReadPrograms(path string) ([]Program, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var programs []Program
	if err := json.Unmarshal(raw, &programs); err != nil {
		return nil, err
	}
	for _, program := range programs {
		if err := validateProgram(program); err != nil {
			return nil, err
		}
	}
	return programs, nil
}
