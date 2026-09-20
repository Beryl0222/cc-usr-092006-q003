package settlement

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// SubsidyPolicy 描述合作方案的补贴比例（基点）与单笔封顶（最小货币单位）。
type SubsidyPolicy struct {
	BPS    int   `json:"bps"`
	CapMin int64 `json:"cap_min"`
}

// Program 描述一项赛事城市权益的有效窗口与合作策略。
type Program struct {
	Code          string        `json:"code"`
	EventID       string        `json:"event_id"`
	Name          string        `json:"name,omitempty"`
	StartsAt      time.Time     `json:"starts_at"`
	EndsAt        time.Time     `json:"ends_at"`
	Currency      string        `json:"currency"`
	EntryRequired bool          `json:"entry_required"`
	Subsidy       SubsidyPolicy `json:"subsidy"`
}

func ReadPrograms(path string) ([]Program, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var programs []Program
	if err := decodeJSON(raw, &programs); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, program := range programs {
		if program.Code == "" || !program.EndsAt.After(program.StartsAt) || program.Currency == "" {
			return nil, fmt.Errorf("权益方案资料无效：%s", program.Code)
		}
		if program.Subsidy.BPS < 0 || program.Subsidy.BPS > 10000 || program.Subsidy.CapMin < 0 {
			return nil, fmt.Errorf("权益方案补贴策略无效：%s", program.Code)
		}
		if seen[program.Code] {
			return nil, fmt.Errorf("权益方案编码重复：%s", program.Code)
		}
		seen[program.Code] = true
	}
	return programs, nil
}

// Match 是赛事基础资料；改期/取消等状态变化由事件驱动，不直接改写资料。
type Match struct {
	EventID     string    `json:"event_id"`
	Name        string    `json:"name"`
	OrganizerID string    `json:"organizer_id"`
	Currency    string    `json:"currency"`
	StartsAt    time.Time `json:"starts_at"`
	EndsAt      time.Time `json:"ends_at"`
}

func ReadMatches(path string) ([]Match, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var matches []Match
	if err := decodeJSON(raw, &matches); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, m := range matches {
		if m.EventID == "" || m.OrganizerID == "" || m.Currency == "" || !m.EndsAt.After(m.StartsAt) {
			return nil, fmt.Errorf("赛事资料无效：%s", m.EventID)
		}
		if seen[m.EventID] {
			return nil, fmt.Errorf("赛事编码重复：%s", m.EventID)
		}
		seen[m.EventID] = true
	}
	return matches, nil
}

// RuleSet 是可版本化的清算规则。账期快照记录出账时的规则版本，
// 使历史金额可按当时规则复算与解释。
type RuleSet struct {
	Version                string    `json:"version"`
	EffectiveAt            time.Time `json:"effective_at"`
	AccountTimezone        string    `json:"account_timezone"`
	LateUploadGraceMinutes int       `json:"late_upload_grace_minutes"`
	EntryGraceMinutes      int       `json:"entry_grace_minutes"`
	PostMatchRetention     bool      `json:"post_match_retention"`
	RescheduleRetention    bool      `json:"reschedule_retention"`
}

func ReadRules(path string) ([]RuleSet, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rules []RuleSet
	if err := decodeJSON(raw, &rules); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, r := range rules {
		if r.Version == "" || r.AccountTimezone == "" || r.LateUploadGraceMinutes < 0 {
			return nil, fmt.Errorf("规则版本资料无效：%s", r.Version)
		}
		if seen[r.Version] {
			return nil, fmt.Errorf("规则版本重复：%s", r.Version)
		}
		seen[r.Version] = true
	}
	return rules, nil
}

// decodeJSON 拒绝未知字段，防止调用方在载荷中夹带系统不接受的信息。
func decodeJSON(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}
