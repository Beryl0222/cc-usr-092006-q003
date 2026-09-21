package settlement

import "sort"

// Scope 描述调用方可见的数据范围：财务全量、主办方限自有场次、商户限自身。
type Scope struct {
	Role       string
	MerchantID string
	MatchID    string
}

// LineView 是按数据范围过滤后的记账行视图，不含任何身份引用。
type LineView struct {
	PostingID   string `json:"posting_id"`
	Kind        string `json:"kind"`
	Period      string `json:"period"`
	Account     string `json:"account"`
	Debit       Money  `json:"debit,omitempty"`
	Credit      Money  `json:"credit,omitempty"`
	MatchID     string `json:"match_id,omitempty"`
	ProgramCode string `json:"program_code,omitempty"`
	Phase       string `json:"phase,omitempty"`
	MerchantID  string `json:"merchant_id,omitempty"`
	RuleVersion string `json:"rule_version,omitempty"`
}

// LedgerView 按角色过滤账簿：财务见全部；主办方限其场次；商户限自身应付款行。
func (e *Engine) LedgerView(s Scope, period string) ([]LineView, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch s.Role {
	case RoleFinance:
	case RoleOrganizer:
		if s.MatchID == "" {
			return nil, ErrForbidden
		}
	case RoleMerchant:
		if s.MerchantID == "" {
			return nil, ErrForbidden
		}
	default:
		return nil, ErrForbidden
	}
	var out []LineView
	for _, p := range e.postings {
		if period != "" && p.Period != period {
			continue
		}
		switch s.Role {
		case RoleOrganizer:
			if p.MatchID != s.MatchID {
				continue
			}
		case RoleMerchant:
			if p.MerchantID != s.MerchantID {
				continue
			}
		}
		for _, l := range p.Lines {
			if s.Role == RoleMerchant && l.Account != "payable:merchant:"+s.MerchantID {
				continue
			}
			out = append(out, LineView{
				PostingID: p.ID, Kind: p.Kind, Period: p.Period,
				Account: l.Account, Debit: l.Debit, Credit: l.Credit,
				MatchID: p.MatchID, ProgramCode: p.ProgramCode, Phase: p.Phase,
				MerchantID: p.MerchantID, RuleVersion: p.RuleVersion,
			})
		}
	}
	return out, nil
}

// ConsumptionRow 是按方案与阶段聚合的带动消费数据，供主办方与城市合作伙伴核对。
type ConsumptionRow struct {
	MatchID     string `json:"match_id"`
	ProgramCode string `json:"program_code"`
	Phase       string `json:"phase"`
	Redemptions int    `json:"redemptions"`
	Gross       Money  `json:"gross"`   // 净消费额（已扣除冲正）
	Subsidy     Money  `json:"subsidy"` // 净补贴额（已扣除冲正）
}

// Consumption 聚合指定场次的带动消费；主办方仅可查询自有场次。
func (e *Engine) Consumption(s Scope, matchID string) ([]ConsumptionRow, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch s.Role {
	case RoleFinance:
	case RoleOrganizer:
		if s.MatchID == "" || s.MatchID != matchID {
			return nil, ErrForbidden
		}
	default:
		return nil, ErrForbidden
	}
	type key struct{ program, phase string }
	rows := map[key]*ConsumptionRow{}
	var keys []key
	for _, p := range e.postings {
		if p.MatchID != matchID {
			continue
		}
		k := key{p.ProgramCode, p.Phase}
		row, ok := rows[k]
		if !ok {
			row = &ConsumptionRow{MatchID: matchID, ProgramCode: p.ProgramCode, Phase: p.Phase}
			rows[k] = row
			keys = append(keys, k)
		}
		if p.Kind == "standard" {
			row.Redemptions++
		}
		row.Gross += p.Gross
		row.Subsidy += p.Subsidy
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].program != keys[j].program {
			return keys[i].program < keys[j].program
		}
		return keys[i].phase < keys[j].phase
	})
	out := make([]ConsumptionRow, 0, len(keys))
	for _, k := range keys {
		out = append(out, *rows[k])
	}
	return out, nil
}
