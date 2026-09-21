package settlement

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

// Line 是一条复式记账行：借方或贷方其一非零。
type Line struct {
	Account string `json:"account"`
	Debit   Money  `json:"debit,omitempty"`
	Credit  Money  `json:"credit,omitempty"`
}

// Posting 是一组平衡记账行及其归因信息。
// Kind: standard（正常入账）| reversal（冲正）| adjustment（封账后调整分录）。
type Posting struct {
	ID          string   `json:"id"`
	Kind        string   `json:"kind"`
	Period      string   `json:"period"`
	MatchID     string   `json:"match_id,omitempty"`
	ProgramCode string   `json:"program_code,omitempty"`
	Phase       string   `json:"phase,omitempty"`
	MerchantID  string   `json:"merchant_id,omitempty"`
	SourceEvent string   `json:"source_event,omitempty"`
	Reverses    string   `json:"reverses,omitempty"`
	Refs        []string `json:"refs,omitempty"`
	RuleVersion string   `json:"rule_version,omitempty"`
	Reason      string   `json:"reason,omitempty"`
	Gross       Money    `json:"gross,omitempty"`   // 归因消费额（冲正为负）
	Subsidy     Money    `json:"subsidy,omitempty"` // 归因补贴额（冲正为负）
	Lines       []Line   `json:"lines"`
}

// Snapshot 是账期封账时生成的可复算快照。
// Totals 为各账户净额（借正贷负），Hash 覆盖该账期全部分录内容。
type Snapshot struct {
	Period   string           `json:"period"`
	Postings int              `json:"postings"`
	Totals   map[string]Money `json:"totals"`
	Hash     string           `json:"hash"`
}

// computeSnapshot 按分录 ID 排序后聚合，保证同一台账复算结果一致。
func computeSnapshot(postings []*Posting, period string) Snapshot {
	var ids []string
	byID := map[string]*Posting{}
	totals := map[string]Money{}
	for _, p := range postings {
		if p.Period != period {
			continue
		}
		ids = append(ids, p.ID)
		byID[p.ID] = p
		for _, l := range p.Lines {
			totals[l.Account] += l.Debit - l.Credit
		}
	}
	sort.Strings(ids)
	h := sha256.New()
	for _, id := range ids {
		p := byID[id]
		fmt.Fprintf(h, "%s|%s|%s|%s|%s|%s|%s|%s|%d|%d|",
			p.ID, p.Kind, p.MatchID, p.ProgramCode, p.Phase, p.MerchantID,
			p.Reverses, p.RuleVersion, p.Gross, p.Subsidy)
		for _, l := range p.Lines {
			fmt.Fprintf(h, "%s:%d:%d;", l.Account, l.Debit, l.Credit)
		}
		h.Write([]byte("\n"))
	}
	return Snapshot{Period: period, Postings: len(ids), Totals: totals, Hash: hex.EncodeToString(h.Sum(nil))}
}
