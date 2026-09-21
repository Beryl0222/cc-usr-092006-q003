package settlement

import "sort"

// Explanation 把一条分录回溯到脱敏后的原始事件、规则版本与冲正/调整链，
// 供审计人员解释任意一笔应收应付。
type Explanation struct {
	Posting     Posting      `json:"posting"`
	SourceEvent *StoredEvent `json:"source_event,omitempty"`
	Chain       []Posting    `json:"chain"` // 同一冲正/调整链上的其余分录
}

// Explain 沿冲正与调整引用双向遍历，汇集与指定分录关联的完整链条。
func (e *Engine) Explain(id string) (Explanation, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	root := e.findPostingLocked(id)
	if root == nil {
		return Explanation{}, errUnknownPosting
	}
	adj := map[string][]string{}
	for _, p := range e.postings {
		if p.Reverses != "" {
			adj[p.Reverses] = append(adj[p.Reverses], p.ID)
		}
		for _, r := range p.Refs {
			adj[r] = append(adj[r], p.ID)
		}
	}
	seen := map[string]bool{id: true}
	queue := []string{id}
	var chain []Posting
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		p := e.findPostingLocked(cur)
		if p == nil {
			continue
		}
		if cur != id {
			chain = append(chain, *p)
		}
		next := append([]string{}, adj[cur]...)
		if p.Reverses != "" {
			next = append(next, p.Reverses)
		}
		next = append(next, p.Refs...)
		for _, n := range next {
			if !seen[n] {
				seen[n] = true
				queue = append(queue, n)
			}
		}
	}
	sort.Slice(chain, func(i, j int) bool { return chain[i].ID < chain[j].ID })
	ex := Explanation{Posting: *root, Chain: chain}
	if se, ok := e.stored[root.SourceEvent]; ok {
		ex.SourceEvent = &se
	}
	return ex, nil
}
