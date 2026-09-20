package settlement

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Program 描述一项赛事城市权益的有效窗口。
type Program struct {
	Code     string    `json:"code"`
	EventID  string    `json:"event_id"`
	StartsAt time.Time `json:"starts_at"`
	EndsAt   time.Time `json:"ends_at"`
	Currency string    `json:"currency"`
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
		if program.Code == "" || !program.EndsAt.After(program.StartsAt) || program.Currency == "" {
			return nil, fmt.Errorf("权益方案资料无效")
		}
	}
	return programs, nil
}
