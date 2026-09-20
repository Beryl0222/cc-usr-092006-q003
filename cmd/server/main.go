// 赛事消费归因与清算后端（内存事件账本，可确定性重放）。
package main

import (
	"log"
	"net/http"
	"os"
	"time"

	settlement "example.com/event-settlement"
)

func main() {
	programs, err := settlement.ReadPrograms("data/programs.json")
	if err != nil {
		log.Fatalf("读取方案失败: %v", err)
	}
	matches, err := settlement.ReadMatches("data/matches.json")
	if err != nil {
		log.Fatalf("读取赛事失败: %v", err)
	}
	rules, err := settlement.ReadRules("data/rules.json")
	if err != nil {
		log.Fatalf("读取规则失败: %v", err)
	}
	catalog, err := settlement.BuildCatalog(programs, matches, rules)
	if err != nil {
		log.Fatalf("基础资料无效: %v", err)
	}
	store, err := settlement.NewStore(catalog)
	if err != nil {
		log.Fatalf("初始化账本失败: %v", err)
	}
	service := settlement.NewService(store)
	service.SetClock(func() time.Time { return time.Now() })

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	log.Printf("赛事清算服务监听 %s", addr)
	if err := http.ListenAndServe(addr, service.Routes()); err != nil {
		log.Fatal(err)
	}
}
