package settlement

import "time"

// Money 以最小货币单位（分）计值，全程整数运算，避免浮点误差。
type Money int64

// bpsOf 按基点（万分之一）计算分成，向下取整，结果确定可复算。
func bpsOf(amount Money, bps int) Money {
	return Money(int64(amount) * int64(bps) / 10000)
}

// periodOf 返回业务时间 t 在指定时区下的账期（YYYY-MM）。
func periodOf(t time.Time, loc *time.Location) string {
	return t.In(loc).Format("2006-01")
}

func nextPeriod(period string) string {
	t, _ := time.Parse("2006-01", period)
	return t.AddDate(0, 1, 0).Format("2006-01")
}
