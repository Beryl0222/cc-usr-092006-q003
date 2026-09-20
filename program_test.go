package settlement

import "testing"

func TestReadProgramsPreservesTimezone(t *testing.T) {
	programs, err := ReadPrograms("data/programs.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(programs) != 2 || programs[0].StartsAt.Location().String() == "UTC" {
		t.Fatalf("权益窗口或时区读取错误：%v", programs)
	}
}
