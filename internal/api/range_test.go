package api

import "testing"

func TestParseSingleRange(t *testing.T) {
	cases := []struct {
		header    string
		wantOK    bool
		wantStart int64
		wantEnd   int64 // -1 表示读到末尾
	}{
		{"bytes=0-9", true, 0, 10},
		{"bytes=100-200", true, 100, 201},
		{"bytes=10-", true, 10, -1},
		{"bytes=-100", false, 0, 0},      // 后缀范围不支持
		{"bytes=0-9,10-19", false, 0, 0}, // 多区间不支持
		{"bytes=9-5", false, 0, 0},       // 反向区间
		{"items=0-9", false, 0, 0},
		{"bytes=", false, 0, 0},
	}
	for _, c := range cases {
		s, e, ok, err := parseSingleRange(c.header)
		if ok != c.wantOK {
			t.Errorf("header=%q: ok 期望 %v 实际 %v (err=%v)", c.header, c.wantOK, ok, err)
			continue
		}
		if ok && (s != c.wantStart || e != c.wantEnd) {
			t.Errorf("header=%q: 范围期望 [%d,%d) 实际 [%d,%d)", c.header, c.wantStart, c.wantEnd, s, e)
		}
	}
}
