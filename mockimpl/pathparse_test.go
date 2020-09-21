package mockimpl

import (
	"testing"
)

func TestPathParser(t *testing.T) {
	x := NewPathParser("/test/*/lol/*/nope/**/*/*/yay")

	a := x.ParseParts("/test/0/lol/1/nope/haha/we/win/3/4/yay")
	if a == nil {
		t.Fatalf("failed to parse at all")
	}
	if a[0] != "0" || a[1] != "1" || a[2] != "haha/we/win" || a[3] != "3" || a[4] != "4" {
		t.Fatalf("failed to parse correctly")
	}

	b := x.ParseParts("/test/0/lolneg/1/nope/haha/we/win/2/3/yay")
	if b != nil {
		t.Fatalf("failed to parse correctly")
	}

	if x.Match("/test/0/lolneg/1/nope/haha/we/win/2/3/yay") {
		t.Fatalf("failed to fail match")
	}
}
