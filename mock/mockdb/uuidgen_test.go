package mockdb

import (
	"testing"
	"time"
)

func TestGenerateNewCasDiffersWithinOneClockReading(t *testing.T) {
	now := time.Now()
	first := GenerateNewCas(now)
	second := GenerateNewCas(now)
	if first == second {
		t.Fatalf("two CAS values generated for the same time are equal: %#x", first)
	}
}
