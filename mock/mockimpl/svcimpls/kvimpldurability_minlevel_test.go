package svcimpls

import (
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
)

// TestRaiseDurability checks raiseDurability against kv_engine's
// Item::increaseDurabilityLevel and EventuallyPersistentEngine::remove.
func TestRaiseDurability(t *testing.T) {
	level := func(l memd.DurabilityLevel) *memd.DurabilityLevelFrame {
		return &memd.DurabilityLevelFrame{DurabilityLevel: l}
	}
	timeout := &memd.DurabilityTimeoutFrame{DurabilityTimeout: 3 * time.Second}
	tests := []struct {
		name     string
		minLevel string
		pak      memd.Packet
		want     *memd.DurabilityLevelFrame
	}{
		{"none leaves a plain write", "none", memd.Packet{Command: memd.CmdSet}, nil},
		{"plain write raised", "majority", memd.Packet{Command: memd.CmdSet}, level(memd.DurabilityLevelMajority)},
		{"delete raised", "persistToMajority", memd.Packet{Command: memd.CmdDelete}, level(memd.DurabilityLevelPersistToMajority)},
		{"subdoc mutation raised", "majorityAndPersistActive", memd.Packet{Command: memd.CmdSubDocMultiMutation},
			level(memd.DurabilityLevelMajorityAndPersistOnMaster)},
		{"lower level raised", "persistToMajority", memd.Packet{Command: memd.CmdAppend,
			DurabilityLevelFrame: level(memd.DurabilityLevelMajority)}, level(memd.DurabilityLevelPersistToMajority)},
		{"higher level kept", "majority", memd.Packet{Command: memd.CmdIncrement,
			DurabilityLevelFrame: level(memd.DurabilityLevelPersistToMajority)}, level(memd.DurabilityLevelPersistToMajority)},
		{"touch not raised", "majority", memd.Packet{Command: memd.CmdTouch}, nil},
		{"get and touch not raised", "majority", memd.Packet{Command: memd.CmdGAT}, nil},
		{"read not raised", "majority", memd.Packet{Command: memd.CmdGet}, nil},
	}
	for _, tt := range tests {
		pak := tt.pak
		pak.DurabilityTimeoutFrame = timeout
		raiseDurability(tt.minLevel, &pak)
		switch {
		case tt.want == nil && pak.DurabilityLevelFrame != nil:
			t.Errorf("%s: expected no level, got %v", tt.name, pak.DurabilityLevelFrame.DurabilityLevel)
		case tt.want != nil && (pak.DurabilityLevelFrame == nil || *pak.DurabilityLevelFrame != *tt.want):
			t.Errorf("%s: expected level %v, got %v", tt.name, tt.want.DurabilityLevel, pak.DurabilityLevelFrame)
		case pak.DurabilityTimeoutFrame != timeout:
			t.Errorf("%s: the timeout was changed", tt.name)
		}
	}
}
