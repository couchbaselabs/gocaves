package testmode

import (
	"reflect"
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/checks"
)

// A received request lists every frame in UnsupportedFrames, which the fields
// memd decodes repeat, so the report takes its frames from the list alone.
func TestReceivedRequestFramesReportedOnce(t *testing.T) {
	pak := &checks.RecordedPacket{Data: memd.Packet{
		Magic:                memd.CmdMagicReq,
		Command:              memd.CmdSet,
		BarrierFrame:         &memd.BarrierFrame{},
		DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
		UnsupportedFrames: []memd.UnsupportedFrame{
			{Type: 1, Data: []byte{0x01}},
			{Type: 0},
			{Type: 9, Data: []byte{0xaa}},
		},
	}}

	got := translateCapturedPacket(pak).ExtFrames
	want := []jsonPacketExtraFrame{
		{"type": "durabilityLevel", "level": uint8(1), "levelStr": "majority"},
		{"type": "barrier"},
		{"type": "unk:9", "dataBase64": "qg=="},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected frames %v, got %v", want, got)
	}
}
