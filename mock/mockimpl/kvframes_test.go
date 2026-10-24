package mockimpl

import (
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
)

// kv_engine's parseFrameExtras checks each frame in the order sent, by its id,
// and refuses the request at the first frame it rejects. memd decodes frames
// into fields that keep neither their order nor ids above 0xff.
func TestFramesValidatedByKvEngineID(t *testing.T) {
	const unknownFrameInfo = memd.StatusCode(0x80)
	set := func(c *testKvConn, opaque uint32, frames []byte) {
		writeRawRequest(t, c, memd.CmdSet, opaque, frames, make([]byte, 8), []byte("doc"), []byte(`{}`))
	}
	noop := func(c *testKvConn, opaque uint32, frames []byte) {
		writeRawRequest(t, c, memd.CmdNoop, opaque, frames, nil, nil, nil)
	}
	for _, test := range []struct {
		name    string
		send    func(*testKvConn, uint32, []byte)
		frames  []byte
		status  memd.StatusCode
		context string
	}{
		// memd decodes id 3 as an OpenTracing frame; kv_engine defines it as
		// VBucketUuid.
		{name: "VBucketUuid of 1 byte", send: noop, frames: []byte{0x31, 0xaa}, status: memd.StatusInvalidArgs, context: "VBucketUuid invalid size:1"},
		{name: "VBucketUuid on NOOP", send: noop, frames: append([]byte{0x38}, make([]byte, 8)...), status: memd.StatusNotSupported, context: "This command does not support VBucketUuid"},
		{name: "VBucketUuid on SET", send: set, frames: append([]byte{0x38}, make([]byte, 8)...), status: memd.StatusSuccess},
		{name: "id 8", send: noop, frames: []byte{0x80}, status: unknownFrameInfo},
		{name: "escaped id 15", send: noop, frames: []byte{0xf0, 0x00}, status: unknownFrameInfo},
		// memd wraps id 256 to a barrier and id 257 to a durability frame.
		{name: "escaped id 256", send: noop, frames: []byte{0xf0, 0xf1}, status: unknownFrameInfo},
		{name: "escaped id 257", send: set, frames: []byte{0xf1, 0xf2, 0x01}, status: unknownFrameInfo},
		{name: "stream id of 2 bytes", send: noop, frames: []byte{0x22, 0x00, 0x01}, status: memd.StatusSuccess},
		{name: "stream id of 1 byte", send: noop, frames: []byte{0x21, 0x00}, status: memd.StatusInvalidArgs, context: "DcpStreamId invalid size:1"},
		{name: "empty impersonated user", send: noop, frames: []byte{0x40}, status: memd.StatusInvalidArgs, context: "Impersonated user must be set"},
		{name: "empty extra privilege", send: noop, frames: []byte{0x60}, status: memd.StatusInvalidArgs, context: "Privilege name must be set"},
		{name: "two impersonated users", send: noop, frames: []byte{0x41, 'a', 0x41, 'b'}, status: memd.StatusInvalidArgs, context: "Only one impersonate frame may be present"},
		{name: "impersonated user and token auth id", send: noop, frames: []byte{0x41, 'a', 0x72, 0x00, 0x01}, status: memd.StatusInvalidArgs, context: "Only one impersonate frame may be present"},
		{name: "token auth id of 1 byte", send: noop, frames: []byte{0x71, 0x00}, status: memd.StatusInvalidArgs, context: "ImpersonateWithTokenAuthDataId invalid size:1"},
		{name: "PreserveTtl with data", send: set, frames: []byte{0x51, 0x00}, status: memd.StatusInvalidArgs, context: "PreserveTtl should not contain value"},
		{name: "unknown id before a barrier with data", send: noop, frames: []byte{0x80, 0x01, 0xaa}, status: unknownFrameInfo},
		{name: "barrier with data before an unknown id", send: noop, frames: []byte{0x01, 0xaa, 0x80}, status: memd.StatusInvalidArgs, context: "Barrier should not contain value"},
		// memd keeps the last durability frame.
		{name: "invalid durability level before a valid one", send: set, frames: []byte{0x11, 0x00, 0x11, 0x01}, status: memd.StatusDurabilityInvalidLevel, context: "Content represents an invalid requirement specification"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication, memd.FeaturePreserveExpiry)
			test.send(c, 1, test.frames)
			resp := readResponse(t, c)
			if resp.Opaque != 1 || resp.Status != test.status {
				t.Fatalf("expected status 0x%02x for opaque 1, got 0x%02x for opaque %d", uint16(test.status), uint16(resp.Status), resp.Opaque)
			}
			switch {
			case test.context != "":
				expectErrorContext(t, resp, test.context)
			case test.status != memd.StatusSuccess && len(resp.Value) > 0:
				t.Errorf("expected no error body, got %q", resp.Value)
			}
			if test.status != memd.StatusSuccess {
				if resp := c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")}); resp.Status != memd.StatusKeyNotFound {
					t.Errorf("expected the refused request to store nothing, GET got status 0x%02x", uint16(resp.Status))
				}
			}
		})
	}
}
