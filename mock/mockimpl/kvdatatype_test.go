package mockimpl

import (
	"encoding/json"
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/golang/snappy"
)

// kv_engine sends a response only the datatype bits the client enabled with
// HELLO (Connection::getEnabledDatatypes).
func TestResponseDatatypeFollowsHello(t *testing.T) {
	for _, test := range []struct {
		name     string
		features []memd.HelloFeature
		datatype uint8
	}{
		{name: "without JSON", datatype: 0},
		{name: "with JSON", features: []memd.HelloFeature{memd.FeatureJSON}, datatype: uint8(memd.DatatypeFlagJSON)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newTestKvConn(t, test.features...)
			c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: make([]byte, 8)})
			for _, req := range []*memd.Packet{
				{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")},
				{Command: memd.CmdGAT, Vbucket: c.vbID, Key: []byte("doc"), Extras: make([]byte, 4)},
			} {
				command := req.Command
				if resp := c.Do(req); resp.Datatype != test.datatype {
					t.Errorf("%s: expected datatype 0x%02x, got 0x%02x", command.Name(), test.datatype, resp.Datatype)
				}
			}
		})
	}
}

// TestRequestDatatypeMustBeEnabled checks that a request carrying datatype
// bits the client did not enable with HELLO is refused, as verify_header does,
// before its frames are checked.
func TestRequestDatatypeMustBeEnabled(t *testing.T) {
	invalidLevel := &memd.DurabilityLevelFrame{DurabilityLevel: 0x05}
	for _, test := range []struct {
		name     string
		features []memd.HelloFeature
		pak      memd.Packet
		status   memd.StatusCode
		context  string
	}{
		{name: "JSON", pak: memd.Packet{Command: memd.CmdSet, Datatype: uint8(memd.DatatypeFlagJSON)},
			status: memd.StatusInvalidArgs, context: "Datatype (json) not enabled for the connection"},
		{name: "JSON and snappy", pak: memd.Packet{Command: memd.CmdSet, Datatype: uint8(memd.DatatypeFlagJSON | memd.DatatypeFlagCompressed)},
			status: memd.StatusInvalidArgs, context: "Datatype (snappy,json) not enabled for the connection"},
		{name: "JSON before frames", pak: memd.Packet{Command: memd.CmdSet, Datatype: uint8(memd.DatatypeFlagJSON), DurabilityLevelFrame: invalidLevel},
			status: memd.StatusInvalidArgs, context: "Datatype (json) not enabled for the connection"},
		{name: "JSON enabled", features: []memd.HelloFeature{memd.FeatureJSON}, pak: memd.Packet{Command: memd.CmdSet, Datatype: uint8(memd.DatatypeFlagJSON)},
			status: memd.StatusSuccess},
		{name: "JSON on GET", features: []memd.HelloFeature{memd.FeatureJSON}, pak: memd.Packet{Command: memd.CmdGet, Datatype: uint8(memd.DatatypeFlagJSON)},
			status: memd.StatusInvalidArgs, context: "Request datatype invalid"},
		// verify_header checks the extras, key, value and CAS before the
		// datatype, with or without frames.
		{name: "extras before datatype", pak: memd.Packet{Command: memd.CmdSet, Datatype: uint8(memd.DatatypeFlagJSON), Extras: make([]byte, 4)},
			status: memd.StatusInvalidArgs, context: "Request must include extras of length 8"},
		{name: "value before datatype", pak: memd.Packet{Command: memd.CmdGet, Datatype: uint8(memd.DatatypeFlagJSON), Value: []byte("1")},
			status: memd.StatusInvalidArgs, context: "Request must not include value"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newTestKvConn(t, append(test.features, memd.FeatureAltRequests, memd.FeatureSyncReplication)...)

			pak := test.pak
			pak.Vbucket = c.vbID
			pak.Key = []byte("doc")
			switch {
			case pak.Command == memd.CmdSet && pak.Extras == nil:
				pak.Extras = make([]byte, 8)
				pak.Value = []byte(`{}`)
			case pak.Command == memd.CmdSet:
				pak.Value = []byte(`{}`)
			}
			if pak.Datatype&uint8(memd.DatatypeFlagCompressed) != 0 {
				pak.Value = snappy.Encode(nil, pak.Value)
			}
			resp := c.Do(&pak)
			if resp.Status != test.status {
				t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(test.status), uint16(resp.Status))
			}
			if test.context == "" {
				return
			}
			var body struct {
				Error struct {
					Context string `json:"context"`
				} `json:"error"`
			}
			if err := json.Unmarshal(resp.Value, &body); err != nil || body.Error.Context != test.context {
				t.Errorf("expected error context %q, got %q", test.context, resp.Value)
			}
		})
	}
}
