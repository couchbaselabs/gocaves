package mockimpl

import (
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
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
