package mockimpl

import (
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
)

func TestMutationTokenFollowsHello(t *testing.T) {
	for _, test := range []struct {
		name       string
		features   []memd.HelloFeature
		extrasSize int
	}{
		{name: "without seqno", extrasSize: 0},
		{name: "with seqno", features: []memd.HelloFeature{memd.FeatureSeqNo}, extrasSize: 16},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newTestKvConn(t, test.features...)

			for _, req := range []*memd.Packet{
				{Command: memd.CmdSet, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)},
				{
					Command: memd.CmdSubDocMultiMutation,
					Key:     []byte("doc"),
					Value:   encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Path: "a", Value: "1"}),
				},
				{Command: memd.CmdDelete, Key: []byte("doc")},
			} {
				req.Vbucket = c.vbID
				resp := c.mustSucceed(req)
				if len(resp.Extras) != test.extrasSize {
					t.Errorf("%s: expected %d bytes of extras, got %d", req.Command.Name(), test.extrasSize, len(resp.Extras))
				}
			}
		})
	}
}
