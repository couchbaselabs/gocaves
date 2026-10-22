package mockimpl

import (
	"encoding/binary"
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
)

func TestMultiMutationReportsIndexOfFailedSpec(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)

	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSet,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Value:   []byte(`{"dictkey":"dictval"}`),
		Extras:  make([]byte, 8),
	})

	tests := []struct {
		name  string
		specs []subdocSpec
		index uint8
	}{
		{
			name: "body paths",
			specs: []subdocSpec{
				{Op: memd.SubDocOpReplace, Path: "dictkey", Value: "null"},
				{Op: memd.SubDocOpReplace, Path: "nested.nonexist", Value: "null"},
				{Op: memd.SubDocOpReplace, Path: "bad..bad", Value: "null"},
			},
			index: 1,
		},
		{
			// gocaves executes the xattr spec after the body specs, so the
			// failing spec is second in execution order but third in the
			// request.
			name: "xattr first",
			specs: []subdocSpec{
				{Op: memd.SubDocOpDictSet, Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagMkDirP, Path: "meta.x", Value: "1"},
				{Op: memd.SubDocOpReplace, Path: "dictkey", Value: "null"},
				{Op: memd.SubDocOpReplace, Path: "nested.nonexist", Value: "null"},
			},
			index: 2,
		},
	}

	for _, test := range tests {
		resp := c.Do(&memd.Packet{
			Command: memd.CmdSubDocMultiMutation,
			Vbucket: c.vbID,
			Key:     []byte("doc"),
			Value:   encodeMutationSpecs(test.specs...),
		})
		if resp.Status != memd.StatusSubDocBadMulti || len(resp.Value) != 3 {
			t.Errorf("%s: expected a multi-path failure, got status 0x%02x body %v", test.name, uint16(resp.Status), resp.Value)
			continue
		}
		if resp.Value[0] != test.index {
			t.Errorf("%s: expected failed spec index %d, got %d", test.name, test.index, resp.Value[0])
		}
		if status := memd.StatusCode(binary.BigEndian.Uint16(resp.Value[1:])); status != memd.StatusSubDocPathNotFound {
			t.Errorf("%s: expected spec status 0x%02x, got 0x%02x", test.name, uint16(memd.StatusSubDocPathNotFound), uint16(status))
		}
	}
}

func TestMultiMutationWithFailedXattrSpecIsNotStored(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)

	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSet,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Value:   []byte(`{"a":1}`),
		Extras:  make([]byte, 8),
	})

	// A virtual attribute is read-only, and kv_engine rejects the whole
	// request.
	resp := c.Do(&memd.Packet{
		Command: memd.CmdSubDocMultiMutation,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Value: encodeMutationSpecs(
			subdocSpec{Op: memd.SubDocOpDictSet, Flags: memd.SubdocFlagXattrPath, Path: "$document.x", Value: "1"},
			subdocSpec{Op: memd.SubDocOpDictSet, Path: "b", Value: "2"},
		),
	})
	if resp.Status != memd.StatusSubDocXattrCannotModifyVAttr {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusSubDocXattrCannotModifyVAttr), uint16(resp.Status))
	}

	resp = c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")})
	if string(resp.Value) != `{"a":1}` {
		t.Errorf("failed multi-mutation was stored: %s", resp.Value)
	}
}
