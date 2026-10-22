package mockimpl

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
)

func (c *testKvConn) expiry(key string) time.Time {
	c.t.Helper()

	doc, err := c.cluster.GetBucket("default").Store().Get(0, uint(c.vbID), 0, []byte(key))
	if err != nil {
		c.t.Fatalf("failed to read %s from the store: %v", key, err)
	}
	return doc.Expiry
}

func TestSubdocMutationSetsExpiry(t *testing.T) {
	c := newTestKvConn(t)

	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSet,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Value:   []byte(`{}`),
		Extras:  make([]byte, 8),
	})

	expiryOnly := make([]byte, 4)
	binary.BigEndian.PutUint32(expiryOnly, 10)
	expiryAndFlags := make([]byte, 5)
	binary.BigEndian.PutUint32(expiryAndFlags, 20)

	for _, test := range []struct {
		name   string
		extras []byte
		ttl    time.Duration
	}{
		{name: "expiry only", extras: expiryOnly, ttl: 10 * time.Second},
		{name: "expiry and flags", extras: expiryAndFlags, ttl: 20 * time.Second},
		{name: "no expiry", extras: nil, ttl: 0},
	} {
		resp := c.Do(&memd.Packet{
			Command: memd.CmdSubDocMultiMutation,
			Vbucket: c.vbID,
			Key:     []byte("doc"),
			Extras:  test.extras,
			Value:   encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Path: "tmppath", Value: "null"}),
		})
		if resp.Status != memd.StatusSuccess {
			t.Errorf("%s: failed with status 0x%02x", test.name, uint16(resp.Status))
			continue
		}

		expiry := c.expiry("doc")
		if test.ttl == 0 {
			if !expiry.IsZero() {
				t.Errorf("%s: expected no expiry, got %v", test.name, expiry)
			}
			continue
		}
		if ttl := time.Until(expiry); ttl <= test.ttl-time.Second || ttl > test.ttl {
			t.Errorf("%s: expected expiry in %v, got %v", test.name, test.ttl, ttl)
		}
	}
}

func TestFailedSubdocMutationKeepsExpiry(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)

	withExpiry := make([]byte, 8)
	binary.BigEndian.PutUint32(withExpiry[4:], 60)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: withExpiry})
	before := c.expiry("doc")

	// A virtual attribute cannot be modified, so the request fails, and a
	// request without an expiry must not clear the stored one.
	resp := c.Do(&memd.Packet{
		Command: memd.CmdSubDocMultiMutation,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Value: encodeMutationSpecs(
			subdocSpec{Op: memd.SubDocOpDictSet, Flags: memd.SubdocFlagXattrPath, Path: "$document.exptime", Value: "0"},
			subdocSpec{Op: memd.SubDocOpDictSet, Path: "b", Value: "2"},
		),
	})
	if resp.Status == memd.StatusSuccess {
		t.Fatalf("mutation of a virtual attribute succeeded")
	}

	if after := c.expiry("doc"); !after.Equal(before) {
		t.Errorf("expected expiry %v to be kept, got %v", before, after)
	}
	resp = c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")})
	if string(resp.Value) != `{"a":1}` {
		t.Errorf("failed mutation was stored: %s", resp.Value)
	}
}
