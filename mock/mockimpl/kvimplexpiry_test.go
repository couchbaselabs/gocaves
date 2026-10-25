package mockimpl

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
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

func TestPreserveExpiry(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeaturePreserveExpiry)

	withExpiry := make([]byte, 8)
	binary.BigEndian.PutUint32(withExpiry[4:], 60)
	counterExtras := make([]byte, 20)
	binary.BigEndian.PutUint64(counterExtras[0:], 1)

	for _, test := range []struct {
		name     string
		doc      string
		req      *memd.Packet
		preserve bool
	}{
		{name: "set", doc: `{}`, req: &memd.Packet{Command: memd.CmdSet, Value: []byte(`{}`), Extras: make([]byte, 8)}, preserve: true},
		{name: "replace", doc: `{}`, req: &memd.Packet{Command: memd.CmdReplace, Value: []byte(`{}`), Extras: make([]byte, 8)}, preserve: true},
		{name: "increment", doc: `1`, req: &memd.Packet{Command: memd.CmdIncrement, Extras: counterExtras}, preserve: true},
		// Arithmetic keeps the expiry of an existing counter with or without
		// the frame.
		{name: "increment without frame", doc: `1`, req: &memd.Packet{Command: memd.CmdIncrement, Extras: counterExtras}},
		{
			name: "subdoc",
			doc:  `{}`,
			req: &memd.Packet{
				Command: memd.CmdSubDocMultiMutation,
				Value:   encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Path: "a", Value: "1"}),
			},
			preserve: true,
		},
	} {
		c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(test.doc), Extras: withExpiry})
		before := c.expiry("doc")

		test.req.Vbucket = c.vbID
		test.req.Key = []byte("doc")
		if test.preserve {
			test.req.PreserveExpiryFrame = &memd.PreserveExpiryFrame{}
		}
		if resp := c.Do(test.req); resp.Status != memd.StatusSuccess {
			t.Errorf("%s: failed with status 0x%02x", test.name, uint16(resp.Status))
			continue
		}

		if after := c.expiry("doc"); !after.Equal(before) {
			t.Errorf("%s: expected expiry %v to be kept, got %v", test.name, before, after)
		}
	}
}

func TestPreserveExpiryFrameValidation(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeaturePreserveExpiry)

	resp := c.Do(&memd.Packet{
		Command:             memd.CmdAdd,
		Vbucket:             c.vbID,
		Key:                 []byte("doc"),
		Value:               []byte(`{}`),
		Extras:              make([]byte, 8),
		PreserveExpiryFrame: &memd.PreserveExpiryFrame{},
	})
	if resp.Status != memd.StatusNotSupported {
		t.Errorf("ADD: expected status 0x%02x, got 0x%02x", uint16(memd.StatusNotSupported), uint16(resp.Status))
	}

	// Commands outside the CRUD handlers are validated too.
	resp = c.Do(&memd.Packet{Command: memd.CmdNoop, PreserveExpiryFrame: &memd.PreserveExpiryFrame{}})
	if resp.Status != memd.StatusNotSupported {
		t.Errorf("NOOP: expected status 0x%02x, got 0x%02x", uint16(memd.StatusNotSupported), uint16(resp.Status))
	}
}

func TestPreserveExpiryOnExpiredDocument(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeaturePreserveExpiry)

	withExpiry := make([]byte, 8)
	binary.BigEndian.PutUint32(withExpiry[4:], 10)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{}`), Extras: withExpiry})
	c.cluster.Chrono().TimeTravel(20 * time.Second)

	// Upserting the expired document creates a new one, which has no
	// expiry to keep.
	resp := c.Do(&memd.Packet{
		Command:             memd.CmdSubDocMultiMutation,
		Vbucket:             c.vbID,
		Key:                 []byte("doc"),
		Extras:              []byte{uint8(memd.SubdocDocFlagMkDoc)},
		Value:               encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Path: "a", Value: "1"}),
		PreserveExpiryFrame: &memd.PreserveExpiryFrame{},
	})
	if resp.Status != memd.StatusSuccess {
		t.Fatalf("upsert failed with status 0x%02x", uint16(resp.Status))
	}

	c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")})
}

// TestPreserveExpirySetOnExpiredOrDeleted checks a SET that preserves the
// expiry against Couchbase Server 7.0.5. VBucket::updateStoredValue copies
// the expiry of the document kv_engine holds in memory: an expired document
// keeps its past expiry and stays expired, and so does the deletion its
// expiry stored when a read found it expired, until that deletion is
// persisted. GET_META and REPLACE do not store the deletion. A persisted
// deletion, as a new key, takes the request's expiry.
func TestPreserveExpirySetOnExpiredOrDeleted(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeaturePreserveExpiry)
	expiryExtras := func(expiry uint32) []byte {
		extras := make([]byte, 8)
		binary.BigEndian.PutUint32(extras[4:], expiry)
		return extras
	}
	request := func(cmd memd.CmdCode, key string, extras []byte) *memd.Packet {
		req := &memd.Packet{Command: cmd, Vbucket: c.vbID, Key: []byte(key), Extras: extras}
		if cmd != memd.CmdGet {
			req.Value = []byte(`{}`)
		}
		return req
	}
	set := func(key string, expiry uint32, preserve bool) {
		req := request(memd.CmdSet, key, expiryExtras(expiry))
		if preserve {
			req.PreserveExpiryFrame = &memd.PreserveExpiryFrame{}
		}
		c.mustSucceed(req)
	}
	expectExpired := func(key string) {
		t.Helper()
		if resp := c.Do(request(memd.CmdGet, key, nil)); resp.Status != memd.StatusKeyNotFound {
			t.Errorf("%s: expected the document to stay expired, got status 0x%02x", key, uint16(resp.Status))
		}
	}
	expectRequestExpiry := func(key string) {
		t.Helper()
		if ttl := c.expiry(key).Sub(c.cluster.Chrono().Now()); ttl <= 99*time.Second || ttl > 100*time.Second {
			t.Errorf("%s: expected the request's expiry in 100s, got %v", key, ttl)
		}
	}

	reads := []struct {
		key    string
		req    *memd.Packet
		stores bool
	}{
		{"get", request(memd.CmdGet, "get", nil), true},
		{"gat", &memd.Packet{Command: memd.CmdGAT, Vbucket: c.vbID, Key: []byte("gat"), Extras: make([]byte, 4)}, true},
		{"delete", &memd.Packet{Command: memd.CmdDelete, Vbucket: c.vbID, Key: []byte("delete")}, true},
		{"get_meta", &memd.Packet{Command: memd.CmdGetMeta, Vbucket: c.vbID, Key: []byte("get_meta")}, false},
		{"replace", request(memd.CmdReplace, "replace", make([]byte, 8)), false},
	}
	set("unread", 10, false)
	set("read now", 10, false)
	for _, read := range reads {
		set(read.key, 10, false)
	}
	set("deleted", 0, false)
	c.mustSucceed(&memd.Packet{Command: memd.CmdDelete, Vbucket: c.vbID, Key: []byte("deleted")})
	c.cluster.Chrono().TimeTravel(20 * time.Second)

	set("unread", 100, true)
	expectExpired("unread")

	c.Do(request(memd.CmdGet, "read now", nil))
	set("read now", 100, true)
	expectExpired("read now")

	for _, read := range reads {
		c.Do(read.req)
	}
	c.cluster.Chrono().TimeTravel(time.Second)
	for _, read := range reads {
		set(read.key, 100, true)
		if read.stores {
			expectRequestExpiry(read.key)
		} else {
			expectExpired(read.key)
		}
	}

	set("deleted", 100, true)
	expectRequestExpiry("deleted")

	set("new", 100, true)
	expectRequestExpiry("new")
}

// TestPreserveExpirySetOnHeldDeletion checks a SET that preserves the
// expiry over a deletion against Couchbase Server 7.0.5: kv_engine holds
// the deletion in memory with the expiry the document had, until a
// couchbase bucket persists it and for as long as an ephemeral bucket
// keeps it, and the SET copies that expiry. A persisted deletion reads
// back with the time it was made as its expiry, and a read that brings it
// back into memory holds it so.
func TestPreserveExpirySetOnHeldDeletion(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeaturePreserveExpiry, memd.FeatureXattr)
	eph, err := c.cluster.AddBucket(mock.NewBucketOptions{Name: "eph", Type: mock.BucketTypeEphemeral})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	set := func(vbID uint16, key string, expiry uint32, preserve bool) {
		extras := make([]byte, 8)
		binary.BigEndian.PutUint32(extras[4:], expiry)
		req := &memd.Packet{Command: memd.CmdSet, Vbucket: vbID, Key: []byte(key), Value: []byte(`{}`), Extras: extras}
		if preserve {
			req.PreserveExpiryFrame = &memd.PreserveExpiryFrame{}
		}
		c.mustSucceed(req)
	}
	setAndDelete := func(vbID uint16, key string, expiry uint32) {
		set(vbID, key, expiry, false)
		c.mustSucceed(&memd.Packet{Command: memd.CmdDelete, Vbucket: vbID, Key: []byte(key)})
	}
	ttl := func(bucket, key string, vbID uint16) time.Duration {
		t.Helper()
		doc, err := c.cluster.GetBucket(bucket).Store().Get(0, uint(vbID), 0, []byte(key))
		if err != nil {
			t.Fatalf("failed to read %s: %v", key, err)
		}
		if doc.Expiry.IsZero() {
			return 0
		}
		return doc.Expiry.Sub(c.cluster.Chrono().Now()).Round(time.Second)
	}

	setAndDelete(c.vbID, "held", 40)
	setAndDelete(c.vbID, "held without expiry", 0)
	if got := ttl("default", "held", c.vbID); got != 40*time.Second {
		t.Errorf("held: expected the deletion to keep the expiry in 40s, got %v", got)
	}
	set(c.vbID, "held", 100, true)
	set(c.vbID, "held without expiry", 100, true)
	if got := ttl("default", "held", c.vbID); got != 40*time.Second {
		t.Errorf("held: expected the expiry in 40s, got %v", got)
	}
	if got := ttl("default", "held without expiry", c.vbID); got != 0 {
		t.Errorf("held without expiry: expected no expiry, got %v", got)
	}

	setAndDelete(c.vbID, "persisted", 40)
	c.cluster.Chrono().TimeTravel(time.Second)
	if got := ttl("default", "persisted", c.vbID); got != -time.Second {
		t.Errorf("persisted: expected the deletion to read back with its time, got %v", got)
	}
	set(c.vbID, "persisted", 100, true)
	if got := ttl("default", "persisted", c.vbID); got != 100*time.Second {
		t.Errorf("persisted: expected the request's expiry in 100s, got %v", got)
	}

	// A read that may see a deleted document, and GET_META, bring a
	// persisted deletion back into memory with the time it was made as its
	// expiry, so a preserving SET leaves the document expired; a GET does
	// not.
	lookupDeleted := &memd.Packet{Command: memd.CmdSubDocMultiLookup, Key: []byte("fetched by lookup"),
		Extras: []byte{uint8(memd.SubdocDocFlagAccessDeleted)}, Value: []byte{uint8(memd.SubDocOpGet),
			uint8(memd.SubdocFlagXattrPath), 0, byte(len("$document.exptime"))}}
	lookupDeleted.Value = append(lookupDeleted.Value, "$document.exptime"...)
	for key, read := range map[string]*memd.Packet{
		"fetched by lookup":   lookupDeleted,
		"fetched by GET_META": {Command: memd.CmdGetMeta, Key: []byte("fetched by GET_META"), Extras: []byte{2}},
		"read by GET":         {Command: memd.CmdGet, Key: []byte("read by GET")},
	} {
		setAndDelete(c.vbID, key, 40)
		c.cluster.Chrono().TimeTravel(time.Second)
		read.Vbucket = c.vbID
		c.Do(read)
		set(c.vbID, key, 100, true)
		resp := c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte(key)})
		if key == "read by GET" {
			if resp.Status != memd.StatusSuccess {
				t.Errorf("%s: expected the document with the request's expiry, got status 0x%02x", key, uint16(resp.Status))
			}
		} else if resp.Status != memd.StatusKeyNotFound {
			t.Errorf("%s: expected the document to stay expired, got status 0x%02x", key, uint16(resp.Status))
		}
	}

	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("eph")})
	vbID := vbucketOwnedAs(t, c, eph, 0)
	setAndDelete(vbID, "ephemeral", 40)
	c.cluster.Chrono().TimeTravel(time.Second)
	set(vbID, "ephemeral", 100, true)
	if got := ttl("eph", "ephemeral", vbID); got != 39*time.Second {
		t.Errorf("ephemeral: expected the expiry the document had, in 39s, got %v", got)
	}
}
