package mockimpl

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

// TestMaxTTLLimitsExpiry checks the expiry of stored documents against
// kv_engine 7.0.5's Collections::VB::Manifest::processExpiryTime, which
// KVBucket::set, add, replace and getAndUpdateTtl apply: the collection's
// maxTTL, or else the bucket's, bounds the expiry to now plus maxTTL, and
// gives a document stored without an expiry that one.
func TestMaxTTLLimitsExpiry(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureCollections, memd.FeatureAltRequests, memd.FeaturePreserveExpiry)
	bucket := c.cluster.GetBucket("default")
	manifest := bucket.CollectionManifest()
	if _, err := manifest.AddScope("app"); err != nil {
		t.Fatalf("failed to add scope: %v", err)
	}
	// kv_engine refuses the collection ids below 8, which this manifest
	// gives the first collections it adds.
	for i := 0; i < 7; i++ {
		if _, err := manifest.AddCollection("app", fmt.Sprintf("unused%d", i), 0); err != nil {
			t.Fatalf("failed to add collection: %v", err)
		}
	}
	for _, col := range []struct {
		name   string
		maxTTL uint32
	}{{"ttl10", 10}, {"ttl30", 30}, {"nottl", 0}} {
		if _, err := manifest.AddCollection("app", col.name, col.maxTTL); err != nil {
			t.Fatalf("failed to add collection: %v", err)
		}
	}
	collectionID := func(name string) uint32 {
		_, cid, err := manifest.GetByName("app", name)
		if err != nil {
			t.Fatalf("failed to find collection %s: %v", name, err)
		}
		return cid
	}
	setBucketMaxTTL := func(maxTTL uint32) {
		err := bucket.Update(mock.UpdateBucketOptions{MaxTTL: &maxTTL})
		if err != nil {
			t.Fatalf("failed to update bucket: %v", err)
		}
	}
	expiryExtras := func(size int, expiry uint32) []byte {
		extras := make([]byte, size)
		binary.BigEndian.PutUint32(extras[size-4:], expiry)
		return extras
	}
	counterExtras := func(expiry uint32) []byte {
		extras := make([]byte, 20)
		binary.BigEndian.PutUint64(extras, 1)
		binary.BigEndian.PutUint32(extras[16:], expiry)
		return extras
	}

	set := func(key, value string, expiry uint32) memd.Packet {
		return memd.Packet{Command: memd.CmdSet, Key: []byte(key), Value: []byte(value), Extras: expiryExtras(8, expiry)}
	}
	preserving := func(req memd.Packet) memd.Packet {
		req.PreserveExpiryFrame = &memd.PreserveExpiryFrame{}
		return req
	}
	tests := []struct {
		name       string
		bucketTTL  uint32
		collection string
		req        memd.Packet
		// ttl is the expected time to expiry; 0 means none.
		ttl time.Duration
	}{
		{"set without expiry", 0, "ttl10", set("k1", "{}", 0), 10 * time.Second},
		{"set below maxTTL", 0, "ttl10", set("k2", "1", 5), 5 * time.Second},
		{"set above maxTTL", 0, "ttl10", set("k3", "1", 100), 10 * time.Second},
		{"add without expiry", 0, "ttl10", memd.Packet{Command: memd.CmdAdd, Key: []byte("k4"), Value: []byte("1"), Extras: expiryExtras(8, 0)}, 10 * time.Second},
		{"replace without expiry", 0, "ttl10", memd.Packet{Command: memd.CmdReplace, Key: []byte("k4"), Value: []byte("2"), Extras: expiryExtras(8, 0)}, 10 * time.Second},
		{"touch without expiry", 0, "ttl10", memd.Packet{Command: memd.CmdTouch, Key: []byte("k2"), Extras: expiryExtras(4, 0)}, 10 * time.Second},
		{"get and touch below maxTTL", 0, "ttl10", memd.Packet{Command: memd.CmdGAT, Key: []byte("k3"), Extras: expiryExtras(4, 3)}, 3 * time.Second},
		{"increment creating", 0, "ttl10", memd.Packet{Command: memd.CmdIncrement, Key: []byte("c1"), Extras: counterExtras(0)}, 10 * time.Second},
		{"subdoc mutation without expiry", 0, "ttl10", memd.Packet{Command: memd.CmdSubDocMultiMutation, Key: []byte("k1"),
			Value: encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Path: "p", Value: "1"})}, 10 * time.Second},
		{"no maxTTL", 0, "nottl", set("n1", "1", 0), 0},
		{"no maxTTL counter", 0, "nottl", memd.Packet{Command: memd.CmdIncrement, Key: []byte("n2"), Extras: counterExtras(0)}, 0},
		{"no maxTTL json", 0, "nottl", set("p1", "{}", 0), 0},
		// kv_engine copies a preserved expiry back after bounding it.
		{"set preserving expiry", 20, "nottl", preserving(set("p1", "{}", 100)), 0},
		{"replace preserving expiry", 20, "nottl", preserving(memd.Packet{Command: memd.CmdReplace, Key: []byte("p1"), Value: []byte("{}"), Extras: expiryExtras(8, 0)}), 0},
		// The daemon copies it for a sub-document mutation, before the store
		// bounds it.
		{"subdoc mutation preserving expiry", 20, "nottl", preserving(memd.Packet{Command: memd.CmdSubDocMultiMutation, Key: []byte("p1"),
			Value: encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Path: "p", Value: "1"})}), 20 * time.Second},
		{"bucket maxTTL", 20, "nottl", set("b1", "1", 100), 20 * time.Second},
		{"bucket maxTTL on append", 20, "nottl", memd.Packet{Command: memd.CmdAppend, Key: []byte("n1"), Value: []byte("2")}, 20 * time.Second},
		{"bucket maxTTL on prepend", 20, "nottl", memd.Packet{Command: memd.CmdPrepend, Key: []byte("n1"), Value: []byte("2")}, 20 * time.Second},
		{"bucket maxTTL on counter", 20, "nottl", memd.Packet{Command: memd.CmdIncrement, Key: []byte("n2"), Extras: counterExtras(0)}, 20 * time.Second},
		{"bucket maxTTL in default collection", 20, "", set("d1", "1", 0), 20 * time.Second},
		{"collection maxTTL above bucket maxTTL", 20, "ttl30", set("t1", "1", 0), 30 * time.Second},
		{"collection maxTTL below bucket maxTTL", 20, "ttl10", set("t2", "1", 0), 10 * time.Second},
	}

	for _, tt := range tests {
		setBucketMaxTTL(tt.bucketTTL)
		req := tt.req
		req.Vbucket = c.vbID
		if tt.collection != "" {
			req.CollectionID = collectionID(tt.collection)
		}
		c.mustSucceed(&req)

		doc, err := bucket.Store().Get(0, uint(c.vbID), uint(req.CollectionID), req.Key)
		if err != nil {
			t.Fatalf("%s: failed to read %s: %v", tt.name, req.Key, err)
		}
		if tt.ttl == 0 {
			if !doc.Expiry.IsZero() {
				t.Errorf("%s: expected no expiry, got %v", tt.name, doc.Expiry)
			}
			continue
		}
		if ttl := time.Until(doc.Expiry); ttl <= tt.ttl-time.Second || ttl > tt.ttl {
			t.Errorf("%s: expected expiry in %v, got %v", tt.name, tt.ttl, ttl)
		}
	}
}
