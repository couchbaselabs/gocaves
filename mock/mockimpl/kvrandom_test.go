package mockimpl

import (
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockdb"
)

// TestGetRandomCountsStoredDocuments checks that GET_RANDOM looks for a
// document only in a vbucket whose item count for the collection is not 0
// (KVBucket::getRandomKey): an EP bucket counts a live document once it has
// persisted it, an ephemeral bucket as it stores it. A deleted document is
// never returned (HashTable::getRandomKeyFromSlot).
func TestGetRandomCountsStoredDocuments(t *testing.T) {
	c := newTestKvConn(t)
	random := func(what string, status memd.StatusCode) {
		t.Helper()
		if resp := c.Do(&memd.Packet{Command: memd.CmdGetRandom}); resp.Status != status {
			t.Errorf("%s: expected status 0x%02x, got 0x%02x", what, uint16(status), uint16(resp.Status))
		}
	}

	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)})
	random("before persistence", memd.StatusKeyNotFound)
	c.cluster.Chrono().TimeTravel(time.Second)
	random("after persistence", memd.StatusSuccess)
	c.mustSucceed(&memd.Packet{Command: memd.CmdDelete, Vbucket: c.vbID, Key: []byte("doc")})
	random("after the deletion", memd.StatusKeyNotFound)

	bucket, err := c.cluster.AddBucket(mock.NewBucketOptions{Name: "ephemeral", Type: mock.BucketTypeEphemeral})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("ephemeral")})
	vbID := vbucketOwnedAs(t, c, bucket, 0)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: vbID, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)})
	random("ephemeral", memd.StatusSuccess)
}

// TestGetRandomLooksInActiveVbuckets checks that GET_RANDOM looks only in the
// vbuckets active on the node that serves it (KVBucket::getRandomKey).
func TestGetRandomLooksInActiveVbuckets(t *testing.T) {
	c := newTestKvConn(t)
	bucket := c.cluster.GetBucket("default")
	replicaVb := vbucketOwnedAs(t, c, bucket, 1)
	_, err := bucket.Store().Insert(&mockdb.Document{VbID: uint(replicaVb), Key: []byte("doc"), Value: []byte(`{}`),
		Cas: mockdb.GenerateNewCas(c.cluster.Chrono().Now())})
	if err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}
	c.cluster.Chrono().TimeTravel(time.Second)

	if resp := c.Do(&memd.Packet{Command: memd.CmdGetRandom}); resp.Status != memd.StatusKeyNotFound {
		t.Errorf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusKeyNotFound), uint16(resp.Status))
	}
}
