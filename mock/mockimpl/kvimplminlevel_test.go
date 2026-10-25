package mockimpl

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

// TestDurabilityMinLevelRaisesWrites checks a bucket with a
// durabilityMinLevel against Couchbase Server 7.0.5: on a bucket whose
// replicas cannot form a majority, every write kv_engine stores or removes
// becomes a durable write and is refused as impossible, where the
// document checks allow it to reach the store; TOUCH is not raised.
func TestDurabilityMinLevelRaisesWrites(t *testing.T) {
	c := newTestKvConn(t)
	bucket, err := c.cluster.AddBucket(mock.NewBucketOptions{Name: "wide", Type: mock.BucketTypeCouchbase, NumReplicas: 3})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("wide")})
	vbID := vbucketOwnedAs(t, c, bucket, 0)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: vbID, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)})

	settings := bucket.Settings()
	settings.DurabilityMinLevel = "majority"
	if err := bucket.Update(mock.UpdateBucketOptions{Settings: &settings}); err != nil {
		t.Fatalf("failed to update bucket: %v", err)
	}

	counterExtras := make([]byte, 20)
	binary.BigEndian.PutUint64(counterExtras, 1)
	for _, tt := range []struct {
		name   string
		req    memd.Packet
		status memd.StatusCode
	}{
		{"set", memd.Packet{Command: memd.CmdSet, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)}, memd.StatusDurabilityImpossible},
		{"add", memd.Packet{Command: memd.CmdAdd, Key: []byte("new"), Value: []byte(`{}`), Extras: make([]byte, 8)}, memd.StatusDurabilityImpossible},
		{"replace of a missing document", memd.Packet{Command: memd.CmdReplace, Key: []byte("missing"), Value: []byte(`{}`), Extras: make([]byte, 8)}, memd.StatusDurabilityImpossible},
		{"increment", memd.Packet{Command: memd.CmdIncrement, Key: []byte("counter"), Extras: counterExtras}, memd.StatusDurabilityImpossible},
		{"append", memd.Packet{Command: memd.CmdAppend, Key: []byte("doc"), Value: []byte(` `)}, memd.StatusDurabilityImpossible},
		{"subdoc mutation", memd.Packet{Command: memd.CmdSubDocMultiMutation, Key: []byte("doc"),
			Value: encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Path: "a", Value: "1"})}, memd.StatusDurabilityImpossible},
		{"delete", memd.Packet{Command: memd.CmdDelete, Key: []byte("doc")}, memd.StatusDurabilityImpossible},
		{"delete of a missing document", memd.Packet{Command: memd.CmdDelete, Key: []byte("missing")}, memd.StatusKeyNotFound},
		{"append to a missing document", memd.Packet{Command: memd.CmdAppend, Key: []byte("missing"), Value: []byte(` `)}, memd.StatusNotStored},
		{"touch", memd.Packet{Command: memd.CmdTouch, Key: []byte("doc"), Extras: make([]byte, 4)}, memd.StatusSuccess},
	} {
		req := tt.req
		req.Vbucket = vbID
		if resp := c.Do(&req); resp.Status != tt.status {
			t.Errorf("%s: expected status 0x%02x, got 0x%02x", tt.name, uint16(tt.status), uint16(resp.Status))
		}
	}
}

// TestDurabilityMinLevelWritesCommit checks that a write raised to the
// bucket's durabilityMinLevel on a bucket that can meet it is answered
// once it is durable: no sooner than the replica latency, the time its
// replica needs to receive it.
func TestDurabilityMinLevelWritesCommit(t *testing.T) {
	c := newTestKvConn(t)
	bucket := c.cluster.GetBucket("default")
	settings := bucket.Settings()
	settings.DurabilityMinLevel = "majority"
	if err := bucket.Update(mock.UpdateBucketOptions{Settings: &settings}); err != nil {
		t.Fatalf("failed to update bucket: %v", err)
	}

	set := func(key string) time.Duration {
		start := time.Now()
		c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte(key), Value: []byte(`{}`), Extras: make([]byte, 8)})
		return time.Since(start)
	}
	// The default cluster's replica latency.
	const replicaLatency = 50 * time.Millisecond
	if elapsed := set("raised"); elapsed < replicaLatency {
		t.Fatalf("expected the raised write to be answered after %v, got %v", replicaLatency, elapsed)
	}

}

// TestDurableDeleteChecksDocumentFirst checks a durable DELETE on a
// bucket whose replicas cannot form a majority, as Couchbase Server 7.0.5
// answers it: RemoveCommandContext reads the document, refuses it locked
// without a CAS and checks the CAS of an unlocked one before the engine
// checks the durability requirements, and only then the lock against a
// CAS given, whether the request carries the requirements or the bucket's
// durabilityMinLevel raises them.
func TestDurableDeleteChecksDocumentFirst(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)
	bucket, err := c.cluster.AddBucket(mock.NewBucketOptions{Name: "wide", Type: mock.BucketTypeCouchbase, NumReplicas: 3})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("wide")})
	vbID := vbucketOwnedAs(t, c, bucket, 0)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: vbID, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)})
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: vbID, Key: []byte("locked"), Value: []byte(`{}`), Extras: make([]byte, 8)})
	lockCas := c.mustSucceed(&memd.Packet{Command: memd.CmdGetLocked, Vbucket: vbID, Key: []byte("locked"), Extras: make([]byte, 4)}).Cas

	run := func(how string, frame *memd.DurabilityLevelFrame) {
		for _, tt := range []struct {
			key    string
			cas    uint64
			status memd.StatusCode
		}{
			{"missing", 0, memd.StatusKeyNotFound},
			{"missing", 1, memd.StatusKeyNotFound},
			{"locked", 0, memd.StatusLocked},
			{"locked", 1, memd.StatusDurabilityImpossible},
			{"locked", lockCas, memd.StatusDurabilityImpossible},
			{"doc", 1, memd.StatusKeyExists},
			{"doc", 0, memd.StatusDurabilityImpossible},
		} {
			resp := c.Do(&memd.Packet{
				Command:              memd.CmdDelete,
				Vbucket:              vbID,
				Key:                  []byte(tt.key),
				Cas:                  tt.cas,
				DurabilityLevelFrame: frame,
			})
			if resp.Status != tt.status {
				t.Errorf("%s: delete %s with CAS %d: expected status 0x%02x, got 0x%02x",
					how, tt.key, tt.cas, uint16(tt.status), uint16(resp.Status))
			}
		}
	}
	run("durability frame", &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority})

	settings := bucket.Settings()
	settings.DurabilityMinLevel = "majority"
	if err := bucket.Update(mock.UpdateBucketOptions{Settings: &settings}); err != nil {
		t.Fatalf("failed to update bucket: %v", err)
	}
	run("bucket minimum level", nil)

	// Without durability, the lock refuses a CAS other than the lock's.
	settings.DurabilityMinLevel = "none"
	if err := bucket.Update(mock.UpdateBucketOptions{Settings: &settings}); err != nil {
		t.Fatalf("failed to update bucket: %v", err)
	}
	resp := c.Do(&memd.Packet{Command: memd.CmdDelete, Vbucket: vbID, Key: []byte("locked"), Cas: 1})
	if resp.Status != memd.StatusLocked {
		t.Errorf("delete locked with CAS 1: expected status 0x%02x, got 0x%02x", uint16(memd.StatusLocked), uint16(resp.Status))
	}
}
