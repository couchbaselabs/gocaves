package mockimpl

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
	"github.com/couchbaselabs/gocaves/mock/mockdb"
)

func TestDurableWriteWaitsForItsLevel(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureSeqNo, memd.FeatureAltRequests, memd.FeatureSyncReplication)
	vbucket := c.cluster.GetBucket("default").Store().GetVbucket(uint(c.vbID))

	for _, test := range []struct {
		name  string
		level memd.DurabilityLevel
		req   *memd.Packet
		// check reports whether the copies the level names hold seqNo.
		check func(seqNo uint64) bool
	}{
		{
			name:  "majority",
			level: memd.DurabilityLevelMajority,
			req:   &memd.Packet{Command: memd.CmdSet, Key: []byte("majority"), Value: []byte(`{}`), Extras: make([]byte, 8)},
			check: func(seqNo uint64) bool {
				return vbucket.CurrentMetaState(1).CurrentSeqNo >= seqNo
			},
		},
		{
			name:  "majority and persist to active",
			level: memd.DurabilityLevelMajorityAndPersistOnMaster,
			req:   &memd.Packet{Command: memd.CmdSet, Key: []byte("persist-active"), Value: []byte(`{}`), Extras: make([]byte, 8)},
			check: func(seqNo uint64) bool {
				return vbucket.CurrentMetaState(1).CurrentSeqNo >= seqNo && vbucket.CurrentMetaState(0).PersistSeqNo >= seqNo
			},
		},
		{
			name:  "persist to majority",
			level: memd.DurabilityLevelPersistToMajority,
			req: &memd.Packet{
				Command: memd.CmdSubDocMultiMutation,
				Key:     []byte("persist-majority"),
				Extras:  []byte{uint8(memd.SubdocDocFlagMkDoc)},
				Value:   encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Path: "a", Value: "1"}),
			},
			check: func(seqNo uint64) bool {
				return vbucket.CurrentMetaState(1).PersistSeqNo >= seqNo
			},
		},
	} {
		test.req.Vbucket = c.vbID
		test.req.DurabilityLevelFrame = &memd.DurabilityLevelFrame{DurabilityLevel: test.level}
		resp := c.mustSucceed(test.req)

		seqNo := binary.BigEndian.Uint64(resp.Extras[8:])
		if !test.check(seqNo) {
			t.Errorf("%s: response arrived before the mutation reached the copies the level names", test.name)
		}
	}
}

// kv_engine's frame validator accepts durability on TOUCH and GAT, and its
// engine then refuses it.
func TestDurableTouchIsNotSupported(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)
	vbucket := c.cluster.GetBucket("default").Store().GetVbucket(uint(c.vbID))

	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)})
	seqNo := vbucket.CurrentMetaState(0).CurrentSeqNo

	for _, cmd := range []memd.CmdCode{memd.CmdTouch, memd.CmdGAT} {
		extras := make([]byte, 4)
		binary.BigEndian.PutUint32(extras, 3600)
		resp := c.Do(&memd.Packet{
			Command:              cmd,
			Vbucket:              c.vbID,
			Key:                  []byte("doc"),
			Extras:               extras,
			DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
		})
		if resp.Status != memd.StatusNotSupported {
			t.Errorf("%s: expected status 0x%02x, got 0x%02x", cmd.Name(), uint16(memd.StatusNotSupported), uint16(resp.Status))
		}
	}

	if got := vbucket.CurrentMetaState(0).CurrentSeqNo; got != seqNo {
		t.Errorf("expected the refused touches not to mutate the document, sequence number moved from %d to %d", seqNo, got)
	}
}

func TestDurableWriteTimesOutAsAmbiguous(t *testing.T) {
	c := newSlowReplicationConn(t)

	// A replica receives the write a second after the active copy, far
	// beyond the timeout.
	resp := c.Do(&memd.Packet{
		Command:                memd.CmdSet,
		Vbucket:                c.vbID,
		Key:                    []byte("doc"),
		Value:                  []byte(`{}`),
		Extras:                 make([]byte, 8),
		DurabilityLevelFrame:   &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
		DurabilityTimeoutFrame: &memd.DurabilityTimeoutFrame{DurabilityTimeout: 10 * time.Millisecond},
	})
	if resp.Status != memd.StatusSyncWriteAmbiguous {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusSyncWriteAmbiguous), uint16(resp.Status))
	}
}

// The replica holds the write before its timeout, and both fall between two
// polls of the wait.
func TestDurableWriteReplicatedBeforeTimeoutBetweenPolls(t *testing.T) {
	c := newReplicationConn(t, 3*time.Millisecond, 0)

	resp := c.Do(&memd.Packet{
		Command:                memd.CmdSet,
		Vbucket:                c.vbID,
		Key:                    []byte("doc"),
		Value:                  []byte(`{}`),
		Extras:                 make([]byte, 8),
		DurabilityLevelFrame:   &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
		DurabilityTimeoutFrame: &memd.DurabilityTimeoutFrame{DurabilityTimeout: 6 * time.Millisecond},
	})
	if resp.Status != memd.StatusSuccess {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusSuccess), uint16(resp.Status))
	}
}

// pipelineDurableSet sends a durable SET of key to the connection's vbucket
// without reading its response.
func pipelineDurableSet(t *testing.T, c *testKvConn, opaque uint32, key string, level memd.DurabilityLevel, timeout time.Duration) {
	t.Helper()

	err := c.conn.WritePacket(&memd.Packet{
		Magic:                  memd.CmdMagicReq,
		Command:                memd.CmdSet,
		Opaque:                 opaque,
		Vbucket:                c.vbID,
		Key:                    []byte(key),
		Value:                  []byte(`{}`),
		Extras:                 make([]byte, 8),
		DurabilityLevelFrame:   &memd.DurabilityLevelFrame{DurabilityLevel: level},
		DurabilityTimeoutFrame: &memd.DurabilityTimeoutFrame{DurabilityTimeout: timeout},
	})
	if err != nil {
		t.Fatalf("failed to write SET of %s: %v", key, err)
	}
}

type timedResponse struct {
	opaque  uint32
	status  memd.StatusCode
	elapsed time.Duration
}

// readTimedResponses reads n responses, in arrival order, with the time each
// arrived after start.
func readTimedResponses(t *testing.T, c *testKvConn, start time.Time, n int) []timedResponse {
	t.Helper()

	if err := c.netConn.SetDeadline(time.Now().Add(testKvTimeout)); err != nil {
		t.Fatalf("failed to set deadline: %v", err)
	}
	var responses []timedResponse
	for len(responses) < n {
		resp, _, err := c.conn.ReadPacket()
		if err != nil {
			t.Fatalf("failed to read response: %v", err)
		}
		responses = append(responses, timedResponse{opaque: resp.Opaque, status: resp.Status, elapsed: time.Since(start)})
	}
	return responses
}

// kv_engine's durability monitor does not acknowledge a write past an earlier
// one on its vbucket that a copy must persist first, and commits the writes of
// a vbucket in sequence-number order.
func TestDurableWriteWaitsBehindPersistLevelWrite(t *testing.T) {
	const replicaLatency = 50 * time.Millisecond
	const persistLatency = 100 * time.Millisecond

	for _, tc := range []struct {
		name  string
		level memd.DurabilityLevel
		// fenceLifted is when the first write no longer holds the second
		// back: once the active, or the replica, has persisted it.
		fenceLifted time.Duration
	}{
		{"majority_and_persist_to_active", memd.DurabilityLevelMajorityAndPersistOnMaster, persistLatency},
		{"persist_to_majority", memd.DurabilityLevelPersistToMajority, replicaLatency + persistLatency},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newReplicationConn(t, replicaLatency, persistLatency, memd.FeatureUnorderedExec)

			start := time.Now()
			pipelineDurableSet(t, c, 1, "first", tc.level, 10*time.Second)
			pipelineDurableSet(t, c, 2, "second", memd.DurabilityLevelMajority, 10*time.Second)
			responses := readTimedResponses(t, c, start, 2)

			for _, r := range responses {
				if r.status != memd.StatusSuccess {
					t.Errorf("opaque %d failed with status 0x%02x", r.opaque, uint16(r.status))
				}
			}
			if responses[0].opaque != 1 || responses[1].opaque != 2 {
				t.Errorf("expected responses in sequence-number order [1 2], got [%d %d]", responses[0].opaque, responses[1].opaque)
			}
			for _, r := range responses {
				if r.opaque == 2 && r.elapsed < tc.fenceLifted {
					t.Errorf("the second write was answered after %v, before the first stopped fencing it at %v", r.elapsed, tc.fenceLifted)
				}
			}
		})
	}
}

// kv_engine commits the writes of a vbucket in sequence-number order, so a
// connection receives their responses in that order even when the later write
// is ready to be answered first.
func TestDurableResponsesFollowSequenceNumberOrder(t *testing.T) {
	c := newReplicationConn(t, 50*time.Millisecond, 0, memd.FeatureUnorderedExec)

	// Both writes reach a replica at once; the first one's response is held
	// back on its way out, so only ordering can put it first.
	c.cluster.KvOutHooks().Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		if pak.Opaque == 1 {
			time.Sleep(200 * time.Millisecond)
		}
		next()
	})

	start := time.Now()
	pipelineDurableSet(t, c, 1, "first", memd.DurabilityLevelMajority, 10*time.Second)
	pipelineDurableSet(t, c, 2, "second", memd.DurabilityLevelMajority, 10*time.Second)
	responses := readTimedResponses(t, c, start, 2)

	for _, r := range responses {
		if r.status != memd.StatusSuccess {
			t.Errorf("opaque %d failed with status 0x%02x", r.opaque, uint16(r.status))
		}
	}
	if responses[0].opaque != 1 || responses[1].opaque != 2 {
		t.Errorf("expected responses in sequence-number order [1 2], got [%d %d]", responses[0].opaque, responses[1].opaque)
	}
}

// kv_engine starts no further request on a connection while
// max_concurrent_commands_per_connection, 32, are in flight.
func TestConnectionRunsAtMostThirtyTwoCommands(t *testing.T) {
	// The replica never receives a write, so each durable write stays in
	// flight until its timeout.
	c := newReplicationConn(t, time.Hour, 0, memd.FeatureUnorderedExec)

	const stalled = 32
	for i := 1; i <= stalled; i++ {
		pipelineDurableSet(t, c, uint32(i), fmt.Sprintf("doc%d", i), memd.DurabilityLevelMajority, 300*time.Millisecond)
	}
	// A GET may run out of order under unordered execution, so only the limit
	// holds it back.
	err := c.conn.WritePacket(&memd.Packet{Magic: memd.CmdMagicReq, Command: memd.CmdGet, Opaque: 100, Vbucket: c.vbID, Key: []byte("doc1")})
	if err != nil {
		t.Fatalf("failed to write GET: %v", err)
	}

	responses := readTimedResponses(t, c, time.Now(), stalled+1)
	if responses[0].opaque == 100 {
		t.Errorf("the GET was answered first, while %d durable writes were in flight", stalled)
	}
	for _, r := range responses {
		if r.opaque != 100 && r.status != memd.StatusSyncWriteAmbiguous {
			t.Errorf("opaque %d: expected status 0x%02x, got 0x%02x", r.opaque, uint16(memd.StatusSyncWriteAmbiguous), uint16(r.status))
		}
	}
}

// A write that times out is aborted and no longer holds later writes back.
func TestTimedOutDurableWriteReleasesLaterWrite(t *testing.T) {
	const replicaLatency = 50 * time.Millisecond
	// The active persists far later than the later write is answered.
	const persistLatency = 10 * time.Second

	c := newReplicationConn(t, replicaLatency, persistLatency, memd.FeatureUnorderedExec)

	start := time.Now()
	pipelineDurableSet(t, c, 1, "first", memd.DurabilityLevelMajorityAndPersistOnMaster, 20*time.Millisecond)
	pipelineDurableSet(t, c, 2, "second", memd.DurabilityLevelMajority, 5*time.Second)
	responses := readTimedResponses(t, c, start, 2)

	statuses := map[uint32]memd.StatusCode{}
	for _, r := range responses {
		statuses[r.opaque] = r.status
		if r.opaque == 2 && r.elapsed >= persistLatency {
			t.Errorf("the second write was answered after %v, only once the first was persisted", r.elapsed)
		}
	}
	if statuses[1] != memd.StatusSyncWriteAmbiguous {
		t.Errorf("expected the first write to time out with status 0x%02x, got 0x%02x", uint16(memd.StatusSyncWriteAmbiguous), uint16(statuses[1]))
	}
	if statuses[2] != memd.StatusSuccess {
		t.Errorf("expected the second write to succeed, got status 0x%02x", uint16(statuses[2]))
	}
}

// kv_engine aborts an expired write only once every earlier write on its
// vbucket has completed, so a write past its timeout behind a pending one
// commits with it.
func TestExpiredDurableWriteWaitsForEarlierWrite(t *testing.T) {
	const replicaLatency = 50 * time.Millisecond
	const persistLatency = 200 * time.Millisecond

	c := newReplicationConn(t, replicaLatency, persistLatency, memd.FeatureUnorderedExec)

	start := time.Now()
	pipelineDurableSet(t, c, 1, "first", memd.DurabilityLevelPersistToMajority, 10*time.Second)
	pipelineDurableSet(t, c, 2, "second", memd.DurabilityLevelMajority, 20*time.Millisecond)
	responses := readTimedResponses(t, c, start, 2)

	for _, r := range responses {
		if r.status != memd.StatusSuccess {
			t.Errorf("opaque %d failed with status 0x%02x", r.opaque, uint16(r.status))
		}
	}
	if responses[0].opaque != 1 || responses[1].opaque != 2 {
		t.Errorf("expected responses in sequence-number order [1 2], got [%d %d]", responses[0].opaque, responses[1].opaque)
	}
}

// Writes aborted together are answered in sequence-number order.
func TestAbortedDurableResponsesFollowSequenceNumberOrder(t *testing.T) {
	// The replica never receives a write.
	c := newReplicationConn(t, time.Hour, 0, memd.FeatureUnorderedExec)

	// The first response is held back on its way out, so only ordering can
	// put it first.
	c.cluster.KvOutHooks().Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		if pak.Opaque == 1 {
			time.Sleep(200 * time.Millisecond)
		}
		next()
	})

	start := time.Now()
	pipelineDurableSet(t, c, 1, "first", memd.DurabilityLevelMajority, 50*time.Millisecond)
	pipelineDurableSet(t, c, 2, "second", memd.DurabilityLevelMajority, 20*time.Millisecond)
	responses := readTimedResponses(t, c, start, 2)

	for _, r := range responses {
		if r.status != memd.StatusSyncWriteAmbiguous {
			t.Errorf("opaque %d: expected status 0x%02x, got 0x%02x", r.opaque, uint16(memd.StatusSyncWriteAmbiguous), uint16(r.status))
		}
	}
	if responses[0].opaque != 1 || responses[1].opaque != 2 {
		t.Errorf("expected responses in sequence-number order [1 2], got [%d %d]", responses[0].opaque, responses[1].opaque)
	}
}

// kv_engine checks durability when arithmetic, APPEND, PREPEND and
// sub-document mutations store, after reading and checking the document, so
// a failure found there is answered instead of durability_impossible.
func TestDurabilityCheckedWhenMutationStores(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)

	// Only one of the three replicas has a node, and a majority needs two.
	bucket, err := c.cluster.AddBucket(mock.NewBucketOptions{Name: "wide", Type: mock.BucketTypeCouchbase, NumReplicas: 3})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("wide")})
	vbID := vbucketOwnedAs(t, c, bucket, 0)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: vbID, Key: []byte("text"), Value: []byte(`"text"`), Extras: make([]byte, 8)})

	noInitial := make([]byte, 20)
	binary.BigEndian.PutUint32(noInitial[16:], 0xffffffff)
	requests := map[string]*memd.Packet{
		"increment without initial value": {Command: memd.CmdIncrement, Key: []byte("missing"), Extras: noInitial},
		"append to missing document":      {Command: memd.CmdAppend, Key: []byte("missing"), Value: []byte("x")},
		"sub-document path mismatch": {
			Command: memd.CmdSubDocMultiMutation,
			Key:     []byte("text"),
			Value:   encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Path: "a.b", Value: "1"}),
		},
	}
	for name, req := range requests {
		t.Run(name, func(t *testing.T) {
			req.Vbucket = vbID
			plain := *req
			want := c.Do(&plain).Status
			if want == memd.StatusSuccess {
				t.Fatalf("expected the request to fail without durability")
			}

			durable := *req
			durable.DurabilityLevelFrame = &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority}
			if got := c.Do(&durable).Status; got != want {
				t.Errorf("expected status 0x%02x, got 0x%02x", uint16(want), uint16(got))
			}
		})
	}
}

// kv_engine's memcached bucket engine refused any durability requirement with
// not_supported when the write reached it.
func TestDurableWriteToMemcachedBucketIsNotSupported(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)

	bucket, err := c.cluster.AddBucket(mock.NewBucketOptions{Name: "cache", Type: mock.BucketTypeMemcached})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("cache")})
	vbID := vbucketOwnedAs(t, c, bucket, 0)

	resp := c.Do(&memd.Packet{
		Command:              memd.CmdSet,
		Vbucket:              vbID,
		Key:                  []byte("doc"),
		Value:                []byte(`{}`),
		Extras:               make([]byte, 8),
		DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
	})
	if resp.Status != memd.StatusNotSupported {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusNotSupported), uint16(resp.Status))
	}
	resp = c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: vbID, Key: []byte("doc")})
	if resp.Status != memd.StatusKeyNotFound {
		t.Errorf("refused write was stored: GET returned status 0x%02x", uint16(resp.Status))
	}
}

// On a locked document kv_engine stores an APPEND that carries a CAS, and an
// INCREMENT that carries none, and checks durability before the lock.
func TestDurabilityCheckedBeforeLockWhereKvEngineStores(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)

	// Only one of the three replicas has a node, and a majority needs two.
	bucket, err := c.cluster.AddBucket(mock.NewBucketOptions{Name: "wide", Type: mock.BucketTypeCouchbase, NumReplicas: 3})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("wide")})
	vbID := vbucketOwnedAs(t, c, bucket, 0)

	lockValue := func(key, value string) {
		c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: vbID, Key: []byte(key), Value: []byte(value), Extras: make([]byte, 8)})
		lockTime := make([]byte, 4)
		binary.BigEndian.PutUint32(lockTime, 30)
		c.mustSucceed(&memd.Packet{Command: memd.CmdGetLocked, Vbucket: vbID, Key: []byte(key), Extras: lockTime})
	}
	lock := func(key string) { lockValue(key, "1") }
	durable := &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority}

	lock("appended")
	resp := c.Do(&memd.Packet{
		Command: memd.CmdAppend, Vbucket: vbID, Key: []byte("appended"), Value: []byte("2"),
		Cas: 12345, DurabilityLevelFrame: durable,
	})
	if resp.Status != memd.StatusDurabilityImpossible {
		t.Errorf("APPEND with a CAS: expected status 0x%02x, got 0x%02x", uint16(memd.StatusDurabilityImpossible), uint16(resp.Status))
	}

	lock("counted")
	resp = c.Do(&memd.Packet{
		Command: memd.CmdIncrement, Vbucket: vbID, Key: []byte("counted"), Extras: make([]byte, 20),
		DurabilityLevelFrame: durable,
	})
	if resp.Status != memd.StatusDurabilityImpossible {
		t.Errorf("INCREMENT without a CAS: expected status 0x%02x, got 0x%02x", uint16(memd.StatusDurabilityImpossible), uint16(resp.Status))
	}

	// kv_engine parses the value before it stores, so a value that is not a
	// number is refused before durability.
	lockValue("text", `"text"`)
	resp = c.Do(&memd.Packet{
		Command: memd.CmdIncrement, Vbucket: vbID, Key: []byte("text"), Extras: make([]byte, 20),
		DurabilityLevelFrame: durable,
	})
	if resp.Status != memd.StatusBadDelta {
		t.Errorf("INCREMENT of a value that is not a number: expected status 0x%02x, got 0x%02x", uint16(memd.StatusBadDelta), uint16(resp.Status))
	}
}

// kv_engine counts the replicas assigned to the vbucket's replication chain
// against sync_writes_max_allowed_replicas, not the configured ones.
func TestDurableWriteWithTwoAssignedOfThreeReplicas(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)

	if _, err := c.cluster.AddNode(mock.NewNodeOptions{}); err != nil {
		t.Fatalf("failed to add node: %v", err)
	}
	bucket, err := c.cluster.AddBucket(mock.NewBucketOptions{Name: "wide", Type: mock.BucketTypeCouchbase, NumReplicas: 3})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("wide")})
	vbID := vbucketOwnedAs(t, c, bucket, 0)

	c.mustSucceed(&memd.Packet{
		Command:              memd.CmdSet,
		Vbucket:              vbID,
		Key:                  []byte("doc"),
		Value:                []byte(`{}`),
		Extras:               make([]byte, 8),
		DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
	})
}

func TestDurableWriteWithoutEnoughReplicasIsImpossible(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)

	// The cluster has two nodes, so only the first of three replicas has a
	// node, and a majority needs two.
	bucket, err := c.cluster.AddBucket(mock.NewBucketOptions{
		Name:        "wide",
		Type:        mock.BucketTypeCouchbase,
		NumReplicas: 3,
	})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("wide")})

	vbID := -1
	for vb, replicaIdx := range bucket.VbucketOwnership(c.cluster.Nodes()[0]) {
		if replicaIdx == 0 {
			vbID = vb
			break
		}
	}
	if vbID < 0 {
		t.Fatalf("node does not own any vbucket of the bucket as master")
	}

	resp := c.Do(&memd.Packet{
		Command:              memd.CmdSet,
		Vbucket:              uint16(vbID),
		Key:                  []byte("doc"),
		Value:                []byte(`{}`),
		Extras:               make([]byte, 8),
		DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
	})
	if resp.Status != memd.StatusDurabilityImpossible {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusDurabilityImpossible), uint16(resp.Status))
	}

	resp = c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: uint16(vbID), Key: []byte("doc")})
	if resp.Status != memd.StatusKeyNotFound {
		t.Errorf("refused write was stored: GET returned status 0x%02x", uint16(resp.Status))
	}
}

// newSlowReplicationConn connects to a cluster laid out like
// NewDefaultCluster, whose replicas receive a mutation one second after the
// active copy. A durable write stays pending long enough for a test to act
// while it waits.
func newSlowReplicationConn(t *testing.T, features ...memd.HelloFeature) *testKvConn {
	return newReplicationConn(t, time.Second, 0, features...)
}

// newReplicationConn connects to a cluster laid out like NewDefaultCluster,
// whose replicas receive a mutation replicaLatency after the active copy, and
// whose copies persist it persistLatency after receiving it. A zero latency
// takes the cluster's default.
func newReplicationConn(t *testing.T, replicaLatency, persistLatency time.Duration, features ...memd.HelloFeature) *testKvConn {
	cluster, err := NewCluster(mock.NewClusterOptions{
		InitialNode:    mock.NewNodeOptions{},
		ReplicaLatency: replicaLatency,
		PersistLatency: persistLatency,
	})
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })

	if _, err := cluster.AddNode(mock.NewNodeOptions{}); err != nil {
		t.Fatalf("failed to add node: %v", err)
	}
	_, err = cluster.AddBucket(mock.NewBucketOptions{Name: "default", Type: mock.BucketTypeCouchbase, NumReplicas: 1})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	err = cluster.Users().UpsertUser(mockauth.UpsertUserOptions{Username: "Administrator", Password: "password", Roles: []string{"admin"}})
	if err != nil {
		t.Fatalf("failed to add user: %v", err)
	}

	return connectTestKv(t, cluster, append([]memd.HelloFeature{memd.FeatureAltRequests, memd.FeatureSyncReplication}, features...)...)
}

// waitForDocument waits until key is stored in the active copy of the
// connection's vbucket.
func waitForDocument(t *testing.T, c *testKvConn, key string) {
	t.Helper()

	store := c.cluster.GetBucket("default").Store()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := store.Get(0, uint(c.vbID), 0, []byte(key))
		if err == nil {
			return
		}
		if !errors.Is(err, mockdb.ErrDocNotFound) {
			t.Fatalf("failed to read %s: %v", key, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s was never stored", key)
		}
		time.Sleep(time.Millisecond)
	}
}

// discardPendingWrite waits until key is stored in the active copy of the
// connection's vbucket, then calls discard and writes fillers until the
// vbucket's sequence number passes the discarded write's.
func discardPendingWrite(c *testKvConn, key string, discard func(store *mockdb.Bucket) error) <-chan error {
	store := c.cluster.GetBucket("default").Store()
	done := make(chan error, 1)
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for {
			if _, err := store.Get(0, uint(c.vbID), 0, []byte(key)); err == nil {
				break
			} else if !errors.Is(err, mockdb.ErrDocNotFound) {
				done <- err
				return
			}
			if time.Now().After(deadline) {
				done <- fmt.Errorf("%s was never stored", key)
				return
			}
			time.Sleep(time.Millisecond)
		}

		if err := discard(store); err != nil {
			done <- err
			return
		}
		for i := 0; i < 5; i++ {
			_, err := store.Insert(&mockdb.Document{
				VbID:  uint(c.vbID),
				Key:   []byte(fmt.Sprintf("filler-%d", i)),
				Value: []byte(`{}`),
				Cas:   mockdb.GenerateNewCas(store.Chrono().Now()),
			})
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	return done
}

func durableSet(c *testKvConn, key string) *memd.Packet {
	return c.Do(&memd.Packet{
		Command:                memd.CmdSet,
		Vbucket:                c.vbID,
		Key:                    []byte(key),
		Value:                  []byte(`{}`),
		Extras:                 make([]byte, 8),
		DurabilityLevelFrame:   &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
		DurabilityTimeoutFrame: &memd.DurabilityTimeoutFrame{DurabilityTimeout: 5 * time.Second},
	})
}

func TestDurableWriteDiscardedByFlushIsAmbiguous(t *testing.T) {
	c := newSlowReplicationConn(t)

	done := discardPendingWrite(c, "doc", func(store *mockdb.Bucket) error {
		store.Flush()
		return nil
	})
	resp := durableSet(c, "doc")
	if err := <-done; err != nil {
		t.Fatalf("failed to discard the write: %v", err)
	}
	if resp.Status != memd.StatusSyncWriteAmbiguous {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusSyncWriteAmbiguous), uint16(resp.Status))
	}
}

// Writes a flush discards are answered in sequence-number order too.
func TestDiscardedDurableResponsesFollowSequenceNumberOrder(t *testing.T) {
	// The replica never receives a write.
	c := newReplicationConn(t, time.Hour, 0, memd.FeatureUnorderedExec)

	// The first response is held back on its way out, so only ordering can
	// put it first.
	c.cluster.KvOutHooks().Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		if pak.Opaque == 1 {
			time.Sleep(200 * time.Millisecond)
		}
		next()
	})

	start := time.Now()
	pipelineDurableSet(t, c, 1, "first", memd.DurabilityLevelMajority, 10*time.Second)
	pipelineDurableSet(t, c, 2, "second", memd.DurabilityLevelMajority, 10*time.Second)
	store := c.cluster.GetBucket("default").Store()
	for _, key := range []string{"first", "second"} {
		for {
			_, err := store.Get(0, uint(c.vbID), 0, []byte(key))
			if err == nil {
				break
			}
			if !errors.Is(err, mockdb.ErrDocNotFound) {
				t.Fatalf("failed to read %s: %v", key, err)
			}
			if time.Since(start) > testKvTimeout {
				t.Fatalf("%s was not stored", key)
			}
			time.Sleep(time.Millisecond)
		}
	}
	store.Flush()
	responses := readTimedResponses(t, c, start, 2)

	for _, r := range responses {
		if r.status != memd.StatusSyncWriteAmbiguous {
			t.Errorf("opaque %d: expected status 0x%02x, got 0x%02x", r.opaque, uint16(memd.StatusSyncWriteAmbiguous), uint16(r.status))
		}
	}
	if responses[0].opaque != 1 || responses[1].opaque != 2 {
		t.Errorf("expected responses in sequence-number order [1 2], got [%d %d]", responses[0].opaque, responses[1].opaque)
	}
}

func TestDurableWriteDiscardedBySecondRollbackIsAmbiguous(t *testing.T) {
	c := newSlowReplicationConn(t)
	store := c.cluster.GetBucket("default").Store()

	// The first rollback starts a new history; the second discards the
	// durable write made in it.
	if err := store.Rollback(store.Snapshot()); err != nil {
		t.Fatalf("failed to roll back: %v", err)
	}
	snap := store.Snapshot()

	done := discardPendingWrite(c, "doc", func(store *mockdb.Bucket) error {
		return store.Rollback(snap)
	})
	resp := durableSet(c, "doc")
	if err := <-done; err != nil {
		t.Fatalf("failed to discard the write: %v", err)
	}
	if resp.Status != memd.StatusSyncWriteAmbiguous {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusSyncWriteAmbiguous), uint16(resp.Status))
	}
}

// vbucketOwnedAs returns a vbucket of bucket that the connected node holds as
// replica index replicaIdx, where 0 is the active copy.
func vbucketOwnedAs(t *testing.T, c *testKvConn, bucket mock.Bucket, replicaIdx int) uint16 {
	t.Helper()

	for vb, idx := range bucket.VbucketOwnership(c.cluster.Nodes()[0]) {
		if idx == replicaIdx {
			return uint16(vb)
		}
	}
	t.Fatalf("node holds no vbucket of %s as replica index %d", bucket.Name(), replicaIdx)
	return 0
}

func TestDurableWriteToReplicaIsNotMyVbucket(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)

	// Durability cannot be met in this bucket, but a request for a vbucket the
	// node is not master of is a routing error first.
	bucket, err := c.cluster.AddBucket(mock.NewBucketOptions{Name: "wide", Type: mock.BucketTypeCouchbase, NumReplicas: 3})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("wide")})

	resp := c.Do(&memd.Packet{
		Command:              memd.CmdSet,
		Vbucket:              vbucketOwnedAs(t, c, bucket, 1),
		Key:                  []byte("doc"),
		Value:                []byte(`{}`),
		Extras:               make([]byte, 8),
		DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
	})
	if resp.Status != memd.StatusNotMyVBucket {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusNotMyVBucket), uint16(resp.Status))
	}
}

func TestEphemeralBucketRefusesPersistenceLevels(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)

	bucket, err := c.cluster.AddBucket(mock.NewBucketOptions{Name: "ephemeral", Type: mock.BucketTypeEphemeral, NumReplicas: 1})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("ephemeral")})
	vbID := vbucketOwnedAs(t, c, bucket, 0)

	for _, test := range []struct {
		level  memd.DurabilityLevel
		status memd.StatusCode
	}{
		{level: memd.DurabilityLevelMajority, status: memd.StatusSuccess},
		{level: memd.DurabilityLevelMajorityAndPersistOnMaster, status: memd.StatusDurabilityInvalidLevel},
		{level: memd.DurabilityLevelPersistToMajority, status: memd.StatusDurabilityInvalidLevel},
	} {
		resp := c.Do(&memd.Packet{
			Command:              memd.CmdSet,
			Vbucket:              vbID,
			Key:                  []byte("doc"),
			Value:                []byte(`{}`),
			Extras:               make([]byte, 8),
			DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: test.level},
		})
		if resp.Status != test.status {
			t.Errorf("level %d: expected status 0x%02x, got 0x%02x", test.level, uint16(test.status), uint16(resp.Status))
		}
	}

	// A vbucket the node is not master of is a routing error first.
	resp := c.Do(&memd.Packet{
		Command:              memd.CmdSet,
		Vbucket:              vbucketOwnedAs(t, c, bucket, 1),
		Key:                  []byte("doc"),
		Value:                []byte(`{}`),
		Extras:               make([]byte, 8),
		DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelPersistToMajority},
	})
	if resp.Status != memd.StatusNotMyVBucket {
		t.Errorf("replica vbucket: expected status 0x%02x, got 0x%02x", uint16(memd.StatusNotMyVBucket), uint16(resp.Status))
	}
}

func TestDurableWriteDoesNotBlockUnorderedConnection(t *testing.T) {
	c := newSlowReplicationConn(t, memd.FeatureUnorderedExec)

	// The durable write waits a second for its replica; the GET behind it is
	// answered first.
	for _, req := range []*memd.Packet{
		{
			Magic:                memd.CmdMagicReq,
			Command:              memd.CmdSet,
			Opaque:               1,
			Vbucket:              c.vbID,
			Key:                  []byte("doc"),
			Value:                []byte(`{}`),
			Extras:               make([]byte, 8),
			DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
		},
		{Magic: memd.CmdMagicReq, Command: memd.CmdGet, Opaque: 2, Vbucket: c.vbID, Key: []byte("missing")},
	} {
		if err := c.conn.WritePacket(req); err != nil {
			t.Fatalf("failed to write %s: %v", req.Command.Name(), err)
		}
	}
	if err := c.netConn.SetDeadline(time.Now().Add(testKvTimeout)); err != nil {
		t.Fatalf("failed to set deadline: %v", err)
	}

	var opaques []uint32
	for len(opaques) < 2 {
		resp, _, err := c.conn.ReadPacket()
		if err != nil {
			t.Fatalf("failed to read response: %v", err)
		}
		opaques = append(opaques, resp.Opaque)
		if resp.Opaque == 1 && resp.Status != memd.StatusSuccess {
			t.Errorf("durable SET failed with status 0x%02x", uint16(resp.Status))
		}
	}
	if opaques[0] != 2 {
		t.Errorf("expected the GET answered before the durable SET, got order %v", opaques)
	}
}

// A response written for a connection already lost, such as a detached
// durable reply, is dropped before the outgoing hooks see it: the packet
// recording of checks reads the connection's addresses and bucket from them.
func TestWriteToLostClientSkipsOutgoingHooks(t *testing.T) {
	c := newTestKvConn(t)

	client := make(chan mock.KvClient, 1)
	c.cluster.KvInHooks().Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		if pak.Command == memd.CmdNoop {
			client <- source
		}
		next()
	})
	var hooked atomic.Int32
	c.cluster.KvOutHooks().Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		if pak.Opaque == 99 {
			hooked.Add(1)
		}
		next()
	})

	c.mustSucceed(&memd.Packet{Command: memd.CmdNoop})
	source := <-client
	if err := c.netConn.Close(); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
	kvService := c.cluster.Nodes()[0].(*clusterNodeInst).kvService
	deadline := time.Now().Add(5 * time.Second)
	for len(kvService.GetAllClients()) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the server still holds the disconnected client")
		}
		time.Sleep(10 * time.Millisecond)
	}

	err := source.WritePacket(&memd.Packet{Magic: memd.CmdMagicRes, Command: memd.CmdSet, Opaque: 99})
	if !errors.Is(err, errKvClientLost) {
		t.Errorf("expected %v, got %v", errKvClientLost, err)
	}
	if n := hooked.Load(); n != 0 {
		t.Errorf("the outgoing hooks saw %d packets for the lost connection", n)
	}
}

// writeDurableSet sends a durable SET with opaque 1 without waiting for its
// response.
func writeDurableSet(t *testing.T, c *testKvConn) {
	t.Helper()

	err := c.conn.WritePacket(&memd.Packet{
		Magic:                memd.CmdMagicReq,
		Command:              memd.CmdSet,
		Opaque:               1,
		Vbucket:              c.vbID,
		Key:                  []byte("doc"),
		Value:                []byte(`{}`),
		Extras:               make([]byte, 8),
		DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
	})
	if err != nil {
		t.Fatalf("failed to write SET: %v", err)
	}
}

// readOpaques reads n responses and returns their opaques in arrival order.
func readOpaques(t *testing.T, c *testKvConn, n int) []uint32 {
	t.Helper()

	if err := c.netConn.SetDeadline(time.Now().Add(testKvTimeout)); err != nil {
		t.Fatalf("failed to set deadline: %v", err)
	}
	var opaques []uint32
	for len(opaques) < n {
		resp, _, err := c.conn.ReadPacket()
		if err != nil {
			t.Fatalf("failed to read response: %v", err)
		}
		if resp.Status != memd.StatusSuccess {
			t.Errorf("%s (opaque %d) failed with status 0x%02x", resp.Command.Name(), resp.Opaque, uint16(resp.Status))
		}
		opaques = append(opaques, resp.Opaque)
	}
	return opaques
}

func TestBarrierWaitsForPendingDurableWrite(t *testing.T) {
	c := newSlowReplicationConn(t, memd.FeatureUnorderedExec)

	writeDurableSet(t, c)
	err := c.conn.WritePacket(&memd.Packet{Magic: memd.CmdMagicReq, Command: memd.CmdNoop, Opaque: 2, BarrierFrame: &memd.BarrierFrame{}})
	if err != nil {
		t.Fatalf("failed to write NOOP: %v", err)
	}

	if opaques := readOpaques(t, c, 2); opaques[0] != 1 {
		t.Errorf("expected the barrier NOOP answered after the durable SET, got order %v", opaques)
	}
}

// kv_engine refuses an invalid request as it reads it, so a request that would
// otherwise wait as a barrier is answered while a durable write is pending.
func TestInvalidRequestDoesNotWaitForPendingDurableWrite(t *testing.T) {
	for _, tc := range []struct {
		name   string
		pak    memd.Packet
		status memd.StatusCode
	}{
		{"unknown command", memd.Packet{Command: memd.CmdCode(0xee)}, memd.StatusUnknownCommand},
		{
			"STAT with a durability level",
			memd.Packet{Command: memd.CmdStat, DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority}},
			memd.StatusInvalidArgs,
		},
		{
			"barrier with an invalid level",
			memd.Packet{
				Command: memd.CmdSet, Key: []byte("other"), Value: []byte(`{}`), Extras: make([]byte, 8),
				BarrierFrame: &memd.BarrierFrame{}, DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevel(4)},
			},
			memd.StatusDurabilityInvalidLevel,
		},
		{
			"barrier without extras",
			memd.Packet{Command: memd.CmdSet, Key: []byte("other"), Value: []byte(`{}`), BarrierFrame: &memd.BarrierFrame{}},
			memd.StatusInvalidArgs,
		},
		{
			"barrier for an unknown collection",
			memd.Packet{Command: memd.CmdSet, Key: []byte("other"), Value: []byte(`{}`), Extras: make([]byte, 8), CollectionID: 999, BarrierFrame: &memd.BarrierFrame{}},
			memd.StatusCollectionUnknown,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The replica never receives the durable write before it times out.
			c := newReplicationConn(t, time.Hour, 0, memd.FeatureUnorderedExec, memd.FeatureCollections)

			pipelineDurableSet(t, c, 1, "doc", memd.DurabilityLevelMajority, 500*time.Millisecond)
			pak := tc.pak
			pak.Magic = memd.CmdMagicReq
			pak.Opaque = 2
			pak.Vbucket = c.vbID
			if err := c.conn.WritePacket(&pak); err != nil {
				t.Fatalf("failed to write request: %v", err)
			}

			responses := readTimedResponses(t, c, time.Now(), 2)
			if responses[0].opaque != 2 {
				t.Fatalf("expected the invalid request answered before the durable write, got order [%d %d]", responses[0].opaque, responses[1].opaque)
			}
			if responses[0].status != tc.status {
				t.Errorf("expected status 0x%02x, got 0x%02x", uint16(tc.status), uint16(responses[0].status))
			}
		})
	}
}

func TestPendingDurableWriteDuringRenegotiation(t *testing.T) {
	c := newSlowReplicationConn(t, memd.FeatureUnorderedExec)

	c.cluster.KvOutHooks().Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		source.SelectedBucket()
		next()
	})

	// HELLO and SELECT_BUCKET change the state the durable SET's reply reads,
	// so kv_engine runs them only after that reply, as barriers.
	writeDurableSet(t, c)
	hello := make([]byte, 0, 6)
	for _, feature := range []memd.HelloFeature{memd.FeatureAltRequests, memd.FeatureSyncReplication, memd.FeatureUnorderedExec} {
		hello = binary.BigEndian.AppendUint16(hello, uint16(feature))
	}
	for _, req := range []*memd.Packet{
		{Magic: memd.CmdMagicReq, Command: memd.CmdHello, Opaque: 2, Key: []byte("renegotiate"), Value: hello},
		{Magic: memd.CmdMagicReq, Command: memd.CmdSelectBucket, Opaque: 3, Key: []byte("default")},
	} {
		if err := c.conn.WritePacket(req); err != nil {
			t.Fatalf("failed to write %s: %v", req.Command.Name(), err)
		}
	}

	if opaques := readOpaques(t, c, 3); opaques[0] != 1 {
		t.Errorf("expected the durable SET answered before HELLO and SELECT_BUCKET, got order %v", opaques)
	}
}

// newStalledReplicationCluster returns a cluster whose replicas never catch
// up, so only the durability timeout would end a durable write's wait, and a
// connection to it with the given features.
func newStalledReplicationCluster(t *testing.T, features ...memd.HelloFeature) (mock.Cluster, *testKvConn) {
	t.Helper()

	cluster, err := NewCluster(mock.NewClusterOptions{InitialNode: mock.NewNodeOptions{}, ReplicaLatency: time.Hour})
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	// Registered before the connection's cleanup, so the connection closes
	// before the cluster stops, and the cluster stops however setup ends.
	t.Cleanup(func() { closeTestCluster(t, cluster) })
	if _, err := cluster.AddNode(mock.NewNodeOptions{}); err != nil {
		t.Fatalf("failed to add node: %v", err)
	}
	if _, err := cluster.AddBucket(mock.NewBucketOptions{Name: "default", Type: mock.BucketTypeCouchbase, NumReplicas: 1}); err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	err = cluster.Users().UpsertUser(mockauth.UpsertUserOptions{Username: "Administrator", Password: "password", Roles: []string{"admin"}})
	if err != nil {
		t.Fatalf("failed to add user: %v", err)
	}
	return cluster, connectTestKv(t, cluster, append([]memd.HelloFeature{memd.FeatureAltRequests, memd.FeatureSyncReplication}, features...)...)
}

// writeStalledDurableSet sends a durable SET with a 10-second timeout and waits
// until it is stored, so its wait has begun.
func writeStalledDurableSet(t *testing.T, c *testKvConn) {
	t.Helper()

	err := c.conn.WritePacket(&memd.Packet{
		Magic:                  memd.CmdMagicReq,
		Command:                memd.CmdSet,
		Opaque:                 1,
		Vbucket:                c.vbID,
		Key:                    []byte("doc"),
		Value:                  []byte(`{}`),
		Extras:                 make([]byte, 8),
		DurabilityLevelFrame:   &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
		DurabilityTimeoutFrame: &memd.DurabilityTimeoutFrame{DurabilityTimeout: 10 * time.Second},
	})
	if err != nil {
		t.Fatalf("failed to write SET: %v", err)
	}
	waitForDocument(t, c, "doc")
}

func TestClosingNodeDoesNotWaitForDurableWrite(t *testing.T) {
	cluster, c := newStalledReplicationCluster(t)

	var dispatchedAfterClose atomic.Int32
	cluster.KvInHooks().Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		if source.Closed() {
			dispatchedAfterClose.Add(1)
		}
		next()
	})

	writeStalledDurableSet(t, c)
	err := c.conn.WritePacket(&memd.Packet{Magic: memd.CmdMagicReq, Command: memd.CmdSet, Opaque: 2, Vbucket: c.vbID, Key: []byte("queued"), Value: []byte(`{}`), Extras: make([]byte, 8)})
	if err != nil {
		t.Fatalf("failed to write queued SET: %v", err)
	}

	start := time.Now()
	closeTestCluster(t, cluster)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("closing the cluster waited %s for a pending durable write", elapsed)
	}

	// Closing the cluster waited for the dispatcher, which has handled the SET
	// queued behind the wait.
	if n := dispatchedAfterClose.Load(); n != 0 {
		t.Errorf("%d requests reached the handlers after the connection was closed", n)
	}
}

// storedOrDisconnected waits until key is stored, and reports false if the
// server disconnects the client first.
func storedOrDisconnected(t *testing.T, cluster mock.Cluster, c *testKvConn, key string) bool {
	t.Helper()

	kvService := cluster.Nodes()[0].(*clusterNodeInst).kvService
	store := c.cluster.GetBucket("default").Store()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := store.Get(0, uint(c.vbID), 0, []byte(key))
		if err == nil {
			return true
		}
		if !errors.Is(err, mockdb.ErrDocNotFound) {
			t.Fatalf("failed to read %s: %v", key, err)
		}
		if len(kvService.GetAllClients()) == 0 {
			return false
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", key)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestQueueOverflowDisconnectsClient(t *testing.T) {
	for _, test := range []struct {
		name     string
		features []memd.HelloFeature
		count    int
		req      func(opaque uint32) *memd.Packet
		// paced waits for each request to be stored before sending the
		// next, so the queue itself never holds more than one.
		paced bool
	}{
		{
			name:  "SETs",
			count: 100,
			req: func(opaque uint32) *memd.Packet {
				return &memd.Packet{Magic: memd.CmdMagicReq, Command: memd.CmdSet, Opaque: opaque, Key: []byte("queued"), Value: make([]byte, 1024), Extras: make([]byte, 8)}
			},
		},
		{
			// Requests without a body still count against the limit.
			name:  "NOOPs",
			count: 4000,
			req: func(opaque uint32) *memd.Packet {
				return &memd.Packet{Magic: memd.CmdMagicReq, Command: memd.CmdNoop, Opaque: opaque}
			},
		},
		{
			// Each is 25 bytes on the wire, one of them its barrier frame:
			// 2700 pass the limit only if the frame is charged.
			name:  "NOOPs with barrier frames",
			count: 2700,
			req: func(opaque uint32) *memd.Packet {
				return &memd.Packet{Magic: memd.CmdMagicReq, Command: memd.CmdNoop, Opaque: opaque, BarrierFrame: &memd.BarrierFrame{}}
			},
		},
		{
			// Up to kv_engine's limit of commands in flight each leaves the
			// queue at once and waits detached, still charged; the rest
			// wait queued.
			name:     "unordered durable SETs",
			features: []memd.HelloFeature{memd.FeatureUnorderedExec},
			count:    100,
			paced:    true,
			req: func(opaque uint32) *memd.Packet {
				return &memd.Packet{
					Magic:                memd.CmdMagicReq,
					Command:              memd.CmdSet,
					Opaque:               opaque,
					Key:                  []byte(fmt.Sprintf("durable-%d", opaque)),
					Value:                make([]byte, 1024),
					Extras:               make([]byte, 8),
					DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// The limit is copied into each connection's queue as it
			// registers, so it applies to this connection only, and is
			// restored however the setup ends.
			limit := maxOutstandingRequestBytes
			t.Cleanup(func() { maxOutstandingRequestBytes = limit })
			maxOutstandingRequestBytes = 64 * 1024
			cluster, c := newStalledReplicationCluster(t, test.features...)
			maxOutstandingRequestBytes = limit

			// The requests queue behind the stalled write; the client never
			// closes.
			writeStalledDurableSet(t, c)
			for i := 0; i < test.count; i++ {
				req := test.req(uint32(i + 2))
				req.Vbucket = c.vbID
				if err := c.conn.WritePacket(req); err != nil {
					// Expected once the server disconnects; the check below
					// decides.
					t.Logf("write %d failed: %v", i, err)
					break
				}
				// The stalled write holds one of the slots for requests in
				// flight.
				if test.paced && i < maxConcurrentCommands-1 && !storedOrDisconnected(t, cluster, c, string(req.Key)) {
					break
				}
			}

			kvService := cluster.Nodes()[0].(*clusterNodeInst).kvService
			deadline := time.Now().Add(5 * time.Second)
			for len(kvService.GetAllClients()) != 0 {
				if time.Now().After(deadline) {
					t.Fatalf("the server kept a client whose queued requests exceed the limit")
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

// queuedGets returns n GET requests with distinct opaques.
func queuedGets(n int) []*memd.Packet {
	reqs := make([]*memd.Packet, n)
	for i := range reqs {
		reqs[i] = &memd.Packet{Magic: memd.CmdMagicReq, Command: memd.CmdGet, Opaque: uint32(i + 2), Key: []byte("doc")}
	}
	return reqs
}

func TestPeerDisconnectEndsDurableWait(t *testing.T) {
	for _, test := range []struct {
		name     string
		features []memd.HelloFeature
		// queued is sent behind the durable write, and waits for it.
		queued []*memd.Packet
	}{
		{name: "ordered"},
		{
			name:   "ordered with a queued GET",
			queued: []*memd.Packet{{Magic: memd.CmdMagicReq, Command: memd.CmdGet, Opaque: 2, Key: []byte("doc")}},
		},
		{
			name:     "unordered with a queued barrier",
			features: []memd.HelloFeature{memd.FeatureUnorderedExec},
			queued:   []*memd.Packet{{Magic: memd.CmdMagicReq, Command: memd.CmdNoop, Opaque: 2, BarrierFrame: &memd.BarrierFrame{}}},
		},
		{
			// Dispatched after the connection is lost, the step would find
			// no SCRAM context.
			name:   "ordered with a queued SASL_STEP",
			queued: []*memd.Packet{{Magic: memd.CmdMagicReq, Command: memd.CmdSASLStep, Opaque: 2, Key: []byte("SCRAM-SHA512"), Value: []byte("c=biws,r=nonce,p=proof")}},
		},
		{name: "ordered with 2000 queued GETs", queued: queuedGets(2000)},
	} {
		t.Run(test.name, func(t *testing.T) {
			cluster, c := newStalledReplicationCluster(t, test.features...)

			// The hook keeps the client of the durable SET, and counts
			// requests that reach the handlers after the connection is lost.
			client := make(chan mock.KvClient, 1)
			var dispatchedAfterLoss atomic.Int32
			cluster.KvInHooks().Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
				if pak.Command == memd.CmdSet {
					client <- source
				}
				if source.Closed() {
					dispatchedAfterLoss.Add(1)
				}
				next()
			})

			writeStalledDurableSet(t, c)
			for _, req := range test.queued {
				req.Vbucket = c.vbID
				if err := c.conn.WritePacket(req); err != nil {
					t.Fatalf("failed to write %s: %v", req.Command.Name(), err)
				}
			}
			if err := c.netConn.Close(); err != nil {
				t.Fatalf("failed to close connection: %v", err)
			}

			// The server drops the client once its reader sees the close.
			kvService := cluster.Nodes()[0].(*clusterNodeInst).kvService
			deadline := time.Now().Add(5 * time.Second)
			for len(kvService.GetAllClients()) != 0 {
				if time.Now().After(deadline) {
					t.Fatalf("the server still holds the disconnected client during its durable write")
				}
				time.Sleep(10 * time.Millisecond)
			}

			// Closing the cluster waits for the dispatcher, so it has handled
			// every request queued behind the wait.
			closeTestCluster(t, cluster)
			if n := dispatchedAfterLoss.Load(); n != 0 {
				t.Errorf("%d requests reached the handlers after the connection was lost", n)
			}
			if (<-client).ScramServer() == nil {
				t.Errorf("the lost client has no SCRAM context")
			}
		})
	}
}

// kv_engine fails to parse a reserved timeout and answers
// durability_invalid_level, although its protocol documentation names
// invalid arguments.
func TestReservedDurabilityTimeoutIsRefused(t *testing.T) {
	for _, timeout := range []time.Duration{0, 0xffff * time.Millisecond} {
		t.Run(timeout.String(), func(t *testing.T) {
			c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)

			resp := c.Do(&memd.Packet{
				Command:                memd.CmdSet,
				Vbucket:                c.vbID,
				Key:                    []byte("doc"),
				Value:                  []byte(`{}`),
				Extras:                 make([]byte, 8),
				DurabilityLevelFrame:   &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
				DurabilityTimeoutFrame: &memd.DurabilityTimeoutFrame{DurabilityTimeout: timeout},
			})
			if resp.Status != memd.StatusDurabilityInvalidLevel {
				t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusDurabilityInvalidLevel), uint16(resp.Status))
			}
			if timeout == 0 {
				expectErrorContext(t, resp, "Timeout(): Cannot specify bucket default timeout")
			} else {
				expectErrorContext(t, resp, "Timeout(): Cannot specify an infinite timeout")
			}

			_, err := c.cluster.GetBucket("default").Store().Get(0, uint(c.vbID), 0, []byte("doc"))
			if !errors.Is(err, mockdb.ErrDocNotFound) {
				t.Errorf("expected the refused write not stored, got %v", err)
			}
		})
	}
}

// kv_engine constructs the timeout of a durability frame before it validates
// the level, so a frame with both invalid is refused for its timeout.
func TestInvalidTimeoutPrecedesInvalidLevel(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)

	// A durability frame (id 1) of 3 bytes: level None and timeout 0.
	writeRawRequest(t, c, memd.CmdSet, 1, []byte{0x13, 0x00, 0x00, 0x00}, make([]byte, 8), []byte("doc"), []byte(`{}`))
	resp := readResponse(t, c)
	if resp.Status != memd.StatusDurabilityInvalidLevel {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusDurabilityInvalidLevel), uint16(resp.Status))
	}
	expectErrorContext(t, resp, "Timeout(): Cannot specify bucket default timeout")
}

func TestUndefinedDurabilityLevelIsRefused(t *testing.T) {
	// Level 0 is kv_engine's None, which a request may not carry.
	for _, level := range []memd.DurabilityLevel{0, 4} {
		t.Run(fmt.Sprintf("level %d", level), func(t *testing.T) {
			c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)

			resp := c.Do(&memd.Packet{
				Command:              memd.CmdSet,
				Vbucket:              c.vbID,
				Key:                  []byte("doc"),
				Value:                []byte(`{}`),
				Extras:               make([]byte, 8),
				DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: level},
			})
			if resp.Status != memd.StatusDurabilityInvalidLevel {
				t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusDurabilityInvalidLevel), uint16(resp.Status))
			}

			_, err := c.cluster.GetBucket("default").Store().Get(0, uint(c.vbID), 0, []byte("doc"))
			if !errors.Is(err, mockdb.ErrDocNotFound) {
				t.Errorf("expected the refused write not stored, got %v", err)
			}
		})
	}
}

func TestDurabilityOnUnsupportedCommandIsInvalid(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)
	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSet,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Value:   []byte(`{}`),
		Extras:  make([]byte, 8),
	})

	resp := c.Do(&memd.Packet{
		Command:              memd.CmdGet,
		Vbucket:              c.vbID,
		Key:                  []byte("doc"),
		DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
	})
	if resp.Status != memd.StatusInvalidArgs {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusInvalidArgs), uint16(resp.Status))
	}
	expectErrorContext(t, resp, "The requested command does not support durability requirements")
}

// expectErrorContext checks that resp carries the error context body kv_engine
// sends with a refusal.
func expectErrorContext(t *testing.T, resp *memd.Packet, want string) {
	t.Helper()

	if resp.Datatype&uint8(memd.DatatypeFlagJSON) == 0 {
		t.Errorf("expected a JSON error body, got datatype 0x%02x", resp.Datatype)
	}
	var body struct {
		Error struct {
			Context string `json:"context"`
		} `json:"error"`
	}
	if err := json.Unmarshal(resp.Value, &body); err != nil {
		t.Fatalf("failed to decode error body %q: %v", resp.Value, err)
	}
	if body.Error.Context != want {
		t.Errorf("expected error context %q, got %q", want, body.Error.Context)
	}
}

// writeRawRequest sends an alternative-encoding request with the frames given
// as raw bytes, for frames memd cannot encode.
func writeRawRequest(t *testing.T, c *testKvConn, cmd memd.CmdCode, opaque uint32, frames, extras, key, value []byte) {
	t.Helper()

	header := make([]byte, 24)
	header[0] = 0x08 // the alternative request magic, which carries frames
	header[1] = byte(cmd)
	header[2] = byte(len(frames))
	header[3] = byte(len(key))
	header[4] = byte(len(extras))
	binary.BigEndian.PutUint16(header[6:], c.vbID)
	binary.BigEndian.PutUint32(header[8:], uint32(len(frames)+len(extras)+len(key)+len(value)))
	binary.BigEndian.PutUint32(header[12:], opaque)
	packet := append(header, frames...)
	packet = append(packet, extras...)
	packet = append(packet, key...)
	packet = append(packet, value...)
	if _, err := c.netConn.Write(packet); err != nil {
		t.Fatalf("failed to write request: %v", err)
	}
}

// readResponse reads the next response on c.
func readResponse(t *testing.T, c *testKvConn) *memd.Packet {
	t.Helper()

	if err := c.netConn.SetDeadline(time.Now().Add(testKvTimeout)); err != nil {
		t.Fatalf("failed to set deadline: %v", err)
	}
	resp, _, err := c.conn.ReadPacket()
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	return resp
}

// memd does not decode these frames; kv_engine refuses them with invalid
// arguments.
func TestMalformedFramesAreInvalid(t *testing.T) {
	for _, tc := range []struct {
		name    string
		frames  []byte
		context string
	}{
		// A durability frame (id 1) of 2 bytes: level and half a timeout.
		{"durability frame of 2 bytes", []byte{0x12, 0x01, 0x00}, "Invalid sized buffer provided: 2"},
		// A barrier frame (id 0) that carries a byte.
		{"barrier frame with data", []byte{0x01, 0x00}, "Barrier should not contain value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)

			writeRawRequest(t, c, memd.CmdSet, 1, tc.frames, make([]byte, 8), []byte("doc"), []byte(`{}`))
			resp := readResponse(t, c)
			if resp.Status != memd.StatusInvalidArgs {
				t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusInvalidArgs), uint16(resp.Status))
			}
			expectErrorContext(t, resp, tc.context)

			_, err := c.cluster.GetBucket("default").Store().Get(0, uint(c.vbID), 0, []byte("doc"))
			if !errors.Is(err, mockdb.ErrDocNotFound) {
				t.Errorf("expected the refused write not stored, got %v", err)
			}
		})
	}
}

// kv_engine answers an unknown command, and checks the request header, before
// it parses the frames.
func TestHeaderChecksPrecedeFrameChecks(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)
	invalidLevel := &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevel(4)}

	resp := c.Do(&memd.Packet{Command: memd.CmdCode(0xee), Vbucket: c.vbID, DurabilityLevelFrame: invalidLevel})
	if resp.Status != memd.StatusUnknownCommand {
		t.Errorf("unknown command: expected status 0x%02x, got 0x%02x", uint16(memd.StatusUnknownCommand), uint16(resp.Status))
	}

	resp = c.Do(&memd.Packet{
		Command:              memd.CmdSet,
		Vbucket:              c.vbID,
		Key:                  []byte("doc"),
		Value:                []byte(`{}`),
		DurabilityLevelFrame: invalidLevel,
	})
	if resp.Status != memd.StatusInvalidArgs {
		t.Fatalf("SET without extras: expected status 0x%02x, got 0x%02x", uint16(memd.StatusInvalidArgs), uint16(resp.Status))
	}
	expectErrorContext(t, resp, "Request must include extras of length 8")
}

// kv_engine's verify_header checks the key, value, CAS and datatype of a
// request before it parses the frames.
func TestHeaderFieldsCheckedBeforeFrames(t *testing.T) {
	invalidLevel := &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevel(4)}
	for _, tc := range []struct {
		name    string
		pak     memd.Packet
		context string
	}{
		{"ADD with a CAS", memd.Packet{Command: memd.CmdAdd, Key: []byte("doc"), Extras: make([]byte, 8), Cas: 1}, "Request CAS must not be set"},
		{"INCREMENT with a CAS", memd.Packet{Command: memd.CmdIncrement, Key: []byte("doc"), Extras: make([]byte, 20), Cas: 1}, "Request CAS must not be set"},
		{"DELETE with a value", memd.Packet{Command: memd.CmdDelete, Key: []byte("doc"), Value: []byte("1")}, "Request must not include value"},
		{"GAT with a value", memd.Packet{Command: memd.CmdGAT, Key: []byte("doc"), Extras: make([]byte, 4), Value: []byte("1")}, "Request must not include value"},
		{"SET without a key", memd.Packet{Command: memd.CmdSet, Extras: make([]byte, 8)}, "Request must include key"},
		{"SET with xattrs", memd.Packet{Command: memd.CmdSet, Key: []byte("doc"), Extras: make([]byte, 8), Datatype: uint8(memd.DatatypeFlagXattrs)}, "Request datatype invalid"},
		// An undefined datatype is refused before the extras are checked.
		{"SET with an undefined datatype", memd.Packet{Command: memd.CmdSet, Key: []byte("doc"), Datatype: 0x08}, "Request datatype invalid"},
		{"DELETE with JSON", memd.Packet{Command: memd.CmdDelete, Key: []byte("doc"), Datatype: uint8(memd.DatatypeFlagJSON)}, "Request datatype invalid"},
		{"SUBDOC_MULTI_MUTATION without a key", memd.Packet{Command: memd.CmdSubDocMultiMutation}, "Request must include key"},
		{"SUBDOC_MULTI_MUTATION with JSON", memd.Packet{Command: memd.CmdSubDocMultiMutation, Key: []byte("doc"), Datatype: uint8(memd.DatatypeFlagJSON)}, "Request datatype invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)

			pak := tc.pak
			pak.Vbucket = c.vbID
			pak.DurabilityLevelFrame = invalidLevel
			resp := c.Do(&pak)
			if resp.Status != memd.StatusInvalidArgs {
				t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusInvalidArgs), uint16(resp.Status))
			}
			expectErrorContext(t, resp, tc.context)
		})
	}
}

// kv_engine checks the extras of a sub-document mutation after the frames.
func TestSubdocExtrasCheckedAfterFrames(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)

	resp := c.Do(&memd.Packet{
		Command:              memd.CmdSubDocMultiMutation,
		Vbucket:              c.vbID,
		Key:                  []byte("doc"),
		Extras:               make([]byte, 2),
		DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevel(4)},
	})
	if resp.Status != memd.StatusDurabilityInvalidLevel {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusDurabilityInvalidLevel), uint16(resp.Status))
	}
}

// A key holds the collection ID on a connection that negotiated collections,
// so an empty document key does not fail the header check.
func TestEmptyKeyWithCollectionsPassesHeaderCheck(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication, memd.FeatureCollections)

	resp := c.Do(&memd.Packet{
		Command:              memd.CmdSet,
		Vbucket:              c.vbID,
		Extras:               make([]byte, 8),
		DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevel(4)},
	})
	if resp.Status != memd.StatusDurabilityInvalidLevel {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusDurabilityInvalidLevel), uint16(resp.Status))
	}
}

// kv_engine resolves the collection while validating the request, and checks
// whether durability is possible only once the engine runs the write.
func TestUnknownCollectionPrecedesImpossibleDurability(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication, memd.FeatureCollections)

	bucket, err := c.cluster.AddBucket(mock.NewBucketOptions{Name: "wide", Type: mock.BucketTypeCouchbase, NumReplicas: 3})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("wide")})
	vbID := vbucketOwnedAs(t, c, bucket, 0)

	resp := c.Do(&memd.Packet{
		Command:              memd.CmdSet,
		Vbucket:              vbID,
		CollectionID:         0x99,
		Key:                  []byte("doc"),
		Value:                []byte(`{}`),
		Extras:               make([]byte, 8),
		DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
	})
	if resp.Status != memd.StatusCollectionUnknown {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusCollectionUnknown), uint16(resp.Status))
	}
}

func TestPipelinedHelloAppliesToNextRequest(t *testing.T) {
	c := newTestKvConn(t)

	// The client encodes the SET with collections, which only the HELLO in
	// front of it enables on the server.
	c.conn.EnableFeature(memd.FeatureCollections)
	hello := binary.BigEndian.AppendUint16(nil, uint16(memd.FeatureCollections))
	for _, req := range []*memd.Packet{
		{Magic: memd.CmdMagicReq, Command: memd.CmdHello, Opaque: 1, Key: []byte("pipelined"), Value: hello},
		{Magic: memd.CmdMagicReq, Command: memd.CmdSet, Opaque: 2, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)},
	} {
		if err := c.conn.WritePacket(req); err != nil {
			t.Fatalf("failed to write %s: %v", req.Command.Name(), err)
		}
	}
	readOpaques(t, c, 2)

	_, err := c.cluster.GetBucket("default").Store().Get(0, uint(c.vbID), 0, []byte("doc"))
	if err != nil {
		t.Errorf("expected doc in the default collection: %v", err)
	}
}

func TestCloseWaitsForRunningHandler(t *testing.T) {
	cluster, c := newStalledReplicationCluster(t)

	entered := make(chan struct{})
	release := make(chan struct{})
	cluster.KvInHooks().Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		if pak.Command == memd.CmdGet {
			close(entered)
			<-release
		}
		next()
	})

	if err := c.conn.WritePacket(&memd.Packet{Magic: memd.CmdMagicReq, Command: memd.CmdGet, Opaque: 1, Vbucket: c.vbID, Key: []byte("doc")}); err != nil {
		t.Fatalf("failed to write GET: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(testKvTimeout):
		t.Fatalf("the GET never reached its handler")
	}

	// The close reports on a buffered channel, so it never touches t after
	// the test has ended.
	closed := make(chan error, 1)
	go func() {
		closed <- closeClusterServices(cluster)
	}()
	select {
	case err := <-closed:
		close(release)
		t.Fatalf("closing the cluster returned while a handler was running (error: %v)", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-closed:
		if err != nil {
			t.Errorf("%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("closing the cluster did not return after the handler did")
	}
}

func TestDeletingBucketEndsDurableWait(t *testing.T) {
	cluster, c := newStalledReplicationCluster(t)
	t.Cleanup(func() { closeTestCluster(t, cluster) })

	writeStalledDurableSet(t, c)
	if err := cluster.DeleteBucket("default"); err != nil {
		t.Fatalf("failed to delete bucket: %v", err)
	}

	// The write's timeout is 10 seconds; the deletion ends it sooner.
	if err := c.netConn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("failed to set deadline: %v", err)
	}
	resp, _, err := c.conn.ReadPacket()
	if err != nil {
		t.Fatalf("failed to read the durable write's response: %v", err)
	}
	if resp.Status != memd.StatusSyncWriteAmbiguous {
		t.Errorf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusSyncWriteAmbiguous), uint16(resp.Status))
	}
}

func TestDurableWriteWithThreeReplicasIsImpossible(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)

	// Four nodes hold every replica of the bucket, but kv_engine allows sync
	// writes on at most two replicas.
	for i := 0; i < 2; i++ {
		if _, err := c.cluster.AddNode(mock.NewNodeOptions{}); err != nil {
			t.Fatalf("failed to add node: %v", err)
		}
	}
	bucket, err := c.cluster.AddBucket(mock.NewBucketOptions{Name: "wide", Type: mock.BucketTypeCouchbase, NumReplicas: 3})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("wide")})
	vbID := vbucketOwnedAs(t, c, bucket, 0)

	resp := c.Do(&memd.Packet{
		Command:              memd.CmdSet,
		Vbucket:              vbID,
		Key:                  []byte("doc"),
		Value:                []byte(`{}`),
		Extras:               make([]byte, 8),
		DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
	})
	if resp.Status != memd.StatusDurabilityImpossible {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusDurabilityImpossible), uint16(resp.Status))
	}
	resp = c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: vbID, Key: []byte("doc")})
	if resp.Status != memd.StatusKeyNotFound {
		t.Errorf("refused write was stored: GET returned status 0x%02x", uint16(resp.Status))
	}
}

func TestCloseAfterPeerHalfCloseWithPendingResponse(t *testing.T) {
	cluster, c := newStalledReplicationCluster(t)

	// One response is larger than the socket buffers on both sides.
	tcpConn, ok := c.netConn.(*net.TCPConn)
	if !ok {
		t.Fatalf("expected a TCP connection, got %T", c.netConn)
	}
	if err := tcpConn.SetReadBuffer(4096); err != nil {
		t.Fatalf("failed to shrink the receive buffer: %v", err)
	}
	value := make([]byte, 16*1024*1024)
	if _, err := rand.Read(value); err != nil {
		t.Fatalf("failed to fill value: %v", err)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("large"), Value: value, Extras: make([]byte, 8)})

	// The client reads none of the responses, so they fill the socket
	// buffers and the dispatcher blocks writing one.
	for i := 0; i < 2; i++ {
		req := &memd.Packet{Magic: memd.CmdMagicReq, Command: memd.CmdGet, Opaque: uint32(i + 10), Vbucket: c.vbID, Key: []byte("large")}
		if err := c.conn.WritePacket(req); err != nil {
			t.Fatalf("failed to write GET: %v", err)
		}
	}
	// The first response has started: the dispatcher is writing a body the
	// client stops reading.
	if err := c.netConn.SetReadDeadline(time.Now().Add(testKvTimeout)); err != nil {
		t.Fatalf("failed to set deadline: %v", err)
	}
	if _, err := io.ReadFull(c.netConn, make([]byte, 24)); err != nil {
		t.Fatalf("failed to read a response header: %v", err)
	}
	if err := tcpConn.CloseWrite(); err != nil {
		t.Fatalf("failed to half-close the connection: %v", err)
	}

	// The server drops the client on EOF, so shutdown no longer closes its
	// socket, while the response write is still blocked.
	kvService := cluster.Nodes()[0].(*clusterNodeInst).kvService
	deadline := time.Now().Add(5 * time.Second)
	for len(kvService.GetAllClients()) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the server did not drop the half-closed client")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The close reports on a buffered channel, so it never touches t after
	// the test has ended.
	closed := make(chan error, 1)
	go func() {
		closed <- closeClusterServices(cluster)
	}()
	select {
	case err := <-closed:
		if err != nil {
			t.Errorf("%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("closing the cluster hung on a response to a half-closed peer")
	}
}

// On an ordered connection a request waits for every earlier one, a pending
// durable write included, so responses arrive in request order.
func TestOrderedConnectionAnswersInRequestOrder(t *testing.T) {
	c := newSlowReplicationConn(t)

	writeDurableSet(t, c)
	if err := c.conn.WritePacket(&memd.Packet{Magic: memd.CmdMagicReq, Command: memd.CmdGet, Opaque: 2, Vbucket: c.vbID, Key: []byte("doc")}); err != nil {
		t.Fatalf("failed to write GET: %v", err)
	}
	if opaques := readOpaques(t, c, 2); opaques[0] != 1 {
		t.Errorf("expected the GET answered after the durable SET, got order %v", opaques)
	}
}

// Under unordered execution a durable write that carries a barrier frame holds
// every later request until it completes.
func TestDurableBarrierHoldsLaterRequests(t *testing.T) {
	c := newSlowReplicationConn(t, memd.FeatureUnorderedExec)

	err := c.conn.WritePacket(&memd.Packet{
		Magic:                memd.CmdMagicReq,
		Command:              memd.CmdSet,
		Opaque:               1,
		Vbucket:              c.vbID,
		Key:                  []byte("doc"),
		Value:                []byte(`{}`),
		Extras:               make([]byte, 8),
		DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
		BarrierFrame:         &memd.BarrierFrame{},
	})
	if err != nil {
		t.Fatalf("failed to write SET: %v", err)
	}
	if err := c.conn.WritePacket(&memd.Packet{Magic: memd.CmdMagicReq, Command: memd.CmdGet, Opaque: 2, Vbucket: c.vbID, Key: []byte("doc")}); err != nil {
		t.Fatalf("failed to write GET: %v", err)
	}
	if opaques := readOpaques(t, c, 2); opaques[0] != 1 {
		t.Errorf("expected the GET held behind the durable barrier SET, got order %v", opaques)
	}
}

// Every durable mutation is answered only once the majority holds it.
func TestDurableMutationsWaitForMajority(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureSeqNo, memd.FeatureAltRequests, memd.FeatureSyncReplication)
	vbucket := c.cluster.GetBucket("default").Store().GetVbucket(uint(c.vbID))

	incr := make([]byte, 20)
	binary.BigEndian.PutUint64(incr[0:], 1)
	for _, req := range []*memd.Packet{
		{Command: memd.CmdAdd, Key: []byte("added"), Value: []byte(`{}`), Extras: make([]byte, 8)},
		{Command: memd.CmdReplace, Key: []byte("added"), Value: []byte(`{}`), Extras: make([]byte, 8)},
		{Command: memd.CmdAppend, Key: []byte("added"), Value: []byte(` `)},
		{Command: memd.CmdPrepend, Key: []byte("added"), Value: []byte(` `)},
		{Command: memd.CmdIncrement, Key: []byte("counter"), Extras: incr},
		{Command: memd.CmdDecrement, Key: []byte("counter"), Extras: incr},
		{Command: memd.CmdDelete, Key: []byte("added")},
	} {
		req.Vbucket = c.vbID
		req.DurabilityLevelFrame = &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority}
		resp := c.mustSucceed(req)
		seqNo := binary.BigEndian.Uint64(resp.Extras[len(resp.Extras)-8:])
		if got := vbucket.CurrentMetaState(1).CurrentSeqNo; got < seqNo {
			t.Errorf("%s: answered before the replica held sequence number %d (replica at %d)", req.Command.Name(), seqNo, got)
		}
	}
}

// Every durable mutation is refused with durability_impossible once it stores
// on a vbucket without enough replicas.
func TestDurableWritesRefusedWhenTheyStore(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)

	// Only one of the three replicas has a node, and a majority needs two.
	bucket, err := c.cluster.AddBucket(mock.NewBucketOptions{Name: "wide", Type: mock.BucketTypeCouchbase, NumReplicas: 3})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("wide")})
	vbID := vbucketOwnedAs(t, c, bucket, 0)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: make([]byte, 8)})
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: vbID, Key: []byte("n"), Value: []byte(`1`), Extras: make([]byte, 8)})

	for name, req := range map[string]*memd.Packet{
		"add":       {Command: memd.CmdAdd, Key: []byte("new"), Value: []byte(`{}`), Extras: make([]byte, 8)},
		"replace":   {Command: memd.CmdReplace, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)},
		"delete":    {Command: memd.CmdDelete, Key: []byte("doc")},
		"increment": {Command: memd.CmdIncrement, Key: []byte("n"), Extras: make([]byte, 20)},
		"decrement": {Command: memd.CmdDecrement, Key: []byte("n"), Extras: make([]byte, 20)},
		"append":    {Command: memd.CmdAppend, Key: []byte("doc"), Value: []byte(` `)},
		"prepend":   {Command: memd.CmdPrepend, Key: []byte("doc"), Value: []byte(` `)},
		"subdoc":    {Command: memd.CmdSubDocMultiMutation, Key: []byte("doc"), Value: encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Path: "b", Value: "1"})},
	} {
		req.Vbucket = vbID
		req.DurabilityLevelFrame = &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority}
		if got := c.Do(req).Status; got != memd.StatusDurabilityImpossible {
			t.Errorf("%s: expected status 0x%02x, got 0x%02x", name, uint16(memd.StatusDurabilityImpossible), uint16(got))
		}
	}
}

// A bucket without replicas needs only the active copy for a majority.
func TestDurableWriteWithoutReplicasNeedsOnlyActive(t *testing.T) {
	c := newSlowReplicationConn(t)

	bucket, err := c.cluster.AddBucket(mock.NewBucketOptions{Name: "single", Type: mock.BucketTypeCouchbase, NumReplicas: 0})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("single")})
	vbID := vbucketOwnedAs(t, c, bucket, 0)

	start := time.Now()
	c.mustSucceed(&memd.Packet{
		Command:              memd.CmdSet,
		Vbucket:              vbID,
		Key:                  []byte("doc"),
		Value:                []byte(`{}`),
		Extras:               make([]byte, 8),
		DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
	})
	// The cluster's replicas lag by a second, which this write must not wait for.
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Errorf("the write waited %v, as if for a replica", elapsed)
	}
}

// Timeouts of 1 ms and 0xfffe ms are the bounds kv_engine accepts.
func TestDurabilityTimeoutBoundsAreAccepted(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication)

	for _, timeout := range []time.Duration{time.Millisecond, 0xfffe * time.Millisecond} {
		resp := c.Do(&memd.Packet{
			Command:                memd.CmdSet,
			Vbucket:                c.vbID,
			Key:                    []byte("doc"),
			Value:                  []byte(`{}`),
			Extras:                 make([]byte, 8),
			DurabilityLevelFrame:   &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
			DurabilityTimeoutFrame: &memd.DurabilityTimeoutFrame{DurabilityTimeout: timeout},
		})
		// A millisecond is shorter than the replica latency, so that write
		// times out; neither is refused.
		if resp.Status != memd.StatusSuccess && resp.Status != memd.StatusSyncWriteAmbiguous {
			t.Errorf("timeout %v: expected the write accepted, got status 0x%02x", timeout, uint16(resp.Status))
		}
	}
}

// kv_engine parses the frames while validating the request, before it resolves
// the collection.
func TestInvalidDurabilityPrecedesUnknownCollection(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureAltRequests, memd.FeatureSyncReplication, memd.FeatureCollections)

	resp := c.Do(&memd.Packet{
		Command:              memd.CmdSet,
		Vbucket:              c.vbID,
		CollectionID:         0x99,
		Key:                  []byte("doc"),
		Value:                []byte(`{}`),
		Extras:               make([]byte, 8),
		DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevel(4)},
	})
	if resp.Status != memd.StatusDurabilityInvalidLevel {
		t.Errorf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusDurabilityInvalidLevel), uint16(resp.Status))
	}
}

// A queued request is charged the memory it holds, not only its bytes on the
// wire, so a flood of header-only requests behind a stalled one is
// disconnected before the heap grows past about twice the limit.
func TestQueuedRequestsChargedTheirMemory(t *testing.T) {
	const limit = 4 * 1024 * 1024
	saved := maxOutstandingRequestBytes
	t.Cleanup(func() { maxOutstandingRequestBytes = saved })
	maxOutstandingRequestBytes = limit
	cluster, c := newStalledReplicationCluster(t)
	maxOutstandingRequestBytes = saved

	kvService := cluster.Nodes()[0].(*clusterNodeInst).kvService
	clients := kvService.GetAllClients()
	if len(clients) != 1 {
		t.Fatalf("expected one client, got %d", len(clients))
	}
	queue := &clients[0].(*kvClient).requests
	outstanding := func() int {
		queue.lock.Lock()
		defer queue.lock.Unlock()
		return queue.outstanding
	}
	heap := func() uint64 {
		var stats runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&stats)
		return stats.HeapAlloc
	}

	writeStalledDurableSet(t, c)
	// The flood runs until the server disconnects, however long that takes.
	if err := c.netConn.SetDeadline(time.Time{}); err != nil {
		t.Fatalf("failed to clear deadline: %v", err)
	}
	base := heap()
	var grown uint64
	noop := make([]byte, 24)
	noop[0] = byte(memd.CmdMagicReq)
	noop[1] = byte(memd.CmdNoop)
	batch := bytes.Repeat(noop, 1000)
	for len(kvService.GetAllClients()) != 0 {
		if _, err := c.netConn.Write(batch); err != nil {
			// Expected once the server disconnects, which is checked
			// below.
			t.Logf("write failed: %v", err)
			break
		}
		// Wait until the reader has queued the batch, or disconnected.
		for last := -1; last != outstanding(); {
			last = outstanding()
			time.Sleep(20 * time.Millisecond)
		}
		if now := heap(); now > base && now-base > grown {
			grown = now - base
		}
		if grown > 2*limit {
			t.Fatalf("the heap grew by %d bytes before the client was disconnected, the limit is %d", grown, limit)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for len(kvService.GetAllClients()) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the server kept the client after its writes failed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
