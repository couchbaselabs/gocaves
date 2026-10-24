package mockimpl

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
)

// kv_engine stores a durable write as a prepare. Until it commits, reads see
// the committed document, and a write to the key is refused with
// sync_write_in_progress: before the CAS and lock checks by the engine's
// set, add, delete, touch, get-locked and unlock paths, after the existence
// check by replace, and when it would store by the requests that read the
// committed document first. An aborted prepare leaves the committed document
// as it was.
func TestPendingDurableWriteIsAPrepare(t *testing.T) {
	// The replica never receives a write, so both durable writes time out.
	c := newReplicationConn(t, time.Hour, 0, memd.FeatureUnorderedExec)
	// Without XERROR kv_engine answers temporary_failure instead.
	other := connectTestKv(t, c.cluster, memd.FeatureAltRequests, memd.FeatureSyncReplication, memd.FeatureXerror)

	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"v":"old"}`), Extras: make([]byte, 8)})
	const timeout = 3 * time.Second
	start := time.Now()
	pipelineDurableSet(t, c, 1, "doc", memd.DurabilityLevelMajority, timeout)
	pipelineDurableSet(t, c, 2, "fresh", memd.DurabilityLevelMajority, timeout)
	// One connection's requests are stored in order.
	waitForDocument(t, c, "fresh")

	expectCommitted := func(when string) {
		t.Helper()
		if resp := other.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")}); string(resp.Value) != `{"v":"old"}` {
			t.Errorf("%s: expected GET of doc to return the committed value, got %s", when, resp.Value)
		}
		if resp := other.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("fresh")}); resp.Status != memd.StatusKeyNotFound {
			t.Errorf("%s: expected GET of fresh to find nothing, got status 0x%02x", when, uint16(resp.Status))
		}
	}
	expectCommitted("while pending")

	lockTime := make([]byte, 4)
	binary.BigEndian.PutUint32(lockTime, 10)
	// Delta 1, initial 0, expiry 0xffffffff: a missing counter is not
	// created.
	noCreate := make([]byte, 20)
	binary.BigEndian.PutUint64(noCreate, 1)
	binary.BigEndian.PutUint32(noCreate[16:], 0xffffffff)
	update := encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Path: "v", Value: `"new"`})
	for _, test := range []struct {
		name   string
		req    memd.Packet
		status memd.StatusCode
	}{
		{"SET", memd.Packet{Command: memd.CmdSet, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)}, memd.StatusSyncWriteInProgress},
		{"ADD", memd.Packet{Command: memd.CmdAdd, Key: []byte("fresh"), Value: []byte(`{}`), Extras: make([]byte, 8)}, memd.StatusSyncWriteInProgress},
		{"REPLACE", memd.Packet{Command: memd.CmdReplace, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)}, memd.StatusSyncWriteInProgress},
		{"DELETE", memd.Packet{Command: memd.CmdDelete, Key: []byte("doc")}, memd.StatusSyncWriteInProgress},
		// Refused before the CAS or lock is looked at, and DELETE before the
		// committed document.
		{"REPLACE without a committed document", memd.Packet{Command: memd.CmdReplace, Key: []byte("fresh"), Value: []byte(`{}`), Extras: make([]byte, 8)}, memd.StatusKeyNotFound},
		{"REPLACE with another CAS", memd.Packet{Command: memd.CmdReplace, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8), Cas: 1}, memd.StatusSyncWriteInProgress},
		{"DELETE without a committed document", memd.Packet{Command: memd.CmdDelete, Key: []byte("fresh")}, memd.StatusSyncWriteInProgress},
		{"TOUCH", memd.Packet{Command: memd.CmdTouch, Key: []byte("doc"), Extras: make([]byte, 4)}, memd.StatusSyncWriteInProgress},
		{"GAT", memd.Packet{Command: memd.CmdGAT, Key: []byte("doc"), Extras: make([]byte, 4)}, memd.StatusSyncWriteInProgress},
		{"GET_LOCKED", memd.Packet{Command: memd.CmdGetLocked, Key: []byte("doc"), Extras: lockTime}, memd.StatusSyncWriteInProgress},
		{"UNLOCK", memd.Packet{Command: memd.CmdUnlockKey, Key: []byte("doc"), Cas: 1}, memd.StatusSyncWriteInProgress},
		{"durable SET", memd.Packet{
			Command: memd.CmdSet, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8),
			DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
		}, memd.StatusSyncWriteInProgress},
		// These read the committed document first.
		{"APPEND", memd.Packet{Command: memd.CmdAppend, Key: []byte("doc"), Value: []byte(" ")}, memd.StatusSyncWriteInProgress},
		{"INCREMENT without a committed document", memd.Packet{Command: memd.CmdIncrement, Key: []byte("fresh"), Extras: noCreate}, memd.StatusKeyNotFound},
		{"INCREMENT creating the document", memd.Packet{Command: memd.CmdIncrement, Key: []byte("fresh"), Extras: make([]byte, 20)}, memd.StatusSyncWriteInProgress},
		{"SUBDOC_MULTI_MUTATION", memd.Packet{Command: memd.CmdSubDocMultiMutation, Key: []byte("doc"), Value: update}, memd.StatusSyncWriteInProgress},
		{"SUBDOC_MULTI_MUTATION without a committed document", memd.Packet{Command: memd.CmdSubDocMultiMutation, Key: []byte("fresh"), Value: update}, memd.StatusKeyNotFound},
	} {
		req := test.req
		req.Vbucket = c.vbID
		if resp := other.Do(&req); resp.Status != test.status {
			t.Errorf("%s: expected status 0x%02x, got 0x%02x", test.name, uint16(test.status), uint16(resp.Status))
		}
	}

	// APPEND reads the committed document first, so a key without one is
	// refused as missing, not as a durable write in progress.
	if resp := other.Do(&memd.Packet{Command: memd.CmdAppend, Vbucket: c.vbID, Key: []byte("fresh"), Value: []byte(" ")}); resp.Status == memd.StatusSyncWriteInProgress || resp.Status == memd.StatusSuccess {
		t.Errorf("APPEND without a committed document: expected it refused as missing, got status 0x%02x", uint16(resp.Status))
	}

	if elapsed := time.Since(start); elapsed >= timeout {
		t.Fatalf("the checks took %v, past the durable writes' timeout of %v", elapsed, timeout)
	}
	for _, r := range readTimedResponses(t, c, start, 2) {
		if r.status != memd.StatusSyncWriteAmbiguous {
			t.Errorf("opaque %d: expected status 0x%02x, got 0x%02x", r.opaque, uint16(memd.StatusSyncWriteAmbiguous), uint16(r.status))
		}
	}
	expectCommitted("after the abort")
	other.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)})
}

// A durable write is readable once it commits.
func TestCommittedDurableWriteIsReadable(t *testing.T) {
	c := newReplicationConn(t, 10*time.Millisecond, 0)

	resp := c.mustSucceed(&memd.Packet{
		Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"v":"new"}`), Extras: make([]byte, 8),
		DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
	})
	if got := c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")}); string(got.Value) != `{"v":"new"}` || got.Cas != resp.Cas {
		t.Errorf("expected the committed write with CAS %x, got %s with CAS %x", resp.Cas, got.Value, got.Cas)
	}
}

// A durable write may replace a locked document with the CAS that locked it.
// While it is pending, kv_engine refuses the store of a request that passed
// its read-time checks with sync_write_in_progress before its lock check, and
// answers a request that fails them as it would without the prepare.
func TestPendingDurableWriteOverLockedDocument(t *testing.T) {
	c := newReplicationConn(t, time.Hour, 0, memd.FeatureUnorderedExec)
	other := connectTestKv(t, c.cluster, memd.FeatureAltRequests, memd.FeatureSyncReplication, memd.FeatureXerror)

	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("num"), Value: []byte("1"), Extras: make([]byte, 8)})
	lockTime := make([]byte, 4)
	binary.BigEndian.PutUint32(lockTime, 30)
	lockCas := c.mustSucceed(&memd.Packet{Command: memd.CmdGetLocked, Vbucket: c.vbID, Key: []byte("num"), Extras: lockTime}).Cas

	const timeout = 3 * time.Second
	start := time.Now()
	if err := c.conn.WritePacket(&memd.Packet{
		Magic: memd.CmdMagicReq, Command: memd.CmdSet, Opaque: 1, Vbucket: c.vbID, Key: []byte("num"), Value: []byte("5"),
		Extras: make([]byte, 8), Cas: lockCas,
		DurabilityLevelFrame:   &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
		DurabilityTimeoutFrame: &memd.DurabilityTimeoutFrame{DurabilityTimeout: timeout},
	}); err != nil {
		t.Fatalf("failed to write the durable SET: %v", err)
	}
	waitForSeqNo(t, c, 3)

	increment := make([]byte, 20)
	binary.BigEndian.PutUint64(increment, 1)
	for _, test := range []struct {
		name   string
		req    memd.Packet
		status memd.StatusCode
	}{
		{"INCREMENT without a CAS", memd.Packet{Command: memd.CmdIncrement, Key: []byte("num"), Extras: increment}, memd.StatusSyncWriteInProgress},
		{"APPEND with another CAS", memd.Packet{Command: memd.CmdAppend, Key: []byte("num"), Value: []byte("0"), Cas: lockCas + 1}, memd.StatusSyncWriteInProgress},
		// Refused when the locked document is read.
		{"APPEND without a CAS", memd.Packet{Command: memd.CmdAppend, Key: []byte("num"), Value: []byte("0")}, memd.StatusLocked},
	} {
		req := test.req
		req.Vbucket = c.vbID
		if resp := other.Do(&req); resp.Status != test.status {
			t.Errorf("%s: expected status 0x%02x, got 0x%02x", test.name, uint16(test.status), uint16(resp.Status))
		}
	}
	if elapsed := time.Since(start); elapsed >= timeout {
		t.Fatalf("the checks took %v, past the durable write's timeout of %v", elapsed, timeout)
	}
}

// waitForSeqNo waits until the active copy of the connection's vbucket has
// reached seqNo.
func waitForSeqNo(t *testing.T, c *testKvConn, seqNo uint64) {
	t.Helper()

	vbucket := c.cluster.GetBucket("default").Store().GetVbucket(uint(c.vbID))
	deadline := time.Now().Add(testKvTimeout)
	for vbucket.CurrentMetaState(0).CurrentSeqNo < seqNo {
		if time.Now().After(deadline) {
			t.Fatalf("the vbucket never reached sequence number %d", seqNo)
		}
		time.Sleep(time.Millisecond)
	}
}
