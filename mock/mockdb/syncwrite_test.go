package mockdb

import (
	"testing"
	"time"

	"github.com/couchbaselabs/gocaves/mock/mocktime"
)

const (
	fenceReplicaLatency = 50 * time.Millisecond
	fencePersistLatency = 100 * time.Millisecond
	// A margin that keeps a query clear of the instant a write completes.
	fenceMargin = time.Millisecond
)

var (
	majority             = SyncWrite{Replicas: 1}
	majorityAndPersist   = SyncWrite{Replicas: 1, PersistActive: true}
	persistToMajority    = SyncWrite{Replicas: 1, PersistActive: true, PersistReplicas: true}
	timeoutAfterPersists = time.Hour
)

// newFenceVbucket returns the single vbucket of a bucket with one replica.
func newFenceVbucket(t *testing.T) (*Bucket, *Vbucket) {
	t.Helper()

	bucket, err := NewBucket(NewBucketOptions{
		Chrono:         &mocktime.Chrono{},
		NumReplicas:    1,
		NumVbuckets:    1,
		ReplicaLatency: fenceReplicaLatency,
		PersistLatency: fencePersistLatency,
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	return bucket, bucket.GetVbucket(0)
}

// storeSyncWrite stores a durable write of the given kind, timing out after
// timeout, or a plain write when w is nil.
func storeSyncWrite(t *testing.T, bucket *Bucket, key string, w *SyncWrite, timeout time.Duration) *Document {
	t.Helper()

	return storeSyncWriteBy(t, bucket, key, w, bucket.Chrono().Now().Add(timeout))
}

// storeSyncWriteBy stores a durable write of the given kind with the given
// deadline, or a plain write when w is nil.
func storeSyncWriteBy(t *testing.T, bucket *Bucket, key string, w *SyncWrite, deadline time.Time) *Document {
	t.Helper()

	doc := &Document{VbID: 0, Key: []byte(key), Value: []byte("{}")}
	if w != nil {
		tracked := *w
		tracked.Deadline = deadline
		doc.SyncWrite = &tracked
	}
	stored, err := bucket.Insert(doc)
	if err != nil {
		t.Fatalf("failed to store %s: %v", key, err)
	}
	return stored
}

func expectState(t *testing.T, vb *Vbucket, doc *Document, at time.Time, want SyncWriteState) {
	t.Helper()

	if got := vb.SyncWriteStateAt(doc.VbUUID, doc.SeqNo, at); got != want {
		t.Errorf("write %d at %v after it was stored: got state %d, expected %d", doc.SeqNo, at.Sub(doc.ModifiedTime), got, want)
	}
}

func TestSyncWriteLevelsCommitWhenCopiesHoldThem(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write SyncWrite
		// after is when the write is committed, from when it was stored.
		after time.Duration
	}{
		{"majority", majority, fenceReplicaLatency},
		{"majority_and_persist_to_active", majorityAndPersist, fencePersistLatency},
		{"persist_to_majority", persistToMajority, fenceReplicaLatency + fencePersistLatency},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bucket, vb := newFenceVbucket(t)
			w := tc.write
			doc := storeSyncWrite(t, bucket, "doc", &w, timeoutAfterPersists)

			expectState(t, vb, doc, doc.ModifiedTime.Add(tc.after-fenceMargin), SyncWritePending)
			expectState(t, vb, doc, doc.ModifiedTime.Add(tc.after+fenceMargin), SyncWriteCommitted)
		})
	}
}

func TestSyncWriteWithoutReplicasNeedsOnlyActive(t *testing.T) {
	bucket, vb := newFenceVbucket(t)
	doc := storeSyncWrite(t, bucket, "doc", &SyncWrite{}, timeoutAfterPersists)

	expectState(t, vb, doc, doc.ModifiedTime, SyncWriteCommitted)
}

// kv_engine's active durability monitor stops acknowledging at the first
// write it must persist and has not: a majority write behind it waits.
func TestMajorityWriteWaitsBehindUnpersistedWriteOnActive(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fence SyncWrite
	}{
		{"majority_and_persist_to_active", majorityAndPersist},
		{"persist_to_majority", persistToMajority},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bucket, vb := newFenceVbucket(t)
			fence := tc.fence
			first := storeSyncWrite(t, bucket, "first", &fence, timeoutAfterPersists)
			second := storeSyncWrite(t, bucket, "second", &majority, timeoutAfterPersists)

			// The replica holds both, but the active has not persisted first.
			expectState(t, vb, second, second.ModifiedTime.Add(fenceReplicaLatency+fenceMargin), SyncWritePending)
			expectState(t, vb, second, first.ModifiedTime.Add(fenceReplicaLatency+fencePersistLatency+fenceMargin), SyncWriteCommitted)
		})
	}
}

// kv_engine's passive durability monitor fences on persist_to_majority writes
// only: a replica acknowledges past a majority_and_persist_to_active write as
// soon as it holds it.
func TestReplicaFencesOnPersistToMajorityOnly(t *testing.T) {
	t.Run("persist_to_majority", func(t *testing.T) {
		bucket, vb := newFenceVbucket(t)
		first := storeSyncWrite(t, bucket, "first", &persistToMajority, timeoutAfterPersists)
		second := storeSyncWrite(t, bucket, "second", &majority, timeoutAfterPersists)

		// The active has persisted first and the replica holds second, but
		// the replica has not persisted first.
		expectState(t, vb, second, first.ModifiedTime.Add(fenceReplicaLatency+fencePersistLatency-fenceMargin), SyncWritePending)
		expectState(t, vb, second, first.ModifiedTime.Add(fenceReplicaLatency+fencePersistLatency+fenceMargin), SyncWriteCommitted)
	})

	t.Run("majority_and_persist_to_active", func(t *testing.T) {
		bucket, vb := newFenceVbucket(t)
		first := storeSyncWrite(t, bucket, "first", &majorityAndPersist, timeoutAfterPersists)
		second := storeSyncWrite(t, bucket, "second", &majority, timeoutAfterPersists)

		// The active persisted first; the replica holds second and has not
		// persisted first, which it does not need to.
		expectState(t, vb, second, first.ModifiedTime.Add(fencePersistLatency+fenceMargin), SyncWriteCommitted)
	})
}

// A write that times out first in line is aborted, which lifts its fence.
func TestAbortedWriteLiftsFence(t *testing.T) {
	bucket, vb := newFenceVbucket(t)
	first := storeSyncWrite(t, bucket, "first", &majorityAndPersist, fenceReplicaLatency/2)
	second := storeSyncWrite(t, bucket, "second", &majority, timeoutAfterPersists)

	// first timed out before the replica received second, and the active
	// has not persisted first.
	at := second.ModifiedTime.Add(fenceReplicaLatency + fenceMargin)
	expectState(t, vb, first, at, SyncWriteAborted)
	expectState(t, vb, second, at, SyncWriteCommitted)
}

// kv_engine aborts expired writes only from the front of its queue: a write
// past its deadline behind an incomplete earlier write is still pending, and
// commits with it.
func TestExpiredWriteWaitsForEarlierWrite(t *testing.T) {
	bucket, vb := newFenceVbucket(t)
	first := storeSyncWrite(t, bucket, "first", &persistToMajority, timeoutAfterPersists)
	second := storeSyncWrite(t, bucket, "second", &majority, fenceReplicaLatency/2)

	firstEnd := first.ModifiedTime.Add(fenceReplicaLatency + fencePersistLatency)
	expectState(t, vb, second, firstEnd.Add(-fenceMargin), SyncWritePending)
	expectState(t, vb, first, firstEnd.Add(fenceMargin), SyncWriteCommitted)
	expectState(t, vb, second, firstEnd.Add(fenceMargin), SyncWriteCommitted)
}

// kv_engine's removeExpired aborts every expired write at the front of its
// queue in one pass: a write behind an aborted one is aborted with it once its
// own deadline has passed, though the copies hold it.
func TestExpiredWriteAbortedWithEarlierWrite(t *testing.T) {
	bucket, vb := newFenceVbucket(t)
	first := storeSyncWrite(t, bucket, "first", &persistToMajority, 2*fenceReplicaLatency)
	second := storeSyncWrite(t, bucket, "second", &majority, fenceReplicaLatency/2)

	// first times out before it is persisted; second is replicated by then.
	firstEnd := first.ModifiedTime.Add(2 * fenceReplicaLatency)
	expectState(t, vb, second, firstEnd.Add(-fenceMargin), SyncWritePending)
	expectState(t, vb, first, firstEnd.Add(fenceMargin), SyncWriteAborted)
	expectState(t, vb, second, firstEnd.Add(fenceMargin), SyncWriteAborted)
}

// kv_engine expires a write only after its deadline, so a write with the same
// deadline as an aborted one has expired too when that one is aborted.
func TestWriteWithSameDeadlineAbortedWithEarlierWrite(t *testing.T) {
	bucket, vb := newFenceVbucket(t)
	deadline := bucket.Chrono().Now().Add(fenceReplicaLatency)
	first := storeSyncWriteBy(t, bucket, "first", &majorityAndPersist, deadline)
	// second needs only the active copy, which holds it at once.
	second := storeSyncWriteBy(t, bucket, "second", &SyncWrite{}, deadline)

	expectState(t, vb, first, deadline, SyncWritePending)
	expectState(t, vb, second, deadline, SyncWritePending)
	expectState(t, vb, first, deadline.Add(time.Nanosecond), SyncWriteAborted)
	expectState(t, vb, second, deadline.Add(time.Nanosecond), SyncWriteAborted)
}

// The writes before the first one without a response are dropped, and the
// writes after them still complete in order.
func TestRepliedWritesAreDropped(t *testing.T) {
	bucket, vb := newFenceVbucket(t)
	first := storeSyncWrite(t, bucket, "first", &persistToMajority, timeoutAfterPersists)
	second := storeSyncWrite(t, bucket, "second", &majority, timeoutAfterPersists)

	vb.MarkSyncWriteReplied(first.VbUUID, first.SeqNo)
	expectState(t, vb, first, first.ModifiedTime, SyncWriteUntracked)

	firstEnd := first.ModifiedTime.Add(fenceReplicaLatency + fencePersistLatency)
	expectState(t, vb, second, firstEnd.Add(-fenceMargin), SyncWritePending)
	expectState(t, vb, second, firstEnd.Add(fenceMargin), SyncWriteCommitted)
}

// A plain write is not tracked and fences nothing.
func TestPlainWriteDoesNotFence(t *testing.T) {
	bucket, vb := newFenceVbucket(t)
	plain := storeSyncWrite(t, bucket, "first", nil, 0)
	second := storeSyncWrite(t, bucket, "second", &majority, timeoutAfterPersists)

	expectState(t, vb, plain, plain.ModifiedTime, SyncWriteUntracked)
	expectState(t, vb, second, second.ModifiedTime.Add(fenceReplicaLatency+fenceMargin), SyncWriteCommitted)
}

// Flushing the vbucket discards its writes, and their fences with them.
func TestFlushDiscardsTrackedWrites(t *testing.T) {
	bucket, vb := newFenceVbucket(t)
	first := storeSyncWrite(t, bucket, "first", &majorityAndPersist, timeoutAfterPersists)
	vb.Flush()
	second := storeSyncWrite(t, bucket, "second", &majority, timeoutAfterPersists)

	expectState(t, vb, first, first.ModifiedTime, SyncWriteUntracked)
	expectState(t, vb, second, second.ModifiedTime.Add(fenceReplicaLatency+fenceMargin), SyncWriteCommitted)
}

// A flush discards pending writes, but their responses to a client still
// follow sequence-number order.
func TestDiscardedWritesKeepReplyOrder(t *testing.T) {
	bucket, vb := newFenceVbucket(t)
	client := &struct{}{}
	w := majority
	w.Client = client
	first := storeSyncWrite(t, bucket, "first", &w, timeoutAfterPersists)
	second := storeSyncWrite(t, bucket, "second", &w, timeoutAfterPersists)
	vb.Flush()

	if vb.EarlierSyncWritesReplied(second.VbUUID, second.SeqNo, client) {
		t.Errorf("expected the discarded second write to wait for the first one's response")
	}
	vb.MarkSyncWriteReplied(first.VbUUID, first.SeqNo)
	if !vb.EarlierSyncWritesReplied(second.VbUUID, second.SeqNo, client) {
		t.Errorf("expected the discarded second write free to reply once the first one has")
	}
	vb.MarkSyncWriteReplied(second.VbUUID, second.SeqNo)
	if len(vb.discardedSyncWrites) != 0 {
		t.Errorf("expected answered discarded writes dropped, %d left", len(vb.discardedSyncWrites))
	}
}

// A dropped write does not keep its client reachable through the vbucket.
func TestDroppedWriteReleasesClient(t *testing.T) {
	bucket, vb := newFenceVbucket(t)
	w := majority
	w.Client = &struct{}{}
	doc := storeSyncWrite(t, bucket, "doc", &w, timeoutAfterPersists)

	backing := vb.syncWrites
	vb.MarkSyncWriteReplied(doc.VbUUID, doc.SeqNo)
	if backing[0].Client != nil {
		t.Errorf("expected the dropped write cleared from the vbucket's array")
	}
}

// A write discarded by a flush is answered before the client's writes in the
// history that follows.
func TestDiscardedWriteRepliesBeforeLaterHistory(t *testing.T) {
	bucket, vb := newFenceVbucket(t)
	client := &struct{}{}
	w := majority
	w.Client = client
	first := storeSyncWrite(t, bucket, "first", &w, timeoutAfterPersists)
	vb.Flush()
	second := storeSyncWrite(t, bucket, "second", &w, timeoutAfterPersists)

	if vb.EarlierSyncWritesReplied(second.VbUUID, second.SeqNo, client) {
		t.Errorf("expected the write after the flush to wait for the discarded write's response")
	}
	vb.MarkSyncWriteReplied(first.VbUUID, first.SeqNo)
	if !vb.EarlierSyncWritesReplied(second.VbUUID, second.SeqNo, client) {
		t.Errorf("expected the write after the flush free to reply once the discarded write has")
	}
}

// A write is dropped as soon as it is answered, even while an earlier write
// from another client is still to be answered.
func TestAnsweredWriteDroppedBehindUnansweredOne(t *testing.T) {
	bucket, vb := newFenceVbucket(t)
	first := storeSyncWrite(t, bucket, "first", &persistToMajority, timeoutAfterPersists)
	second := storeSyncWrite(t, bucket, "second", &majority, timeoutAfterPersists)

	vb.MarkSyncWriteReplied(second.VbUUID, second.SeqNo)
	if len(vb.syncWrites) != 1 || vb.syncWrites[0].seqNo != first.SeqNo {
		t.Fatalf("expected only the unanswered write tracked, got %d writes", len(vb.syncWrites))
	}
	firstEnd := first.ModifiedTime.Add(fenceReplicaLatency + fencePersistLatency)
	expectState(t, vb, first, firstEnd.Add(fenceMargin), SyncWriteCommitted)
}
