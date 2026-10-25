package mockdb

import (
	"testing"
	"time"

	"github.com/couchbaselabs/gocaves/mock/mocktime"
)

func newExpiringBucket(t *testing.T) (*Bucket, *mocktime.Chrono, *Document) {
	t.Helper()

	chrono := &mocktime.Chrono{}
	bucket, err := NewBucket(NewBucketOptions{
		Chrono:         chrono,
		NumReplicas:    1,
		NumVbuckets:    1,
		ReplicaLatency: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	doc, err := bucket.Insert(&Document{
		Key:      []byte("doc"),
		Value:    []byte(`{"a":1}`),
		Datatype: 1,
		Xattrs:   map[string][]byte{"_sys": []byte(`1`), "user": []byte(`2`)},
		Expiry:   chrono.Now().Add(10 * time.Second),
		Cas:      GenerateNewCas(chrono.Now()),
	})
	if err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}
	chrono.TimeTravel(20 * time.Second)
	return bucket, chrono, doc
}

func checkExpired(t *testing.T, what string, doc *Document) {
	t.Helper()

	if !doc.IsDeleted || len(doc.Value) != 0 || doc.Datatype != 0 {
		t.Errorf("%s: expected a deleted document with no body, got deleted=%t body %q datatype %d", what, doc.IsDeleted, doc.Value, doc.Datatype)
	}
	if _, ok := doc.Xattrs["user"]; ok || string(doc.Xattrs["_sys"]) != "1" {
		t.Errorf("%s: expected only the system xattrs, got %v", what, doc.Xattrs)
	}
}

// TestReadExpiresDocument checks that a read of the active copy stores the
// deletion of a document that expired, once, with a new CAS, sequence number
// and revision, as kv_engine's VBucket::processExpiredItem does.
func TestReadExpiresDocument(t *testing.T) {
	bucket, _, stored := newExpiringBucket(t)

	doc, err := bucket.Get(0, 0, 0, []byte("doc"))
	if err != nil {
		t.Fatalf("failed to read document: %v", err)
	}
	checkExpired(t, "read", doc)
	if doc.Cas == stored.Cas || doc.SeqNo != stored.SeqNo+1 || doc.RevID != stored.RevID+1 {
		t.Errorf("expected a new mutation, got CAS 0x%x seqno %d revid %d after CAS 0x%x seqno %d revid %d",
			doc.Cas, doc.SeqNo, doc.RevID, stored.Cas, stored.SeqNo, stored.RevID)
	}

	again, err := bucket.Get(0, 0, 0, []byte("doc"))
	if err != nil {
		t.Fatalf("failed to read document: %v", err)
	}
	if again.Cas != doc.Cas || again.SeqNo != doc.SeqNo {
		t.Errorf("expected the deletion to be stored once, got seqno %d after %d", again.SeqNo, doc.SeqNo)
	}

	mutations, _, err := bucket.GetVbucket(0).GetAllWithin(0, 0, 0)
	if err != nil {
		t.Fatalf("failed to list mutations: %v", err)
	}
	if len(mutations) != 2 || !mutations[1].IsDeleted {
		t.Errorf("expected the deletion as the second mutation, got %d mutations", len(mutations))
	}
}

// TestReplicaReadDoesNotExpireDocument checks that a replica hides a document
// that expired without storing anything: kv_engine expires it on the active
// copy, and the deletion reaches the replica from there.
func TestReplicaReadDoesNotExpireDocument(t *testing.T) {
	bucket, _, _ := newExpiringBucket(t)

	doc, err := bucket.Get(1, 0, 0, []byte("doc"))
	if err != nil {
		t.Fatalf("failed to read document: %v", err)
	}
	if !doc.IsDeleted {
		t.Errorf("expected the replica to see the document as deleted")
	}
	mutations, _, err := bucket.GetVbucket(0).GetAllWithin(0, 0, 0)
	if err != nil {
		t.Fatalf("failed to list mutations: %v", err)
	}
	if len(mutations) != 1 {
		t.Errorf("expected no new mutation, got %d mutations", len(mutations))
	}
}

// TestGetAllLeavesStoredDocuments checks that GetAll reports a document that
// expired as kv_engine leaves it, without changing the stored document.
func TestGetAllLeavesStoredDocuments(t *testing.T) {
	bucket, _, _ := newExpiringBucket(t)

	docs, err := bucket.GetAll(0, 0)
	if err != nil {
		t.Fatalf("failed to list documents: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected one document, got %d", len(docs))
	}
	checkExpired(t, "GetAll", docs[0])

	mutations, _, err := bucket.GetVbucket(0).GetAllWithin(0, 0, 0)
	if err != nil {
		t.Fatalf("failed to list mutations: %v", err)
	}
	if stored := mutations[0]; stored.IsDeleted || string(stored.Value) != `{"a":1}` || len(stored.Xattrs) != 2 {
		t.Errorf("expected the stored document unchanged, got deleted=%t body %q xattrs %v", stored.IsDeleted, stored.Value, stored.Xattrs)
	}
}

// TestInMemoryChanges checks that a change made in memory, as a lock is, is
// seen by reads of the active copy, Get, GetAll and GetRandom, not by a
// replica, and that its entry goes once a mutation supersedes it or it no
// longer differs from the stored document.
func TestInMemoryChanges(t *testing.T) {
	chrono := &mocktime.Chrono{}
	bucket, err := NewBucket(NewBucketOptions{Chrono: chrono, NumReplicas: 1, NumVbuckets: 1})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	stored, err := bucket.Insert(&Document{Key: []byte("doc"), Value: []byte(`{}`), Cas: GenerateNewCas(chrono.Now())})
	if err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}
	vb := bucket.GetVbucket(0)
	inMemory := func(cas uint64, lockExpiry time.Time) {
		t.Helper()
		_, err := bucket.Update(0, 0, []byte("doc"), func(doc *Document) (*Document, error) {
			doc.Cas, doc.LockExpiry, doc.InMemory = cas, lockExpiry, true
			return doc, nil
		})
		if err != nil {
			t.Fatalf("failed to change the document in memory: %v", err)
		}
	}

	lockCas := stored.Cas + 1
	inMemory(lockCas, chrono.Now().Add(time.Minute))
	if doc, _ := bucket.Get(0, 0, 0, []byte("doc")); doc.Cas != lockCas || doc.SeqNo != stored.SeqNo {
		t.Errorf("Get: expected CAS %x at seqno %d, got %x at %d", lockCas, stored.SeqNo, doc.Cas, doc.SeqNo)
	}
	if doc, _ := bucket.Get(1, 0, 0, []byte("doc")); doc != nil && doc.Cas != stored.Cas {
		t.Errorf("replica: expected the stored CAS %x, got %x", stored.Cas, doc.Cas)
	}
	if docs, _ := bucket.GetAll(0, 0); len(docs) != 1 || docs[0].Cas != lockCas {
		t.Errorf("GetAll: expected CAS %x", lockCas)
	}
	if doc, _ := bucket.GetRandom(0, 0); doc == nil || doc.Cas != lockCas {
		t.Errorf("GetRandom: expected CAS %x", lockCas)
	}

	inMemory(stored.Cas, time.Time{})
	if len(vb.inMemory) != 0 {
		t.Errorf("expected no entry once the document is as stored, got %v", vb.inMemory)
	}
	inMemory(lockCas, time.Time{})
	if _, err := bucket.Update(0, 0, []byte("doc"), func(doc *Document) (*Document, error) {
		doc.Cas = GenerateNewCas(chrono.Now())
		return doc, nil
	}); err != nil {
		t.Fatalf("failed to update the document: %v", err)
	}
	if len(vb.inMemory) != 0 {
		t.Errorf("expected no entry once a mutation supersedes it, got %v", vb.inMemory)
	}
}

// TestLastSeqNoForUUID checks that the last sequence number is reported for
// an earlier history only, where the next one starts, as kv_engine's
// FailoverTable::getLastSeqnoForUUID reports it.
func TestLastSeqNoForUUID(t *testing.T) {
	chrono := &mocktime.Chrono{}
	bucket, err := NewBucket(NewBucketOptions{Chrono: chrono, NumReplicas: 1, NumVbuckets: 1})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	vbucket := bucket.GetVbucket(0)
	first := vbucket.CurrentMetaState(0).VbUUID
	if _, ok := vbucket.LastSeqNoForUUID(first); ok {
		t.Errorf("expected no last sequence number for the latest history")
	}

	snap := bucket.Snapshot()
	if _, err := bucket.Insert(&Document{Key: []byte("doc"), Value: []byte("{}"), Cas: GenerateNewCas(chrono.Now())}); err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}
	if err := bucket.Rollback(snap); err != nil {
		t.Fatalf("failed to roll back: %v", err)
	}
	latest := vbucket.CurrentMetaState(0).VbUUID
	if seqNo, ok := vbucket.LastSeqNoForUUID(first); !ok || seqNo != 0 {
		t.Errorf("expected the first history to end at 0, got %d (%v)", seqNo, ok)
	}
	if _, ok := vbucket.LastSeqNoForUUID(latest); ok {
		t.Errorf("expected no last sequence number for the latest history")
	}
	if _, ok := vbucket.LastSeqNoForUUID(latest + 1); ok {
		t.Errorf("expected no last sequence number for an unknown UUID")
	}
}
