package mockdb

import (
	"errors"
	"testing"
	"time"

	"github.com/couchbaselabs/gocaves/mock/mocktime"
)

func TestBasic(t *testing.T) {
	chrono := &mocktime.Chrono{}
	bucket, err := NewBucket(NewBucketOptions{
		Chrono:         chrono,
		NumReplicas:    2,
		NumVbuckets:    4,
		ReplicaLatency: 50 * time.Millisecond,
		PersistLatency: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	insDoc, err := bucket.Insert(&Document{
		VbID:      3,
		Key:       []byte("test"),
		Value:     []byte("hello world"),
		Xattrs:    nil,
		IsDeleted: false,
		Cas:       GenerateNewCas(chrono.Now()),
	})
	if err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}
	if insDoc.Cas == 0 {
		t.Fatalf("cas was not assigned correctly")
	}

	getDoc, err := bucket.Get(0, 3, 0, []byte("test"))
	if err != nil {
		t.Fatalf("failed to get document: %v", err)
	}
	if getDoc.Cas != insDoc.Cas {
		t.Fatalf("get cas was not retreived correctly")
	}
}

func TestReplication(t *testing.T) {
	chrono := &mocktime.Chrono{}
	bucket, err := NewBucket(NewBucketOptions{
		Chrono:         chrono,
		NumReplicas:    2,
		NumVbuckets:    4,
		ReplicaLatency: 50 * time.Millisecond,
		PersistLatency: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	insDoc, err := bucket.Insert(&Document{
		VbID:      3,
		Key:       []byte("test"),
		Value:     []byte("hello world"),
		Xattrs:    nil,
		IsDeleted: false,
		Cas:       GenerateNewCas(chrono.Now()),
	})
	if err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}
	if insDoc.Cas == 0 {
		t.Fatalf("cas was not assigned correctly")
	}

	repDoc1, err := bucket.Get(1, 3, 0, []byte("test"))
	if err == nil {
		t.Fatalf("first replica fetch should have failed")
	}
	if repDoc1 != nil {
		t.Fatalf("first replica fetch should have a nil document")
	}

	chrono.TimeTravel(100 * time.Millisecond)

	repDoc2, err := bucket.Get(1, 3, 0, []byte("test"))
	if err != nil {
		t.Fatalf("second replica fetch should have worked")
	}
	if repDoc2.Cas != insDoc.Cas {
		t.Fatalf("second replica cas was not retreived correctly")
	}
}

func TestGetRandom(t *testing.T) {
	chrono := &mocktime.Chrono{}
	bucket, err := NewBucket(NewBucketOptions{
		Chrono:         chrono,
		NumReplicas:    2,
		NumVbuckets:    1024,
		ReplicaLatency: 50 * time.Millisecond,
		PersistLatency: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	insDoc, err := bucket.Insert(&Document{
		VbID:  578,
		Key:   []byte("test"),
		Value: []byte("hello world"),
		Cas:   GenerateNewCas(chrono.Now()),
	})
	if err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}
	if insDoc.Cas == 0 {
		t.Fatalf("cas was not assigned correctly")
	}

	getDoc, err := bucket.GetRandom(0, 0)
	if err != nil {
		t.Fatalf("failed to get document: %v", err)
	}
	if getDoc.Cas != insDoc.Cas {
		t.Fatalf("get cas was not retreived correctly")
	}
}

func TestGetRandomExpired(t *testing.T) {
	chrono := &mocktime.Chrono{}
	bucket, err := NewBucket(NewBucketOptions{
		Chrono:         chrono,
		NumReplicas:    2,
		NumVbuckets:    1024,
		ReplicaLatency: 50 * time.Millisecond,
		PersistLatency: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	insDoc, err := bucket.Insert(&Document{
		VbID:   578,
		Key:    []byte("test"),
		Value:  []byte("hello world"),
		Expiry: time.Now().Add(-1 * time.Second),
		Cas:    GenerateNewCas(chrono.Now()),
	})
	if err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}
	if insDoc.Cas == 0 {
		t.Fatalf("cas was not assigned correctly")
	}

	_, err = bucket.GetRandom(0, 0)
	if !errors.Is(err, ErrDocNotFound) {
		t.Fatalf("expected doc not found, was: %s", err)
	}
}

func TestRollbackStartsNewHistory(t *testing.T) {
	chrono := &mocktime.Chrono{}
	bucket, err := NewBucket(NewBucketOptions{
		Chrono:      chrono,
		NumReplicas: 1,
		NumVbuckets: 1,
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	vbucket := bucket.GetVbucket(0)

	uuids := map[uint64]bool{vbucket.CurrentMetaState(0).VbUUID: true}
	for i := 0; i < 2; i++ {
		snap := bucket.Snapshot()
		_, err := bucket.Insert(&Document{Key: []byte("doc"), Value: []byte("{}"), Cas: GenerateNewCas(chrono.Now())})
		if err != nil {
			t.Fatalf("failed to insert document: %v", err)
		}
		if err := bucket.Rollback(snap); err != nil {
			t.Fatalf("failed to roll back: %v", err)
		}

		uuid := vbucket.CurrentMetaState(0).VbUUID
		if uuids[uuid] {
			t.Fatalf("rollback %d reused vbucket UUID %x", i+1, uuid)
		}
		uuids[uuid] = true
	}
}

func TestGetVbucketOutOfRange(t *testing.T) {
	bucket, err := NewBucket(NewBucketOptions{Chrono: &mocktime.Chrono{}, NumVbuckets: 4})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	for _, vbIdx := range []uint{4, 5} {
		if vbucket := bucket.GetVbucket(vbIdx); vbucket != nil {
			t.Errorf("vbucket %d of 4: expected nil, got %v", vbIdx, vbucket)
		}
	}
}

func TestRollbackToSameSnapshotTwice(t *testing.T) {
	chrono := &mocktime.Chrono{}
	bucket, err := NewBucket(NewBucketOptions{Chrono: chrono, NumReplicas: 1, NumVbuckets: 1})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// The first rollback starts a branch at the snapshot's sequence number,
	// which the snapshot still names.
	snap := bucket.Snapshot()
	for i := 0; i < 2; i++ {
		if _, err := bucket.Insert(&Document{Key: []byte("doc"), Value: []byte("{}"), Cas: GenerateNewCas(chrono.Now())}); err != nil {
			t.Fatalf("failed to insert document: %v", err)
		}
		if err := bucket.Rollback(snap); err != nil {
			t.Fatalf("rollback %d: %v", i+1, err)
		}
	}
}

func TestRollbackToSnapshotDiscardedByLaterRollback(t *testing.T) {
	chrono := &mocktime.Chrono{}
	bucket, err := NewBucket(NewBucketOptions{Chrono: chrono, NumReplicas: 1, NumVbuckets: 1})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	insert := func(key string) {
		t.Helper()
		if _, err := bucket.Insert(&Document{Key: []byte(key), Value: []byte("{}"), Cas: GenerateNewCas(chrono.Now())}); err != nil {
			t.Fatalf("failed to insert document: %v", err)
		}
	}

	early := bucket.Snapshot()
	insert("first")
	late := bucket.Snapshot()
	insert("second")
	if err := bucket.Rollback(late); err != nil {
		t.Fatalf("failed to roll back to the late snapshot: %v", err)
	}
	if err := bucket.Rollback(early); err != nil {
		t.Fatalf("failed to roll back to the early snapshot: %v", err)
	}

	// The second rollback discarded what the late snapshot names.
	if err := bucket.Rollback(late); err == nil {
		t.Errorf("expected the discarded snapshot to be refused")
	}
}
