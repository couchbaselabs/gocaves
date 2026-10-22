package kvproc

import (
	"errors"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock/mockdb"
	"github.com/couchbaselabs/gocaves/mock/mocktime"
)

func TestMultiMutateDoesNotReviveDocumentExpiredSinceRead(t *testing.T) {
	chrono := &mocktime.Chrono{}
	db, err := mockdb.NewBucket(mockdb.NewBucketOptions{Chrono: chrono, NumVbuckets: 1})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	e := New(db, []int{0})

	if _, err := e.Set(StoreOptions{Key: []byte("doc"), Value: []byte(`{"a":1}`), Expiry: 10}); err != nil {
		t.Fatalf("failed to store doc: %v", err)
	}

	// The document expires after the mutation reads it.
	afterMultiMutateRead = func() { chrono.TimeTravel(20 * time.Second) }
	t.Cleanup(func() { afterMultiMutateRead = func() {} })

	_, err = e.MultiMutate(MultiMutateOptions{
		Key: []byte("doc"),
		Ops: []*SubDocOp{{Op: memd.SubDocOpDictSet, Path: "b", Value: []byte("2")}},
	})
	if !errors.Is(err, ErrDocNotFound) {
		t.Errorf("expected ErrDocNotFound, got %v", err)
	}
	if _, err := e.Get(GetOptions{Key: []byte("doc")}); !errors.Is(err, ErrDocNotFound) {
		t.Errorf("expected the expired document to stay expired, got %v", err)
	}
}
