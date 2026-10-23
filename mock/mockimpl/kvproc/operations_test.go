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

// A multi-mutation of a locked document skips the CAS check when it reads the
// document, so its CAS is checked when it is stored, even if the lock has
// gone since the read.
func TestMultiMutateChecksCasOfDocumentUnlockedSinceRead(t *testing.T) {
	for _, test := range []struct {
		name   string
		unlock func(e *Engine, chrono *mocktime.Chrono, lockCas uint64) error
	}{
		{name: "lock expired", unlock: func(e *Engine, chrono *mocktime.Chrono, lockCas uint64) error {
			chrono.TimeTravel(20 * time.Second)
			return nil
		}},
		{name: "unlocked", unlock: func(e *Engine, chrono *mocktime.Chrono, lockCas uint64) error {
			_, err := e.Unlock(UnlockOptions{Key: []byte("doc"), Cas: lockCas})
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			chrono := &mocktime.Chrono{}
			db, err := mockdb.NewBucket(mockdb.NewBucketOptions{Chrono: chrono, NumVbuckets: 1})
			if err != nil {
				t.Fatalf("failed to create bucket: %v", err)
			}
			e := New(db, []int{0})

			if _, err := e.Set(StoreOptions{Key: []byte("doc"), Value: []byte(`{"a":1}`)}); err != nil {
				t.Fatalf("failed to store doc: %v", err)
			}
			locked, err := e.GetLocked(GetLockedOptions{Key: []byte("doc"), LockTime: 10})
			if err != nil {
				t.Fatalf("failed to lock doc: %v", err)
			}

			var unlockErr error
			unlocked := false
			afterMultiMutateRead = func() {
				if !unlocked {
					unlocked = true
					unlockErr = test.unlock(e, chrono, locked.Cas)
				}
			}
			t.Cleanup(func() { afterMultiMutateRead = func() {} })

			_, err = e.MultiMutate(MultiMutateOptions{
				Key: []byte("doc"),
				Cas: locked.Cas + 1,
				Ops: []*SubDocOp{{Op: memd.SubDocOpDictSet, Path: "a", Value: []byte("2")}},
			})
			if unlockErr != nil {
				t.Fatalf("failed to unlock doc: %v", unlockErr)
			}
			if !errors.Is(err, ErrCasMismatch) {
				t.Errorf("expected ErrCasMismatch, got %v", err)
			}
			doc, err := e.Get(GetOptions{Key: []byte("doc")})
			if err != nil {
				t.Fatalf("failed to read doc: %v", err)
			}
			if string(doc.Value) != `{"a":1}` {
				t.Errorf("expected the document unchanged, got %s", doc.Value)
			}
		})
	}
}

// A document that expired is not found, though the lock it carried has not
// run out.
func TestMultiMutateOfExpiredLockedDocumentIsNotFound(t *testing.T) {
	chrono := &mocktime.Chrono{}
	db, err := mockdb.NewBucket(mockdb.NewBucketOptions{Chrono: chrono, NumVbuckets: 1})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	e := New(db, []int{0})

	if _, err := e.Set(StoreOptions{Key: []byte("doc"), Value: []byte(`{"a":1}`), Expiry: 10}); err != nil {
		t.Fatalf("failed to store doc: %v", err)
	}
	if _, err := e.GetLocked(GetLockedOptions{Key: []byte("doc"), LockTime: 30}); err != nil {
		t.Fatalf("failed to lock doc: %v", err)
	}
	chrono.TimeTravel(20 * time.Second)

	_, err = e.MultiMutate(MultiMutateOptions{
		Key: []byte("doc"),
		Ops: []*SubDocOp{{Op: memd.SubDocOpDictSet, Path: "a", Value: []byte("2")}},
	})
	if !errors.Is(err, ErrDocNotFound) {
		t.Errorf("expected ErrDocNotFound, got %v", err)
	}
}

// kv_engine expands ${$document.CAS} from the CAS a read reports, which for
// a locked document is 0xffffffffffffffff.
func TestMultiMutateOfLockedDocumentExpandsReadCas(t *testing.T) {
	chrono := &mocktime.Chrono{}
	db, err := mockdb.NewBucket(mockdb.NewBucketOptions{Chrono: chrono, NumVbuckets: 1})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	e := New(db, []int{0})

	if _, err := e.Set(StoreOptions{Key: []byte("doc"), Value: []byte(`{"a":1}`)}); err != nil {
		t.Fatalf("failed to store doc: %v", err)
	}
	locked, err := e.GetLocked(GetLockedOptions{Key: []byte("doc"), LockTime: 30})
	if err != nil {
		t.Fatalf("failed to lock doc: %v", err)
	}

	_, err = e.MultiMutate(MultiMutateOptions{
		Key: []byte("doc"),
		Cas: locked.Cas,
		Ops: []*SubDocOp{{Op: memd.SubDocOpDictSet, Path: "cas", Value: []byte(`"${$document.CAS}"`), IsXattrPath: true, ExpandMacros: true}},
	})
	if err != nil {
		t.Fatalf("failed to mutate doc: %v", err)
	}
	res, err := e.MultiLookup(MultiLookupOptions{
		Key: []byte("doc"),
		Ops: []*SubDocOp{{Op: memd.SubDocOpGet, Path: "cas", IsXattrPath: true}},
	})
	if err != nil {
		t.Fatalf("failed to look up doc: %v", err)
	}
	if res.Ops[0].Err != nil || string(res.Ops[0].Value) != `"0xffffffffffffffff"` {
		t.Errorf(`expected "0xffffffffffffffff", got %s (error %v)`, res.Ops[0].Value, res.Ops[0].Err)
	}
}
