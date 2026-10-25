package kvproc

import (
	"errors"
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock/mockdb"
	"github.com/couchbaselabs/gocaves/mock/mocktime"
)

func xattrSet(path, value string) *SubDocOp {
	return &SubDocOp{Op: memd.SubDocOpDictSet, Path: path, Value: []byte(value), IsXattrPath: true, CreatePath: true}
}

// newTombstone stores doc deleted with the xattr old and returns its CAS.
func newTombstone(t *testing.T, e *Engine) uint64 {
	t.Helper()

	res, err := e.MultiMutate(MultiMutateOptions{
		Key:             []byte("doc"),
		CreateIfMissing: true,
		CreateAsDeleted: true,
		Ops:             []*SubDocOp{xattrSet("old.x", "1")},
	})
	if err != nil {
		t.Fatalf("failed to create the deleted document: %v", err)
	}
	return res.Cas
}

// kv_engine stores a revived document with Add semantics and a zero CAS
// (subdoc_update): it replaces the deleted document even if that changed
// after the read, and fails on a live one. A request with Mkdoc retries the
// failure, unless it carries a CAS; any other answers not_stored.
func TestReviveStoresWithAddSemantics(t *testing.T) {
	createLive := func(e *Engine) error {
		_, err := e.Set(StoreOptions{Key: []byte("doc"), Value: []byte(`{}`)})
		return err
	}
	for _, test := range []struct {
		name      string
		mkdoc     bool
		cas       bool
		meanwhile func(e *Engine) error
		err       error
	}{
		{name: "deleted document updated", meanwhile: func(e *Engine) error {
			_, err := e.MultiMutate(MultiMutateOptions{Key: []byte("doc"), AccessDeleted: true, Ops: []*SubDocOp{xattrSet("old.y", "2")}})
			return err
		}},
		{name: "live document created", meanwhile: createLive, err: ErrNotStored},
		{name: "live document created, with a CAS", cas: true, meanwhile: createLive, err: ErrNotStored},
		{name: "live document created, with Mkdoc", mkdoc: true, meanwhile: createLive, err: ErrSdCanOnlyReviveDeletedDocuments},
		{name: "live document created, with Mkdoc and a CAS", mkdoc: true, cas: true, meanwhile: createLive, err: ErrDocExists},
	} {
		t.Run(test.name, func(t *testing.T) {
			chrono := &mocktime.Chrono{}
			db, err := mockdb.NewBucket(mockdb.NewBucketOptions{Chrono: chrono, NumVbuckets: 1})
			if err != nil {
				t.Fatalf("failed to create bucket: %v", err)
			}
			e := New(db, []int{0})
			tombstoneCas := newTombstone(t, e)

			var meanwhileErr error
			done := false
			afterMultiMutateRead = func() {
				if !done {
					done = true
					meanwhileErr = test.meanwhile(e)
				}
			}
			t.Cleanup(func() { afterMultiMutateRead = func() {} })

			opts := MultiMutateOptions{
				Key:             []byte("doc"),
				AccessDeleted:   true,
				ReviveDocument:  true,
				CreateIfMissing: test.mkdoc,
				Ops:             []*SubDocOp{xattrSet("old.z", "3")},
			}
			if test.cas {
				opts.Cas = tombstoneCas
			}
			_, err = e.MultiMutate(opts)
			if meanwhileErr != nil {
				t.Fatalf("failed to change the document meanwhile: %v", meanwhileErr)
			}
			if !errors.Is(err, test.err) {
				t.Fatalf("expected %v, got %v", test.err, err)
			}
			if test.err != nil {
				return
			}

			doc, err := db.Get(0, 0, 0, []byte("doc"))
			if err != nil {
				t.Fatalf("failed to read doc: %v", err)
			}
			// The specs ran on the document read, so the change made meanwhile
			// is replaced.
			if doc.IsDeleted || string(doc.Xattrs["old"]) != `{"x":1,"z":3}` {
				t.Errorf("expected the live document from the read, got deleted=%t xattr old %s", doc.IsDeleted, doc.Xattrs["old"])
			}
		})
	}
}
