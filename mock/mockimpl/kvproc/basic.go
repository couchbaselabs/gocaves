package kvproc

import (
	"strconv"
	"strings"
	"time"

	"github.com/couchbaselabs/gocaves/mock/mockdb"
)

// Engine represents a specific engine.
type Engine struct {
	db          *mockdb.Bucket
	vbOwnership []int

	durabilityErr error
	syncWrite     *mockdb.SyncWrite
}

// Store returns the bucket store the engine operates on.
func (e *Engine) Store() *mockdb.Bucket {
	return e.db
}

// SetDurabilityError makes SET, ADD, REPLACE, DELETE, arithmetic, APPEND,
// PREPEND and sub-document mutations fail with err at the point where
// kv_engine checks the durability requirements of a write: SET, ADD, REPLACE
// and DELETE first, once the vbucket is known to be active; the others just
// before the store, after the document was read and checked.
func (e *Engine) SetDurabilityError(err error) {
	e.durabilityErr = err
}

// SetSyncWrite makes the mutations SetDurabilityError lists durable writes,
// tracked by the vbucket from the moment they get their sequence number.
func (e *Engine) SetSyncWrite(w *mockdb.SyncWrite) {
	e.syncWrite = w
}

// update and insert store a mutation as a durable write when SetSyncWrite
// made the request one.
func (e *Engine) update(vbID, collectionID uint, key []byte,
	fn func(*mockdb.Document) (*mockdb.Document, error)) (*mockdb.Document, error) {
	return e.db.Update(vbID, collectionID, key, e.durable(fn))
}

// updateAfterRead is update for a request kv_engine serves by reading the
// committed document and then storing a new one: a durable write in progress
// on the key refuses it only when it would store.
func (e *Engine) updateAfterRead(vbID, collectionID uint, key []byte,
	fn func(*mockdb.Document) (*mockdb.Document, error)) (*mockdb.Document, error) {
	return e.db.UpdateAfterRead(vbID, collectionID, key, e.durable(fn))
}

// updateIfFound is update for REPLACE, which kv_engine refuses for a missing
// document before it looks for a durable write in progress.
func (e *Engine) updateIfFound(vbID, collectionID uint, key []byte,
	fn func(*mockdb.Document) (*mockdb.Document, error)) (*mockdb.Document, error) {
	return e.db.UpdateIfFound(vbID, collectionID, key, e.durable(fn))
}

// syncWriteStoreErr is what kv_engine answers the store of a request that
// passed its read-time checks on a key with a durable write in progress: its
// durability check comes first, and its lock check after.
func (e *Engine) syncWriteStoreErr() error {
	if e.durabilityErr != nil {
		return e.durabilityErr
	}
	return mockdb.ErrSyncWriteInProgress
}

func (e *Engine) durable(fn mockdb.UpdateFunc) mockdb.UpdateFunc {
	return func(idoc *mockdb.Document) (*mockdb.Document, error) {
		doc, err := fn(idoc)
		if doc != nil {
			doc.SyncWrite = e.syncWrite
		}
		return doc, err
	}
}

func (e *Engine) insert(doc *mockdb.Document) (*mockdb.Document, error) {
	doc.SyncWrite = e.syncWrite
	return e.db.Insert(doc)
}

// New creates a new crudproc engine using a mockdb and a list of what replicas
// are owned by this particular engine.
func New(db *mockdb.Bucket, vbOwnership []int) *Engine {
	return &Engine{
		db:          db,
		vbOwnership: vbOwnership,
	}
}

func (e *Engine) findReplicaIdx(vbIdx uint) int {
	if vbIdx >= uint(len(e.vbOwnership)) {
		return -1
	}

	return e.vbOwnership[vbIdx]
}

func (e *Engine) confirmIsMaster(vbIdx uint) error {
	repIdx := e.findReplicaIdx(vbIdx)
	if repIdx != 0 {
		return ErrNotMyVbucket
	}

	return nil
}

// lockedCas is the CAS a read reports for a locked document.
const lockedCas = 0xFFFFFFFFFFFFFFFF

// parseCounter parses a counter value as kv_engine's safe_strtoull does.
// Leading white space and one plus sign are skipped, and the digits may be
// followed by white space and then anything.
func parseCounter(value []byte) (uint64, bool) {
	s := strings.TrimLeft(string(value), " \t\n\v\f\r")
	s = strings.TrimPrefix(s, "+")
	digits := strings.IndexFunc(s, func(r rune) bool { return r < '0' || r > '9' })
	if digits == -1 {
		digits = len(s)
	} else if !strings.ContainsRune(" \t\n\v\f\r", rune(s[digits])) {
		return 0, false
	}
	val, err := strconv.ParseUint(s[:digits], 10, 64)
	if err != nil {
		return 0, false
	}
	return val, true
}

// docIsLocked reports whether doc is locked. A deleted or expired document is
// not, whatever lock it carried.
func (e *Engine) docIsLocked(doc *mockdb.Document) bool {
	if doc == nil || doc.IsDeleted {
		return false
	}

	if doc.LockExpiry.IsZero() {
		return false
	}

	return e.db.Chrono().Now().Before(doc.LockExpiry)
}

func (e *Engine) parseExpiry(expiry uint32) time.Time {
	if expiry == 0 {
		return time.Time{}
	}

	// TODO(brett19): Check if this is the right edge for expiry.
	if expiry > 30*24*60*60 {
		return time.Unix(int64(expiry), 0).Add(e.db.Chrono().TimeShift())
	}

	expiryDura := time.Duration(expiry) * time.Second
	return e.db.Chrono().Now().Add(expiryDura)
}

func (e *Engine) HLC() time.Time {
	return e.db.Chrono().Now()
}
