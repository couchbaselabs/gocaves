package kvproc

import (
	"time"

	"github.com/couchbaselabs/gocaves/mock/mockdb"
)

// Engine represents a specific engine.
type Engine struct {
	db          *mockdb.Bucket
	vbOwnership []int
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

func (e *Engine) docIsLocked(doc *mockdb.Document) bool {
	if doc == nil {
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
