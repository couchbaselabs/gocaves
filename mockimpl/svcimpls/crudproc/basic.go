package crudproc

import "github.com/couchbaselabs/gocaves/mockdb"

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

	return e.db.Chrono().Now().Before(doc.LockExpiry)
}
