package kvproc

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock/mockdb"
)

// GetOptions specifies options for a GET operation.
type GetOptions struct {
	Vbucket      uint
	CollectionID uint
	Key          []byte
}

// GetResult contains the results of a GET operation.
type GetResult struct {
	Cas      uint64
	Datatype uint8
	Value    []byte
	Flags    uint32
}

// Get performs a GET operation.
func (e *Engine) Get(opts GetOptions) (*GetResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	doc, err := e.db.Get(0, opts.Vbucket, opts.CollectionID, opts.Key)
	if err == mockdb.ErrDocNotFound {
		return nil, ErrDocNotFound
	} else if err != nil {
		return nil, err
	}

	if doc.IsDeleted {
		return nil, ErrDocNotFound
	}

	if e.docIsLocked(doc) {
		// If the doc is locked, we return -1 as the CAS instead.
		doc.Cas = lockedCas
	}

	return &GetResult{
		Cas:      doc.Cas,
		Datatype: doc.Datatype,
		Value:    doc.Value,
		Flags:    doc.Flags,
	}, nil
}

// GetMetaOptions specifies options for a GET_META operation.
type GetMetaOptions struct {
	Vbucket      uint
	CollectionID uint
	Key          []byte
}

// GetMetaResult contains the results of a GET_META operation.
type GetMetaResult struct {
	Cas       uint64
	Datatype  uint8
	Flags     uint32
	IsDeleted bool
	ExpTime   time.Time
	RevID     uint64
}

// GetMeta performs a GET_META operation.
func (e *Engine) GetMeta(opts GetMetaOptions) (*GetMetaResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	doc, err := e.db.GetMeta(opts.Vbucket, opts.CollectionID, opts.Key)
	if err == mockdb.ErrDocNotFound {
		return nil, ErrDocNotFound
	} else if err != nil {
		return nil, err
	}

	if e.docIsLocked(doc) {
		// If the doc is locked, we return -1 as the CAS instead.
		doc.Cas = lockedCas
	}

	// kv_engine reports the datatype as stored, with the xattr bit.
	datatype := doc.Datatype
	if len(doc.Xattrs) > 0 {
		datatype |= uint8(memd.DatatypeFlagXattrs)
	}

	return &GetMetaResult{
		Cas:       doc.Cas,
		Datatype:  datatype,
		Flags:     doc.Flags,
		IsDeleted: doc.IsDeleted,
		ExpTime:   doc.Expiry,
		RevID:     doc.RevID,
	}, nil
}

// GetRandomOptions specifies options for a GET_RANDOM operation.
type GetRandomOptions struct {
	CollectionID uint
}

// GetRandomResult contains the results of a GET_RANDOM operation.
type GetRandomResult struct {
	Cas      uint64
	Datatype uint8
	Value    []byte
	Flags    uint32
	Key      []byte
}

// GetRandom performs a GET_RANDOM operation. kv_engine looks in the vbuckets
// active on the node, from a random one (KVBucket::getRandomKey).
func (e *Engine) GetRandom(opts GetRandomOptions) (*GetRandomResult, error) {
	var doc *mockdb.Document
	numVbuckets := len(e.vbOwnership)
	start := rand.Intn(numVbuckets)
	for i := 0; i < numVbuckets && doc == nil; i++ {
		vbIdx := (start + i) % numVbuckets
		if e.vbOwnership[vbIdx] == 0 {
			doc = e.db.GetVbucket(uint(vbIdx)).GetRandom(0, opts.CollectionID)
		}
	}
	if doc == nil {
		return nil, ErrDocNotFound
	}

	return &GetRandomResult{
		Cas:      doc.Cas,
		Datatype: doc.Datatype,
		Value:    doc.Value,
		Flags:    doc.Flags,
		Key:      doc.Key,
	}, nil
}

// GetReplica performs a GET_REPLICA operation.
func (e *Engine) GetReplica(opts GetOptions) (*GetResult, error) {
	repIdx := e.findReplicaIdx(opts.Vbucket)
	if repIdx < 1 {
		return nil, ErrNotMyVbucket
	}

	doc, err := e.db.Get(uint(repIdx), opts.Vbucket, opts.CollectionID, opts.Key)
	if err == mockdb.ErrDocNotFound {
		return nil, ErrDocNotFound
	} else if err != nil {
		return nil, err
	}

	if doc.IsDeleted {
		return nil, ErrDocNotFound
	}

	return &GetResult{
		Cas:      doc.Cas,
		Datatype: doc.Datatype,
		Value:    doc.Value,
		Flags:    doc.Flags,
	}, nil
}

// StoreOptions specifies options for various store operations.
type StoreOptions struct {
	Vbucket      uint
	CollectionID uint
	Key          []byte
	Cas          uint64
	Datatype     uint8
	Value        []byte
	Flags        uint32
	Expiry       uint32
	// PreserveExpiry keeps the expiry of an existing document instead of
	// replacing it with Expiry.
	PreserveExpiry bool
}

// StoreResult contains the results for various store operations.
type StoreResult struct {
	Cas    uint64
	VbUUID uint64
	SeqNo  uint64
}

// Add performs an ADD operation.
func (e *Engine) Add(opts StoreOptions) (*StoreResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}
	if e.durabilityErr != nil {
		return nil, e.durabilityErr
	}

	doc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
		Value:        opts.Value,
		Flags:        opts.Flags,
		Datatype:     SetDatatypeJSONFromValue(opts.Datatype, opts.Value),
		Expiry:       e.parseExpiry(opts.Expiry),
		Cas:          mockdb.GenerateNewCas(e.HLC()),
	}

	newDoc, err := e.insert(doc)

	if err == mockdb.ErrDocExists {
		return nil, ErrDocExists
	} else if err == mockdb.ErrValueTooBig {
		return nil, ErrValueTooBig
	} else if err == mockdb.ErrSyncWriteInProgress {
		return nil, err
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in an ADD.
		return nil, ErrInternal
	}

	return &StoreResult{
		Cas:    newDoc.Cas,
		VbUUID: newDoc.VbUUID,
		SeqNo:  newDoc.SeqNo,
	}, nil
}

// Set performs an SET operation.
func (e *Engine) Set(opts StoreOptions) (*StoreResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}
	if e.durabilityErr != nil {
		return nil, e.durabilityErr
	}

	doc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
		Value:        opts.Value,
		Flags:        opts.Flags,
		Datatype:     SetDatatypeJSONFromValue(opts.Datatype, opts.Value),
		Expiry:       e.parseExpiry(opts.Expiry),
		Cas:          mockdb.GenerateNewCas(e.HLC()),
	}

	newDoc, err := e.overwrite(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if opts.Cas != 0 {
				if idoc == nil || idoc.IsDeleted {
					// The user specified a CAS and the document didn't exist.
					return nil, ErrDocNotFound
				}

				if e.docIsLocked(idoc) && idoc.Cas != opts.Cas {
					return nil, ErrLocked
				}

				if idoc.Cas != opts.Cas {
					return nil, ErrCasMismatch
				}
			} else {
				// Insert the document if it wasn't already existing.
				if idoc == nil {
					return doc, nil
				}

				if e.docIsLocked(idoc) {
					return nil, ErrLocked
				}
			}

			// Otherwise we simply update the value
			if !opts.PreserveExpiry || idoc.IsDeleted {
				idoc.Expiry = doc.Expiry
			}
			idoc.IsDeleted = false
			idoc.Value = doc.Value
			idoc.Flags = doc.Flags
			idoc.Datatype = doc.Datatype
			idoc.LockExpiry = doc.LockExpiry
			idoc.Cas = doc.Cas
			return idoc, nil
		})
	if err == mockdb.ErrValueTooBig {
		return nil, ErrValueTooBig
	} else if err != nil {
		return nil, err
	}

	return &StoreResult{
		Cas:    newDoc.Cas,
		VbUUID: newDoc.VbUUID,
		SeqNo:  newDoc.SeqNo,
	}, nil
}

// Replace performs an REPLACE operation.
func (e *Engine) Replace(opts StoreOptions) (*StoreResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}
	if e.durabilityErr != nil {
		return nil, e.durabilityErr
	}

	doc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
		Value:        opts.Value,
		Flags:        opts.Flags,
		Datatype:     SetDatatypeJSONFromValue(opts.Datatype, opts.Value),
		Expiry:       e.parseExpiry(opts.Expiry),
		Cas:          mockdb.GenerateNewCas(e.HLC()),
	}

	newDoc, err := e.updateIfFound(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if idoc == nil || idoc.IsDeleted {
				return nil, ErrDocNotFound
			}

			if e.docIsLocked(idoc) && idoc.Cas != opts.Cas {
				return nil, ErrLocked
			}

			if opts.Cas != 0 && idoc.Cas != opts.Cas {
				return nil, ErrCasMismatch
			}

			// Otherwise we simply update the value
			idoc.Value = doc.Value
			idoc.Flags = doc.Flags
			idoc.Datatype = doc.Datatype
			if !opts.PreserveExpiry {
				idoc.Expiry = doc.Expiry
			}
			idoc.LockExpiry = doc.LockExpiry
			idoc.Cas = doc.Cas
			return idoc, nil
		})
	if err == mockdb.ErrValueTooBig {
		return nil, ErrValueTooBig
	} else if err != nil {
		return nil, err
	}

	return &StoreResult{
		Cas:    newDoc.Cas,
		VbUUID: newDoc.VbUUID,
		SeqNo:  newDoc.SeqNo,
	}, nil
}

// DeleteOptions specifies options for a DELETE operation.
type DeleteOptions struct {
	Vbucket      uint
	CollectionID uint
	Key          []byte
	Cas          uint64
}

// DeleteResult contains the results of a DELETE operation.
type DeleteResult struct {
	Cas    uint64
	VbUUID uint64
	SeqNo  uint64
}

// Delete performs an DELETE operation.
func (e *Engine) Delete(opts DeleteOptions) (*DeleteResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}
	if e.durabilityErr != nil {
		return nil, e.durabilityErr
	}

	lkpDoc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
	}

	newDoc, err := e.update(
		lkpDoc.VbID, lkpDoc.CollectionID, lkpDoc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if idoc == nil || idoc.IsDeleted {
				return nil, ErrDocNotFound
			}

			if e.docIsLocked(idoc) && idoc.Cas != opts.Cas {
				return nil, ErrLocked
			}

			if opts.Cas != 0 && idoc.Cas != opts.Cas {
				return nil, ErrCasMismatch
			}

			// TODO(brett19): Check if DELETE generates a new CAS or not.

			idoc.IsDeleted = true
			idoc.LockExpiry = time.Time{}
			idoc.Cas = mockdb.GenerateNewCas(e.HLC())
			idoc.Value = []byte{}
			idoc.Datatype &^= uint8(memd.DatatypeFlagJSON)
			// We need to keep the system xattrs, i.e. those which start with an _.
			for key := range idoc.Xattrs {
				if !strings.HasPrefix(key, "_") {
					idoc.RemoveXattr(key)
				}
			}
			// kv_engine stores a deletion that keeps system xattrs with no
			// flags and no expiry (RemoveCommandContext::allocateDeletedItem,
			// MB-25273); any other it removes, keeping them
			// (StoredValue::deleteImpl).
			if len(idoc.Xattrs) > 0 {
				idoc.Flags = 0
				idoc.Expiry = time.Time{}
			} else {
				idoc.SoftDelete = true
			}
			return idoc, nil
		})
	if err != nil {
		return nil, err
	}

	return &DeleteResult{
		Cas:    newDoc.Cas,
		VbUUID: newDoc.VbUUID,
		SeqNo:  newDoc.SeqNo,
	}, nil
}

// CounterOptions specifies options for a INCREMENT or DECREMENT operation.
type CounterOptions struct {
	Vbucket      uint
	CollectionID uint
	Key          []byte
	Cas          uint64
	Initial      uint64
	Delta        uint64
	Expiry       uint32
}

// CounterResult contains the results of a INCREMENT or DECREMENT operation.
type CounterResult struct {
	Cas    uint64
	Value  uint64
	VbUUID uint64
	SeqNo  uint64
}

func (e *Engine) counter(opts CounterOptions, isIncr bool) (*CounterResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	doc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
		Value:        []byte(fmt.Sprintf("%d", opts.Initial)),
		Flags:        0,
		// kv_engine stores a counter's value as JSON.
		Datatype: uint8(memd.DatatypeFlagJSON),
		Expiry:   e.parseExpiry(opts.Expiry),
		Cas:      mockdb.GenerateNewCas(e.HLC()),
	}

	newDoc, err := e.updateAfterRead(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if idoc == nil || idoc.IsDeleted {
				if opts.Expiry == 0xffffffff {
					return nil, ErrDocNotFound
				}

				if opts.Cas != 0 {
					return nil, ErrCasMismatch
				}
				if e.durabilityErr != nil {
					return nil, e.durabilityErr
				}

				return doc, nil
			}

			// kv_engine compares a supplied CAS with the one a read reports,
			// which for a locked document is lockedCas. It then parses the
			// value before it stores the new one, so a value that is not a
			// number fails before the lock and durability checks.
			// The lock is read once, so the checks below agree on it.
			locked := e.docIsLocked(idoc)
			readCas := idoc.Cas
			if locked {
				readCas = lockedCas
			}
			if opts.Cas != 0 && opts.Cas != readCas {
				return nil, ErrCasMismatch
			}
			val, ok := parseCounter(idoc.Value)
			if !ok {
				return nil, ErrDeltaBadval
			}

			// The read-time checks passed, so the request stores.
			if idoc.SyncWriteInProgress {
				return nil, e.syncWriteStoreErr()
			}

			// kv_engine stores a counter on a locked document with the locked
			// CAS when the request has none, and checks durability first.
			if locked && opts.Cas == 0 && e.durabilityErr != nil {
				return nil, e.durabilityErr
			}

			if locked && idoc.Cas != opts.Cas {
				return nil, ErrLocked
			}

			// TODO(brett19): Double-check the saturation logic on the server...
			if isIncr {
				if val+opts.Delta < val {
					// overflow
					val = 0xffffffffffffffff
				} else {
					val += opts.Delta
				}
			} else {
				if opts.Delta > val {
					// underflow
					val = 0
				} else {
					val -= opts.Delta
				}
			}

			if e.durabilityErr != nil {
				return nil, e.durabilityErr
			}

			// The expiry of the request applies only when the counter is
			// created; kv_engine keeps the expiry and flags of an existing
			// counter.
			idoc.Value = []byte(fmt.Sprintf("%d", val))
			idoc.Datatype = doc.Datatype
			idoc.LockExpiry = doc.LockExpiry
			idoc.Cas = doc.Cas
			return idoc, nil
		})
	if err != nil {
		return nil, err
	}

	docValue, _ := strconv.ParseUint(string(newDoc.Value), 10, 64)

	return &CounterResult{
		Cas:    newDoc.Cas,
		Value:  docValue,
		VbUUID: newDoc.VbUUID,
		SeqNo:  newDoc.SeqNo,
	}, nil
}

// Increment performs an INCREMENT operation.
func (e *Engine) Increment(opts CounterOptions) (*CounterResult, error) {
	return e.counter(opts, true)
}

// Decrement performs an DECREMENT operation.
func (e *Engine) Decrement(opts CounterOptions) (*CounterResult, error) {
	return e.counter(opts, false)
}

func (e *Engine) adjoin(opts StoreOptions, isAppend bool) (*StoreResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	doc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
		Value:        opts.Value,
		Cas:          mockdb.GenerateNewCas(e.HLC()),
	}

	newDoc, err := e.updateAfterRead(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			// kv_engine answers APPEND and PREPEND on a missing document with
			// not_stored.
			if idoc == nil || idoc.IsDeleted {
				return nil, ErrNotStored
			}

			// kv_engine refuses a locked document without a CAS, and a CAS
			// that does not match an unlocked one, when it reads it; a request
			// that passes then stores.
			locked := e.docIsLocked(idoc)
			readFails := locked && opts.Cas == 0 || !locked && opts.Cas != 0 && idoc.Cas != opts.Cas
			if idoc.SyncWriteInProgress && !readFails {
				return nil, e.syncWriteStoreErr()
			}

			// kv_engine stores to a locked document with the CAS the request
			// carries, and checks durability first.
			if e.docIsLocked(idoc) && opts.Cas != 0 && e.durabilityErr != nil {
				return nil, e.durabilityErr
			}

			if e.docIsLocked(idoc) && idoc.Cas != opts.Cas {
				return nil, ErrLocked
			}

			if opts.Cas != 0 && idoc.Cas != opts.Cas {
				return nil, ErrCasMismatch
			}

			if e.durabilityErr != nil {
				return nil, e.durabilityErr
			}

			// Otherwise we simply update the value
			if isAppend {
				idoc.Value = append(idoc.Value, doc.Value...)
			} else {
				idoc.Value = append(doc.Value, idoc.Value...)
			}
			idoc.Datatype = SetDatatypeJSONFromValue(idoc.Datatype, idoc.Value)

			idoc.LockExpiry = doc.LockExpiry
			idoc.Cas = doc.Cas
			return idoc, nil
		})

	if err == mockdb.ErrValueTooBig {
		return nil, ErrValueTooBig
	} else if err != nil {
		return nil, err
	}

	return &StoreResult{
		Cas:    newDoc.Cas,
		VbUUID: newDoc.VbUUID,
		SeqNo:  newDoc.SeqNo,
	}, nil
}

// Append performs an APPEND operation.
func (e *Engine) Append(opts StoreOptions) (*StoreResult, error) {
	return e.adjoin(opts, true)
}

// Prepend performs an PREPEND operation.
func (e *Engine) Prepend(opts StoreOptions) (*StoreResult, error) {
	return e.adjoin(opts, false)
}

// TouchOptions specifies options for a TOUCH operation.
type TouchOptions struct {
	ReplicaIdx   int
	Vbucket      uint
	CollectionID uint
	Key          []byte
	Expiry       uint32
}

// Touch performs a GET_AND_TOUCH operation.
func (e *Engine) Touch(opts TouchOptions) (*StoreResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	doc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
		Expiry:       e.parseExpiry(opts.Expiry),
		Cas:          mockdb.GenerateNewCas(e.HLC()),
	}

	newDoc, err := e.db.Update(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if idoc == nil || idoc.IsDeleted {
				return nil, ErrDocNotFound
			}

			if e.docIsLocked(idoc) {
				return nil, ErrLocked
			}

			// Otherwise we simply update the value
			// kv_engine stores a mutation only when the expiry changes
			// (VBucket::processGetAndUpdateTtl).
			if idoc.Expiry.Unix() == doc.Expiry.Unix() && idoc.Expiry.IsZero() == doc.Expiry.IsZero() {
				idoc.InMemory = true
				return idoc, nil
			}
			idoc.Expiry = doc.Expiry
			idoc.LockExpiry = doc.LockExpiry
			idoc.Cas = doc.Cas
			return idoc, nil
		})
	if err != nil {
		return nil, err
	}

	return &StoreResult{
		Cas:    newDoc.Cas,
		VbUUID: newDoc.VbUUID,
		SeqNo:  newDoc.SeqNo,
	}, err
}

// GetAndTouchOptions specifies options for a GET_AND_TOUCH operation.
type GetAndTouchOptions struct {
	ReplicaIdx   int
	Vbucket      uint
	CollectionID uint
	Key          []byte
	Expiry       uint32
}

// GetAndTouch performs a GET_AND_TOUCH operation.
func (e *Engine) GetAndTouch(opts GetAndTouchOptions) (*GetResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	doc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
		Expiry:       e.parseExpiry(opts.Expiry),
		Cas:          mockdb.GenerateNewCas(e.HLC()),
	}

	newDoc, err := e.db.Update(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if idoc == nil || idoc.IsDeleted {
				return nil, ErrDocNotFound
			}

			if e.docIsLocked(idoc) {
				return nil, ErrLocked
			}

			// Otherwise we simply update the value
			// kv_engine stores a mutation only when the expiry changes
			// (VBucket::processGetAndUpdateTtl).
			if idoc.Expiry.Unix() == doc.Expiry.Unix() && idoc.Expiry.IsZero() == doc.Expiry.IsZero() {
				idoc.InMemory = true
				return idoc, nil
			}
			idoc.Expiry = doc.Expiry
			idoc.LockExpiry = doc.LockExpiry
			idoc.Cas = doc.Cas
			return idoc, nil
		})
	if err != nil {
		return nil, err
	}

	return &GetResult{
		Cas:      newDoc.Cas,
		Datatype: newDoc.Datatype,
		Value:    newDoc.Value,
		Flags:    newDoc.Flags,
	}, nil
}

// GetLockedOptions specifies options for a GET_LOCKED operation.
type GetLockedOptions struct {
	ReplicaIdx   int
	Vbucket      uint
	CollectionID uint
	Key          []byte
	LockTime     uint32
}

// GetLocked performs a GET_LOCKED operation.
func (e *Engine) GetLocked(opts GetLockedOptions) (*GetResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	// kv_engine locks for getl_default_timeout, 15 seconds, when the request
	// gives no time or one above getl_max_timeout, 30 seconds
	// (EventuallyPersistentEngine::getLockedInner).
	if opts.LockTime == 0 || opts.LockTime > 30 {
		opts.LockTime = 15
	}

	lockDura := time.Duration(opts.LockTime) * time.Second
	lockExpiryTime := e.db.Chrono().Now().Add(lockDura)

	lkpDoc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
	}
	doc, err := e.db.Update(
		lkpDoc.VbID, lkpDoc.CollectionID, lkpDoc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if idoc == nil || idoc.IsDeleted {
				return nil, ErrDocNotFound
			}

			if e.docIsLocked(idoc) {
				return nil, ErrLocked
			}

			// kv_engine locks a document in memory with a new CAS, without
			// a mutation (VBucket::getLocked).
			idoc.LockExpiry = lockExpiryTime
			idoc.Cas = mockdb.GenerateNewCas(e.HLC())
			idoc.InMemory = true
			return idoc, nil
		})
	if err != nil {
		return nil, err
	}

	return &GetResult{
		Cas:      doc.Cas,
		Datatype: doc.Datatype,
		Value:    doc.Value,
		Flags:    doc.Flags,
	}, nil
}

// UnlockOptions specifies options for an UNLOCK operation.
type UnlockOptions struct {
	ReplicaIdx   int
	Vbucket      uint
	CollectionID uint
	Key          []byte
	Cas          uint64
}

// Unlock performs an UNLOCK operation.
func (e *Engine) Unlock(opts UnlockOptions) (*StoreResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	doc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
	}

	newDoc, err := e.db.Update(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if idoc == nil || idoc.IsDeleted {
				return nil, ErrDocNotFound
			}

			if !e.docIsLocked(idoc) {
				return nil, ErrNotLocked
			}

			if idoc.Cas != opts.Cas {
				return nil, ErrLocked
			}

			// Otherwise we simply mark it as no longer locked.
			idoc.LockExpiry = time.Time{}
			// kv_engine unlocks it in memory and keeps the CAS the lock
			// gave it (KVBucket::unlockKey).
			idoc.InMemory = true
			return idoc, nil
		})
	if err != nil {
		return nil, err
	}

	return &StoreResult{
		Cas:    newDoc.Cas,
		VbUUID: newDoc.VbUUID,
		SeqNo:  newDoc.SeqNo,
	}, nil
}

// SubDocOp represents one sub-document operation.
type SubDocOp struct {
	Op           memd.SubDocOpType
	Path         string
	Value        []byte
	CreatePath   bool
	IsXattrPath  bool
	ExpandMacros bool
}

// SubDocResult represents one result from a sub-document operation.
type SubDocResult struct {
	Value []byte
	Err   error
}

// MultiLookupOptions specifies options for an SD_MULTILOOKUP operation.
type MultiLookupOptions struct {
	Vbucket        uint
	CollectionID   uint
	Key            []byte
	Ops            []*SubDocOp
	AccessDeleted  bool
	ReviveDocument bool
	Cas            uint64
}

// MultiLookupResult contains the results of a SD_MULTILOOKUP operation.
type MultiLookupResult struct {
	Cas       uint64
	Ops       []*SubDocResult
	IsDeleted bool
}

// MultiLookup performs an SD_MULTILOOKUP operation.
func (e *Engine) MultiLookup(opts MultiLookupOptions) (*MultiLookupResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	// A read of deleted documents fetches a persisted deletion from disk.
	if opts.AccessDeleted {
		e.db.RestoreDeleted(opts.Vbucket, opts.CollectionID, opts.Key)
	}
	doc, err := e.db.Get(0, opts.Vbucket, opts.CollectionID, opts.Key)
	if err == mockdb.ErrDocNotFound || (doc.IsDeleted && !opts.AccessDeleted) {
		return nil, ErrDocNotFound
	} else if err != nil {
		return nil, err
	}

	// A locked document is read only with a CAS other than the one a GET
	// reports for it, which is not compared; otherwise a CAS must match
	// (SubdocCmdContext::get_document_for_searching).
	if e.docIsLocked(doc) {
		if opts.Cas == 0 || opts.Cas == lockedCas {
			return nil, ErrLocked
		}
	} else if opts.Cas != 0 && opts.Cas != doc.Cas {
		return nil, ErrCasMismatch
	}

	if opts.ReviveDocument && !doc.IsDeleted {
		return nil, ErrSdCanOnlyReviveDeletedDocuments
	}

	// The specs see the CAS a read reports, which for a locked document is
	// lockedCas (SubdocCmdContext::get_document_for_searching).
	if e.docIsLocked(doc) {
		doc.Cas = lockedCas
	}
	sdRes, err := e.executeSdOps(doc, doc, opts.Ops, true, false)
	if err != nil {
		return nil, err
	}

	// The response carries the CAS of the request, or the one read.
	cas := doc.Cas
	if opts.Cas != 0 {
		cas = opts.Cas
	}
	return &MultiLookupResult{
		Cas:       cas,
		Ops:       sdRes,
		IsDeleted: doc.IsDeleted,
	}, nil
}

// MultiMutateOptions specifies options for an SD_MULTIMUTATE operation.
type MultiMutateOptions struct {
	Vbucket         uint
	CollectionID    uint
	Key             []byte
	Ops             []*SubDocOp
	AccessDeleted   bool
	CreateAsDeleted bool
	CreateIfMissing bool
	CreateOnly      bool
	ReviveDocument  bool
	Expiry          uint32
	PreserveExpiry  bool
	Cas             uint64
}

// MultiMutateResult contains the results of a SD_MULTIMUTATE operation.
type MultiMutateResult struct {
	Cas    uint64
	Ops    []*SubDocResult
	VbUUID uint64
	SeqNo  uint64
	// IsDeleted reports that the specs ran on a deleted document, which
	// kv_engine answers with SubdocSuccessDeleted.
	IsDeleted bool
}

// MultiMutate performs an SD_MULTIMUTATE operation.
// afterMultiMutateRead runs between MultiMutate's checks of the document it
// read and its update. Tests use it to change the document in that window.
var afterMultiMutateRead = func() {}

func (e *Engine) MultiMutate(opts MultiMutateOptions) (*MultiMutateResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	// Some doc options imply path options.
	if opts.CreateIfMissing || opts.CreateOnly {
		for opIdx := range opts.Ops {
			opts.Ops[opIdx].CreatePath = true
		}
	}

	for attemptIdx := 0; attemptIdx < 10; attemptIdx++ {
		mdoc := &mockdb.Document{
			VbID:         opts.Vbucket,
			CollectionID: opts.CollectionID,
			Key:          opts.Key,
			Value:        nil,
			Cas:          0,
			Xattrs:       make(map[string][]byte),
			Expiry:       e.parseExpiry(opts.Expiry),
			IsDeleted:    opts.CreateAsDeleted,
		}

		// kv_engine creates a document from a JSON template whose root it
		// takes from the path of the first sub-document spec, xattr or not
		// (subdocument_context.cc@v7.0.5); one created deleted gets none.
		xattrUpdateOnly := true
		rootChosen := opts.CreateAsDeleted
		for _, op := range opts.Ops {
			if !op.IsXattrPath {
				xattrUpdateOnly = false
			}
			if !rootChosen && subdocOpIsSubJSON(op) {
				rootChosen = true
				mdoc.Value = subdocRootTemplate(op)
			}
		}
		if mdoc.Value != nil {
			mdoc.Datatype = uint8(memd.DatatypeFlagJSON)
		}

		if opts.AccessDeleted {
			e.db.RestoreDeleted(mdoc.VbID, mdoc.CollectionID, mdoc.Key)
		}
		doc, err := e.db.Get(0, mdoc.VbID, mdoc.CollectionID, mdoc.Key)
		if err == mockdb.ErrDocNotFound {
			if !opts.CreateIfMissing && !opts.CreateOnly {
				return nil, ErrDocNotFound
			}
		} else if err != nil {
			return nil, err
		}
		// The update below is valid only for the document as read here; doc
		// itself is replaced below for a create.
		readDeleted := doc != nil && doc.IsDeleted
		// kv_engine reads with DocStateFilter::Alive without AccessDeleted.
		visible := doc != nil && (!doc.IsDeleted || opts.AccessDeleted)

		// kv_engine refuses an Add of an existing document before it looks at
		// the CAS or the lock. With AccessDeleted a deleted document exists.
		if opts.CreateOnly && visible {
			return nil, ErrDocExists
		}

		// A locked document is refused only without a CAS, or with the CAS a
		// GET reports for it; a supplied CAS is checked against the lock when
		// the mutation is stored, as in kv_engine.
		locked := e.docIsLocked(doc)
		if locked && (opts.Cas == 0 || opts.Cas == lockedCas) {
			return nil, ErrLocked
		}

		// Check for cas mismatch before we do any work. We'll effectively be doing this check again during the update
		// too so if anyone performs a mutation during that time then we'll catch it there too.
		// kv_engine compares the CAS only with a document it read: one it
		// creates is stored with a zero CAS (subdocument.cc, in_cas).
		if !locked && opts.Cas != 0 && (visible || doc == nil) && doc.Cas != opts.Cas && !(doc.IsDeleted && !xattrUpdateOnly) {
			return nil, ErrCasMismatch
		}

		// For Mkdoc or Add, kv_engine creates a document it does not see
		// afresh, a deleted or expired one too: from the template, with no
		// xattrs or flags, and deleted with CreateAsDeleted.
		created := !visible
		if created {
			if !opts.CreateIfMissing && !opts.CreateOnly {
				return nil, ErrDocNotFound
			}
			if doc == nil {
				doc = mdoc
			} else {
				doc.Value = mdoc.Value
				doc.Datatype = mdoc.Datatype
				doc.Xattrs = mdoc.Xattrs
				doc.XattrOrder = nil
				doc.Flags = 0
				doc.IsDeleted = mdoc.IsDeleted
				doc.Expiry = time.Time{}
			}
		}

		// The response reports the state of the document the specs ran on,
		// not the state a DeleteDoc spec stores.
		deleted := doc.IsDeleted
		if opts.ReviveDocument && !deleted {
			return nil, ErrSdCanOnlyReviveDeletedDocuments
		}

		afterMultiMutateRead()

		newMetaDoc := &mockdb.Document{
			Cas: mockdb.GenerateNewCas(e.HLC()),
		}

		// The specs see the CAS a read reports, which for a locked document
		// is lockedCas, as kv_engine expands ${$document.CAS} from it.
		readCas := doc.Cas
		if locked {
			doc.Cas = lockedCas
		}
		sdRes, err := e.executeSdOps(doc, newMetaDoc, opts.Ops, false, created)
		doc.Cas = readCas
		if err != nil {
			return nil, err
		}

		// A failed spec fails the whole mutation, which must not be stored.
		for _, res := range sdRes {
			if res != nil && res.Err != nil {
				return &MultiMutateResult{Ops: sdRes}, nil
			}
		}

		deleteDoc := false
		for _, op := range opts.Ops {
			if op.Op != memd.SubDocOpDeleteDoc {
				continue
			}
			deleteDoc = true
			// kv_engine keeps only the system xattrs of a document a spec
			// deletes (do_xattr_delete_phase).
			for key := range doc.Xattrs {
				if !strings.HasPrefix(key, "_") {
					doc.RemoveXattr(key)
				}
			}
		}

		// A document a spec deletes with no system xattrs left is removed,
		// as DELETE removes one, before kv_engine looks at ReviveDocument
		// (subdoc_update).
		remove := deleteDoc && len(doc.Xattrs) == 0
		if !remove {
			// kv_engine stores a revived document live unless a spec deletes
			// it, and answers as for a live one.
			if opts.ReviveDocument {
				doc.IsDeleted = deleteDoc
				deleted = false
			}
			// A ReplaceBodyWithXattr spec can give a deleted document a body.
			if doc.IsDeleted && len(doc.Value) > 0 {
				return nil, ErrSdDeletedDocumentCantHaveValue
			}
		}

		// kv_engine checks durability when it stores, after the specs.
		if e.durabilityErr != nil {
			return nil, e.durabilityErr
		}

		// kv_engine stores a document it creates or revives with Add
		// semantics and a zero CAS (subdoc_update). When the Add fails, a
		// request with Mkdoc retries, unless it carries a CAS, and any other
		// answers not_stored.
		addFailed := func() error {
			switch {
			case !opts.CreateIfMissing:
				return ErrNotStored
			case opts.Cas != 0:
				return ErrDocExists
			default:
				return ErrCasMismatch
			}
		}

		newDoc, err := e.updateAfterRead(
			doc.VbID, doc.CollectionID, doc.Key,
			func(idoc *mockdb.Document) (*mockdb.Document, error) {
				// The specs ran on the committed document; kv_engine refuses
				// the store of a key with a durable write in progress before
				// its lock check.
				if idoc != nil && idoc.SyncWriteInProgress {
					return nil, mockdb.ErrSyncWriteInProgress
				}
				if remove {
					// bucket_remove with the CAS read: a deleted document is
					// not found, and the removal keeps the flags and the
					// expiry.
					if idoc == nil || idoc.IsDeleted {
						return nil, ErrDocNotFound
					}
					if e.docIsLocked(idoc) && idoc.Cas != opts.Cas {
						return nil, ErrLocked
					}
					if idoc.Cas != doc.Cas {
						return nil, ErrCasMismatch
					}
					idoc.Value = nil
					idoc.Xattrs = doc.Xattrs
					idoc.XattrOrder = doc.XattrOrder
					idoc.Datatype = 0
					idoc.IsDeleted = true
					idoc.SoftDelete = true
					idoc.LockExpiry = time.Time{}
					idoc.Cas = newMetaDoc.Cas
					return idoc, nil
				}
				if created || opts.ReviveDocument {
					// An Add stores a live document over a deleted one only,
					// and a deleted one over none (VBucket::processAdd).
					if idoc != nil && (!idoc.IsDeleted || doc.IsDeleted) {
						return nil, addFailed()
					}
				} else {
					if idoc == nil {
						return nil, ErrDocNotFound
					}
					if e.docIsLocked(idoc) && idoc.Cas != opts.Cas {
						return nil, ErrLocked
					}
					// The CAS of a request for a document locked when it was
					// read is checked here. If the lock has since gone, the
					// retry reads the document unlocked and reports the
					// mismatch.
					if locked && idoc.Cas != opts.Cas {
						return nil, ErrCasMismatch
					}

					// A document that expired or was deleted since it was read
					// keeps its CAS; retry, so the read sees it as it is now.
					if idoc.Cas != doc.Cas || idoc.IsDeleted != readDeleted {
						return nil, ErrCasMismatch
					}
				}

				if idoc == nil {
					idoc = doc
				} else {
					// A mutation takes the expiry of the request, and a request
					// without one clears it. PreserveExpiry keeps the expiry of
					// the document read, deleted or expired as well, even when
					// it has passed; a created document takes the request's
					// (subdoc_executor, fetchedItem).
					if !opts.PreserveExpiry || created {
						idoc.Expiry = mdoc.Expiry
					} else {
						idoc.Expiry = doc.Expiry
					}
					idoc.Value = doc.Value
					idoc.Flags = doc.Flags
					idoc.Xattrs = doc.Xattrs
					idoc.XattrOrder = doc.XattrOrder
					idoc.Datatype = doc.Datatype
					idoc.IsDeleted = doc.IsDeleted
				}

				idoc.LockExpiry = newMetaDoc.LockExpiry
				idoc.Cas = newMetaDoc.Cas
				idoc.SeqNoMacro = SeqNoPlaceholder
				return idoc, nil
			})
		// The Add of a deleted document an EP bucket does not hold in memory
		// fetches the key from disk, which brings back a persisted deletion
		// even when the Add then fails (VBucket::processAdd,
		// AddTmpAndBgFetch). A durable write in progress refuses the store
		// before that (VBucket::add). It is restored here, after the store,
		// as the store holds the vbucket; the Add fails over any deletion,
		// so the order changes nothing.
		if created && mdoc.IsDeleted && err != mockdb.ErrSyncWriteInProgress {
			e.db.RestoreDeleted(mdoc.VbID, mdoc.CollectionID, mdoc.Key)
		}
		if err == ErrCasMismatch {
			continue
		} else if err == mockdb.ErrValueTooBig {
			return nil, ErrValueTooBig
		} else if err != nil {
			return nil, err
		}

		return &MultiMutateResult{
			Cas:       newDoc.Cas,
			Ops:       sdRes,
			VbUUID:    newDoc.VbUUID,
			SeqNo:     newDoc.SeqNo,
			IsDeleted: deleted,
		}, nil
	}

	return nil, ErrSdToManyTries
}

// ObserveSeqNoOptions specifies options for an OBSERVE_SEQNO operation.
type ObserveSeqNoOptions struct {
	Vbucket uint
	VbUUID  uint64
}

// ObserveSeqNoResult contains the results of a OBSERVE_SEQNO operation.
type ObserveSeqNoResult struct {
	VbUUID       uint64
	CurrentSeqNo uint64
	PersistSeqNo uint64
	// FailedOver reports that the request named an earlier history of the
	// vbucket, which ended at LastSeqNo.
	FailedOver bool
	LastSeqNo  uint64
}

// ObserveSeqNo performs an OBSERVE_SEQNO operation.
func (e *Engine) ObserveSeqNo(opts ObserveSeqNoOptions) (*ObserveSeqNoResult, error) {
	repIdx := e.findReplicaIdx(opts.Vbucket)
	if repIdx == -1 {
		return nil, ErrNotMyVbucket
	}

	vbucket := e.db.GetVbucket(opts.Vbucket)
	metaState := vbucket.CurrentMetaState(uint(repIdx))
	res := &ObserveSeqNoResult{
		VbUUID:       metaState.VbUUID,
		CurrentSeqNo: metaState.CurrentSeqNo,
		PersistSeqNo: metaState.PersistSeqNo,
	}

	// kv_engine answers an unknown vbucket UUID with no_such_key
	// (EventuallyPersistentEngine::observe_seqno).
	if opts.VbUUID != metaState.VbUUID {
		lastSeqNo, ok := vbucket.LastSeqNoForUUID(opts.VbUUID)
		if !ok {
			return nil, ErrDocNotFound
		}
		res.FailedOver, res.LastSeqNo = true, lastSeqNo
	}
	return res, nil
}
