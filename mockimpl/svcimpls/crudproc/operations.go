package crudproc

import (
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mockdb"
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
		doc.Cas = 0xFFFFFFFFFFFFFFFF
	}

	return &GetResult{
		Cas:      doc.Cas,
		Datatype: doc.Datatype,
		Value:    doc.Value,
		Flags:    doc.Flags,
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
}

// StoreResult contains the results for various store operations.
type StoreResult struct {
	Cas uint64
}

// Add performs an ADD operation.
func (e *Engine) Add(opts StoreOptions) (*StoreResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	doc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
		Value:        opts.Value,
		Flags:        opts.Flags,
		Datatype:     opts.Datatype,
		Expiry:       e.parseExpiry(opts.Expiry),
		Cas:          mockdb.GenerateNewCas(),
	}

	newDoc, err := e.db.Insert(doc)

	if err == mockdb.ErrDocExists {
		return nil, ErrDocExists
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in an ADD.
		return nil, ErrInternal
	}

	// TODO(brett19): Return mutation tokens with ADD responses.
	return &StoreResult{
		Cas: newDoc.Cas,
	}, nil
}

// Set performs an SET operation.
func (e *Engine) Set(opts StoreOptions) (*StoreResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	doc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
		Value:        opts.Value,
		Flags:        opts.Flags,
		Datatype:     opts.Datatype,
		Expiry:       e.parseExpiry(opts.Expiry),
		Cas:          mockdb.GenerateNewCas(),
	}

	newDoc, err := e.db.Update(
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
			idoc.IsDeleted = false
			idoc.Value = doc.Value
			idoc.Flags = doc.Flags
			idoc.Datatype = doc.Datatype
			idoc.Expiry = doc.Expiry
			idoc.LockExpiry = doc.LockExpiry
			idoc.Cas = doc.Cas
			return idoc, nil
		})
	if err != nil {
		return nil, err
	}

	// TODO(brett19): Return mutation tokens with SET responses.
	return &StoreResult{
		Cas: newDoc.Cas,
	}, nil
}

// Replace performs an REPLACE operation.
func (e *Engine) Replace(opts StoreOptions) (*StoreResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	doc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
		Value:        opts.Value,
		Flags:        opts.Flags,
		Datatype:     opts.Datatype,
		Expiry:       e.parseExpiry(opts.Expiry),
		Cas:          mockdb.GenerateNewCas(),
	}

	newDoc, err := e.db.Update(
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
			idoc.Expiry = doc.Expiry
			idoc.LockExpiry = doc.LockExpiry
			idoc.Cas = doc.Cas
			return idoc, nil
		})
	if err != nil {
		return nil, err
	}

	// TODO(brett19): Return mutation tokens with REPLACE responses.
	return &StoreResult{
		Cas: newDoc.Cas,
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
	Cas uint64
}

// Delete performs an DELETE operation.
func (e *Engine) Delete(opts DeleteOptions) (*DeleteResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	lkpDoc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
	}

	newDoc, err := e.db.Update(
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

			// Otherwise we simply update the value
			idoc.IsDeleted = true
			idoc.LockExpiry = time.Time{}
			idoc.Cas = mockdb.GenerateNewCas()
			return idoc, nil
		})
	if err != nil {
		return nil, err
	}

	// TODO(brett19): Return mutation tokens with DELETE responses.
	return &DeleteResult{
		Cas: newDoc.Cas,
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
	Cas   uint64
	Value uint64
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
		Datatype:     0,
		Expiry:       e.parseExpiry(opts.Expiry),
		Cas:          mockdb.GenerateNewCas(),
	}

	newDoc, err := e.db.Update(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if idoc == nil || idoc.IsDeleted {
				if opts.Expiry == 0xffffffff {
					return nil, ErrDocNotFound
				}

				if opts.Cas != 0 {
					return nil, ErrCasMismatch
				}

				return doc, nil
			}

			if e.docIsLocked(idoc) && idoc.Cas != opts.Cas {
				return nil, ErrLocked
			}

			if opts.Cas != 0 && idoc.Cas != opts.Cas {
				return nil, ErrCasMismatch
			}

			// Otherwise we simply update the value
			val, err := strconv.ParseUint(string(idoc.Value), 10, 64)
			if err != nil {
				return nil, err
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

			idoc.Value = []byte(fmt.Sprintf("%d", val))
			idoc.Flags = doc.Flags
			idoc.Datatype = doc.Datatype
			idoc.Expiry = doc.Expiry
			idoc.LockExpiry = doc.LockExpiry
			idoc.Cas = doc.Cas
			return idoc, nil
		})
	if err != nil {
		return nil, err
	}

	docValue, _ := strconv.ParseUint(string(newDoc.Value), 10, 64)

	// TODO(brett19): Return mutation tokens with a INCREMENT/DECREMENT responses.
	return &CounterResult{
		Cas:   newDoc.Cas,
		Value: docValue,
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
		Cas:          mockdb.GenerateNewCas(),
	}

	newDoc, err := e.db.Update(
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
			if isAppend {
				idoc.Value = append(idoc.Value, doc.Value...)
			} else {
				idoc.Value = append(doc.Value, idoc.Value...)
			}

			idoc.LockExpiry = doc.LockExpiry
			idoc.Cas = doc.Cas
			return idoc, nil
		})
	if err != nil {
		return nil, err
	}

	// TODO(brett19): Return mutation tokens with APPEND/PREPEND responses.
	return &StoreResult{
		Cas: newDoc.Cas,
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

	// We cheat for now.
	resp, err := e.GetAndTouch(GetAndTouchOptions{
		ReplicaIdx:   opts.ReplicaIdx,
		Vbucket:      opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
		Expiry:       opts.Expiry,
	})
	if err != nil {
		return nil, err
	}

	// TODO(brett19): Return mutation tokens with TOUCH responses.
	return &StoreResult{
		Cas: resp.Cas,
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
		Cas:          mockdb.GenerateNewCas(),
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
			idoc.Expiry = doc.Expiry
			idoc.LockExpiry = doc.LockExpiry
			idoc.Cas = doc.Cas
			return idoc, nil
		})
	if err != nil {
		return nil, err
	}

	// TODO(brett19): Return mutation tokens with GET_AND_TOUCH responses.
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

	if opts.LockTime == 0 {
		// TODO(brett19): Confirm this default lock time is correct.
		opts.LockTime = 30
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

			idoc.LockExpiry = lockExpiryTime
			idoc.Cas = mockdb.GenerateNewCas()
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
				return nil, ErrCasMismatch
			}

			// Otherwise we simply mark it as no longer locked.
			idoc.LockExpiry = time.Time{}
			// We intentionally do not update the CAS here as locking has
			// already changed it and nobody can see it until unlock anyways.
			return idoc, nil
		})
	if err != nil {
		return nil, err
	}

	// TODO(brett19): Return mutation tokens with UNLOCK responses.
	return &StoreResult{
		Cas: newDoc.Cas,
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
	Vbucket       uint
	CollectionID  uint
	Key           []byte
	Ops           []*SubDocOp
	AccessDeleted bool
}

// MultiLookupResult contains the results of a SD_MULTILOOKUP operation.
type MultiLookupResult struct {
	Cas uint64
	Ops []*SubDocResult
}

func (e *Engine) executeSdOps(doc, newMeta *mockdb.Document, ops []*SubDocOp) ([]*SubDocResult, error) {
	opReses := make([]*SubDocResult, len(ops))

	for opIdx, op := range ops {
		var opRes *SubDocResult
		var err error

		switch op.Op {
		case memd.SubDocOpGet:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpGet(op)
		case memd.SubDocOpExists:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpExists(op)
		case memd.SubDocOpGetCount:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpGetCount(op)
		case memd.SubDocOpGetDoc:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpGetDoc(op)

		case memd.SubDocOpDictAdd:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpDictAdd(op)
		case memd.SubDocOpDictSet:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpDictSet(op)
		case memd.SubDocOpDelete:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpDelete(op)
		case memd.SubDocOpReplace:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpReplace(op)
		case memd.SubDocOpArrayPushLast:
		case memd.SubDocOpArrayPushFirst:
		case memd.SubDocOpArrayInsert:
		case memd.SubDocOpArrayAddUnique:
		case memd.SubDocOpCounter:
		case memd.SubDocOpSetDoc:
		case memd.SubDocOpAddDoc:
		case memd.SubDocOpDeleteDoc:
		}

		if err != nil {
			return nil, err
		}
		if opRes == nil {
			return nil, ErrInternal
		}

		opReses[opIdx] = opRes
	}

	return opReses, nil
}

// MultiLookup performs an SD_MULTILOOKUP operation.
func (e *Engine) MultiLookup(opts MultiLookupOptions) (*MultiLookupResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	doc, err := e.db.Get(0, opts.Vbucket, opts.CollectionID, opts.Key)
	if err == mockdb.ErrDocNotFound || doc.IsDeleted {
		return nil, ErrDocNotFound
	} else if err != nil {
		return nil, err
	}

	if e.docIsLocked(doc) {
		return nil, ErrLocked
	}

	sdRes, err := e.executeSdOps(doc, doc, opts.Ops)
	if err != nil {
		return nil, err
	}

	return &MultiLookupResult{
		Cas: doc.Cas,
		Ops: sdRes,
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
}

// MultiMutateResult contains the results of a SD_MULTIMUTATE operation.
type MultiMutateResult struct {
	Cas uint64
	Ops []*SubDocResult
}

// MultiMutate performs an SD_MULTIMUTATE operation.
func (e *Engine) MultiMutate(opts MultiMutateOptions) (*MultiMutateResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	for attemptIdx := 0; attemptIdx < 10; attemptIdx++ {
		mdoc := &mockdb.Document{
			VbID:         opts.Vbucket,
			CollectionID: opts.CollectionID,
			Key:          opts.Key,
			Value:        []byte(`{}`),
			Cas:          0,
		}

		doc, err := e.db.Get(0, mdoc.VbID, mdoc.CollectionID, mdoc.Key)
		if err == mockdb.ErrDocNotFound {
			if !opts.CreateIfMissing && !opts.CreateOnly {
				return nil, ErrDocNotFound
			}
		} else if err != nil {
			return nil, err
		}

		if opts.CreateOnly && doc != nil && !doc.IsDeleted {
			return nil, ErrDocExists
		}

		if opts.CreateIfMissing || opts.CreateOnly {
			if doc == nil {
				doc = mdoc
			} else if doc.IsDeleted {
				doc.IsDeleted = false
			}
		}

		if doc == nil {
			return nil, ErrDocNotFound
		}

		if doc.IsDeleted && !opts.AccessDeleted {
			return nil, ErrDocNotFound
		}

		if e.docIsLocked(doc) {
			return nil, ErrLocked
		}

		newMetaDoc := &mockdb.Document{
			Cas: mockdb.GenerateNewCas(),
		}

		sdRes, err := e.executeSdOps(doc, newMetaDoc, opts.Ops)
		if err != nil {
			return nil, err
		}

		newDoc, err := e.db.Update(
			doc.VbID, doc.CollectionID, doc.Key,
			func(idoc *mockdb.Document) (*mockdb.Document, error) {
				if idoc == nil {
					// Check if our source document existed or not
					if doc.Cas != 0 {
						// IsDeleted will be handled below as part of cas check.
						return nil, ErrDocNotFound
					}

					idoc = doc
				} else {
					if e.docIsLocked(idoc) {
						return nil, ErrLocked
					}

					if idoc.Cas != doc.Cas {
						return nil, ErrCasMismatch
					}

					idoc.Value = doc.Value
					idoc.Xattrs = doc.Xattrs
					idoc.IsDeleted = doc.IsDeleted
				}

				idoc.LockExpiry = newMetaDoc.LockExpiry
				idoc.Cas = newMetaDoc.Cas
				return idoc, nil
			})
		if err == ErrCasMismatch {
			continue
		} else if err != nil {
			return nil, err
		}

		return &MultiMutateResult{
			Cas: newDoc.Cas,
			Ops: sdRes,
		}, nil
	}

	return nil, ErrSdToManyTries
}
