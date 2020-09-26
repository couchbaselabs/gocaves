package crudproc

import (
	"strconv"
	"time"

	"github.com/couchbaselabs/gocaves/mockdb"
)

type GetOptions struct {
	Vbucket      uint
	CollectionID uint
	Key          []byte
}

type GetResult struct {
	Cas      uint64
	Datatype uint8
	Value    []byte
	Flags    uint32
}

func (e *Engine) Get(opts GetOptions) (*GetResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	doc, err := e.db.Get(0, opts.Vbucket, opts.CollectionID, opts.Key)
	if err == mockdb.ErrDocNotFound || doc.IsDeleted {
		return nil, ErrDocNotFound
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in a GET.
		return nil, ErrInternal
	}

	return &GetResult{
		Cas:      doc.Cas,
		Datatype: doc.Datatype,
		Value:    doc.Value,
		Flags:    doc.Flags,
	}, nil
}

func (e *Engine) GetReplica(opts GetOptions) (*GetResult, error) {
	repIdx := e.findReplicaIdx(opts.Vbucket)
	if repIdx < 1 {
		return nil, ErrNotMyVbucket
	}

	doc, err := e.db.Get(uint(repIdx), opts.Vbucket, opts.CollectionID, opts.Key)
	if err == mockdb.ErrDocNotFound || doc.IsDeleted {
		return nil, ErrDocNotFound
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in a GET.
		return nil, ErrInternal
	}

	return &GetResult{
		Cas:      doc.Cas,
		Datatype: doc.Datatype,
		Value:    doc.Value,
		Flags:    doc.Flags,
	}, nil
}

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

type StoreResult struct {
	Cas uint64
}

func (e *Engine) Add(opts StoreOptions) (*StoreResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	doc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
		Value:        opts.Value,
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

func (e *Engine) Set(opts StoreOptions) (*StoreResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	doc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
		Value:        opts.Value,
	}

	newDoc, err := e.db.Update(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if opts.Cas != 0 {
				if idoc == nil || idoc.IsDeleted {
					// The user specified a CAS and the document didn't exist.
					return nil, mockdb.ErrDocNotFound
				}

				if idoc.Cas != opts.Cas {
					return nil, mockdb.ErrDocExists
				}
			}

			// Insert the document if it wasn't already existing.
			if idoc == nil {
				return doc, nil
			}
			if idoc.IsDeleted {
				idoc.IsDeleted = false
			}

			// Otherwise we simply update the value
			idoc.Value = doc.Value
			return idoc, nil
		})

	if err == mockdb.ErrDocExists {
		return nil, ErrDocExists
	} else if err == mockdb.ErrDocNotFound {
		return nil, ErrDocNotFound
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in a SET.
		return nil, ErrInternal
	}

	// TODO(brett19): Return mutation tokens with SET responses.
	return &StoreResult{
		Cas: newDoc.Cas,
	}, nil
}

func (e *Engine) Replace(opts StoreOptions) (*StoreResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	doc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
		Value:        opts.Value,
	}

	newDoc, err := e.db.Update(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if idoc == nil || idoc.IsDeleted {
				return nil, mockdb.ErrDocNotFound
			}

			if idoc.Cas != opts.Cas {
				return nil, mockdb.ErrDocExists
			}

			// Otherwise we simply update the value
			idoc.Value = doc.Value
			return idoc, nil
		})

	if err == mockdb.ErrDocExists {
		return nil, ErrDocExists
	} else if err == mockdb.ErrDocNotFound {
		return nil, ErrDocNotFound
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in a REPLACE.
		return nil, ErrInternal
	}

	// TODO(brett19): Return mutation tokens with REPLACE responses.
	return &StoreResult{
		Cas: newDoc.Cas,
	}, nil
}

type DeleteOptions struct {
	Vbucket      uint
	CollectionID uint
	Key          []byte
	Cas          uint64
}

type DeleteResult struct {
	Cas uint64
}

func (e *Engine) Delete(opts DeleteOptions) (*DeleteResult, error) {
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
				return nil, mockdb.ErrDocNotFound
			}

			if opts.Cas != 0 && idoc.Cas != opts.Cas {
				return nil, mockdb.ErrDocExists
			}

			// Otherwise we simply update the value
			idoc.IsDeleted = true
			return idoc, nil
		})

	if err == mockdb.ErrDocExists {
		return nil, ErrDocExists
	} else if err == mockdb.ErrDocNotFound {
		return nil, ErrDocNotFound
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in a DELETE.
		return nil, ErrInternal
	}

	// TODO(brett19): Return mutation tokens with DELETE responses.
	return &DeleteResult{
		Cas: newDoc.Cas,
	}, nil
}

type CounterOptions struct {
	Vbucket      uint
	CollectionID uint
	Key          []byte
	Cas          uint64
	Initial      uint64
	Delta        uint64
	Expiry       uint32
}

type CounterResult struct {
	Cas   uint64
	Value uint64
}

func (e *Engine) counter(opts CounterOptions, isIncr bool) (*CounterResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	var expiryTime time.Time
	if opts.Expiry > 0 && opts.Expiry != 0xffffffff {
		expiryTime = e.db.Chrono().Now().Add(time.Duration(opts.Expiry) * time.Second)
	}

	doc := &mockdb.Document{
		VbID:         opts.Vbucket,
		CollectionID: opts.CollectionID,
		Key:          opts.Key,
		Value:        strconv.AppendUint(nil, opts.Initial, 64),
	}

	newDoc, err := e.db.Update(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if idoc == nil || idoc.IsDeleted {
				if opts.Expiry != 0xffffffff {
					return nil, mockdb.ErrDocNotFound
				}

				idoc = doc
			}

			if opts.Cas != 0 && idoc.Cas != opts.Cas {
				return nil, mockdb.ErrDocExists
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

			idoc.Value = strconv.AppendUint(nil, val, 64)
			idoc.Expiry = expiryTime
			return idoc, nil
		})

	if err == mockdb.ErrDocExists {
		return nil, ErrDocExists
	} else if err == mockdb.ErrDocNotFound {
		return nil, ErrDocNotFound
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in an INCREMENT/DECREMENT.
		return nil, ErrInternal
	}

	docValue, _ := strconv.ParseUint(string(newDoc.Value), 10, 64)

	// TODO(brett19): Return mutation tokens with a INCREMENT/DECREMENT responses.
	return &CounterResult{
		Cas:   newDoc.Cas,
		Value: docValue,
	}, nil
}

func (e *Engine) Increment(opts CounterOptions) (*CounterResult, error) {
	return e.counter(opts, true)
}

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
	}

	newDoc, err := e.db.Update(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if idoc == nil || idoc.IsDeleted {
				return nil, mockdb.ErrDocNotFound
			}

			if idoc.Cas != opts.Cas {
				return nil, mockdb.ErrDocExists
			}

			// Otherwise we simply update the value
			if isAppend {
				idoc.Value = append(idoc.Value, doc.Value...)
			} else {
				idoc.Value = append(doc.Value, idoc.Value...)
			}

			return idoc, nil
		})

	if err == mockdb.ErrDocExists {
		return nil, ErrDocExists
	} else if err == mockdb.ErrDocNotFound {
		return nil, ErrDocNotFound
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in a APPEND/PREPEND.
		return nil, ErrInternal
	}

	// TODO(brett19): Return mutation tokens with APPEND/PREPEND responses.
	return &StoreResult{
		Cas: newDoc.Cas,
	}, nil
}

func (e *Engine) Append(opts StoreOptions) (*StoreResult, error) {
	return e.adjoin(opts, true)
}

func (e *Engine) Prepend(opts StoreOptions) (*StoreResult, error) {
	return e.adjoin(opts, false)
}

type GetAndTouchOptions struct {
	ReplicaIdx   int
	Vbucket      uint
	CollectionID uint
	Key          []byte
	Expiry       uint32
}

func (e *Engine) GetAndTouch(opts GetAndTouchOptions) (*GetResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	// TODO(brett19): Implement GetAndTouch
	return nil, ErrNotSupported
}

type GetLockedOptions struct {
	ReplicaIdx   int
	Vbucket      uint
	CollectionID uint
	Key          []byte
	LockTime     uint32
}

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
				return nil, mockdb.ErrDocNotFound
			}

			idoc.LockExpiry = lockExpiryTime

			return idoc, nil
		})

	if err == mockdb.ErrDocNotFound {
		return nil, ErrDocNotFound
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in a GET.
		return nil, ErrInternal
	}

	return &GetResult{
		Cas:      doc.Cas,
		Datatype: doc.Datatype,
		Value:    doc.Value,
		Flags:    doc.Flags,
	}, nil
}

type UnlockOptions struct {
	ReplicaIdx   int
	Vbucket      uint
	CollectionID uint
	Key          []byte
	Cas          uint64
}

func (e *Engine) Unlock(opts UnlockOptions) (*StoreResult, error) {
	if err := e.confirmIsMaster(opts.Vbucket); err != nil {
		return nil, err
	}

	// TODO(brett19): Implement Unlock
	return nil, ErrNotSupported
}
