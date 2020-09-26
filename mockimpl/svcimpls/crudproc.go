package svcimpls

import (
	"encoding/binary"
	"strconv"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mockdb"
)

/*
We separate out the handlers for basic CRUD operations here to enable
us to test them independantly of any networking.
*/

type crudProc struct {
}

func (p *crudProc) makeStatusReply(pak *memd.Packet, status memd.StatusCode) *memd.Packet {
	return &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: pak.Command,
		Opaque:  pak.Opaque,
		Status:  status,
	}
}

func (p *crudProc) Get(bucket *mockdb.Bucket, repIdx uint, pak *memd.Packet) *memd.Packet {
	doc, err := bucket.Get(repIdx, uint(pak.Vbucket), uint(pak.CollectionID), pak.Key)
	if err == mockdb.ErrDocNotFound || doc.IsDeleted {
		return p.makeStatusReply(pak, memd.StatusKeyNotFound)
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in a GET.
		return p.makeStatusReply(pak, memd.StatusInternalError)
	}

	extrasBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extrasBuf[0:], doc.Flags)

	return &memd.Packet{
		Magic:    memd.CmdMagicRes,
		Command:  pak.Command,
		Opaque:   pak.Opaque,
		Status:   memd.StatusSuccess,
		Cas:      doc.Cas,
		Datatype: doc.Datatype,
		Value:    doc.Value,
		Extras:   extrasBuf,
	}
}

func (p *crudProc) Add(bucket *mockdb.Bucket, pak *memd.Packet) *memd.Packet {
	doc := &mockdb.Document{
		VbID:         uint(pak.Vbucket),
		CollectionID: uint(pak.CollectionID),
		Key:          pak.Key,
		Value:        pak.Value,
	}

	newDoc, err := bucket.Insert(doc)

	if err == mockdb.ErrDocExists {
		return p.makeStatusReply(pak, memd.StatusKeyExists)
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in an ADD.
		return p.makeStatusReply(pak, memd.StatusInternalError)
	}

	// TODO(brett19): Return mutation tokens with ADD responses.
	return &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: pak.Command,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
		Cas:     newDoc.Cas,
	}
}

func (p *crudProc) Set(bucket *mockdb.Bucket, pak *memd.Packet) *memd.Packet {
	doc := &mockdb.Document{
		VbID:         uint(pak.Vbucket),
		CollectionID: uint(pak.CollectionID),
		Key:          pak.Key,
		Value:        pak.Value,
	}

	newDoc, err := bucket.Update(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if pak.Cas != 0 {
				if idoc == nil || idoc.IsDeleted {
					// The user specified a CAS and the document didn't exist.
					return nil, mockdb.ErrDocNotFound
				}

				if idoc.Cas != pak.Cas {
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
		return p.makeStatusReply(pak, memd.StatusKeyExists)
	} else if err == mockdb.ErrDocNotFound {
		return p.makeStatusReply(pak, memd.StatusKeyNotFound)
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in a SET.
		return p.makeStatusReply(pak, memd.StatusInternalError)
	}

	// TODO(brett19): Return mutation tokens with SET responses.
	return &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: pak.Command,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
		Cas:     newDoc.Cas,
	}
}

func (p *crudProc) Replace(bucket *mockdb.Bucket, pak *memd.Packet) *memd.Packet {
	doc := &mockdb.Document{
		VbID:         uint(pak.Vbucket),
		CollectionID: uint(pak.CollectionID),
		Key:          pak.Key,
		Value:        pak.Value,
	}

	newDoc, err := bucket.Update(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if idoc == nil || idoc.IsDeleted {
				return nil, mockdb.ErrDocNotFound
			}

			if idoc.Cas != pak.Cas {
				return nil, mockdb.ErrDocExists
			}

			// Otherwise we simply update the value
			idoc.Value = doc.Value
			return idoc, nil
		})

	if err == mockdb.ErrDocExists {
		return p.makeStatusReply(pak, memd.StatusKeyExists)
	} else if err == mockdb.ErrDocNotFound {
		return p.makeStatusReply(pak, memd.StatusKeyNotFound)
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in a REPLACE.
		return p.makeStatusReply(pak, memd.StatusInternalError)
	}

	// TODO(brett19): Return mutation tokens with REPLACE responses.
	return &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: pak.Command,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
		Cas:     newDoc.Cas,
	}
}

func (p *crudProc) Delete(bucket *mockdb.Bucket, pak *memd.Packet) *memd.Packet {
	doc := &mockdb.Document{
		VbID:         uint(pak.Vbucket),
		CollectionID: uint(pak.CollectionID),
		Key:          pak.Key,
	}

	newDoc, err := bucket.Update(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if idoc == nil || idoc.IsDeleted {
				return nil, mockdb.ErrDocNotFound
			}

			if pak.Cas != 0 && idoc.Cas != pak.Cas {
				return nil, mockdb.ErrDocExists
			}

			// Otherwise we simply update the value
			idoc.IsDeleted = true
			return idoc, nil
		})

	if err == mockdb.ErrDocExists {
		return p.makeStatusReply(pak, memd.StatusKeyExists)
	} else if err == mockdb.ErrDocNotFound {
		return p.makeStatusReply(pak, memd.StatusKeyNotFound)
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in a DELETE.
		return p.makeStatusReply(pak, memd.StatusInternalError)
	}

	// TODO(brett19): Return mutation tokens with DELETE responses.
	return &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdDelete,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
		Cas:     newDoc.Cas,
	}
}

func (p *crudProc) counter(bucket *mockdb.Bucket, pak *memd.Packet, isIncr bool) *memd.Packet {
	if len(pak.Extras) != 20 {
		return p.makeStatusReply(pak, memd.StatusInvalidArgs)
	}

	delta := binary.BigEndian.Uint64(pak.Extras[0:])
	initial := binary.BigEndian.Uint64(pak.Extras[8:])
	expiry := binary.BigEndian.Uint32(pak.Extras[16:])

	var expiryTime time.Time
	if expiry > 0 && expiry != 0xffffffff {
		expiryTime = bucket.Chrono().Now().Add(time.Duration(expiry) * time.Second)
	}

	doc := &mockdb.Document{
		VbID:         uint(pak.Vbucket),
		CollectionID: uint(pak.CollectionID),
		Key:          pak.Key,
		Value:        strconv.AppendUint(nil, initial, 64),
	}

	newDoc, err := bucket.Update(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if idoc == nil || idoc.IsDeleted {
				if expiry != 0xffffffff {
					return nil, mockdb.ErrDocNotFound
				}

				idoc = doc
			}

			if pak.Cas != 0 && idoc.Cas != pak.Cas {
				return nil, mockdb.ErrDocExists
			}

			// Otherwise we simply update the value
			val, err := strconv.ParseUint(string(idoc.Value), 10, 64)
			if err != nil {
				return nil, err
			}

			// TODO(brett19): Double-check the saturation logic on the server...
			if isIncr {
				if val+delta < val {
					// overflow
					val = 0xffffffffffffffff
				} else {
					val += delta
				}
			} else {
				if delta > val {
					// underflow
					val = 0
				} else {
					val -= delta
				}
			}

			idoc.Value = strconv.AppendUint(nil, val, 64)
			idoc.Expiry = expiryTime
			return idoc, nil
		})

	if err == mockdb.ErrDocExists {
		return p.makeStatusReply(pak, memd.StatusKeyExists)
	} else if err == mockdb.ErrDocNotFound {
		return p.makeStatusReply(pak, memd.StatusKeyNotFound)
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in an INCREMENT/DECREMENT.
		return p.makeStatusReply(pak, memd.StatusInternalError)
	}

	// TODO(brett19): Return mutation tokens with a INCREMENT/DECREMENT responses.
	return &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: pak.Command,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
		Cas:     newDoc.Cas,
	}
}

func (p *crudProc) Increment(bucket *mockdb.Bucket, pak *memd.Packet) *memd.Packet {
	return p.counter(bucket, pak, true)
}

func (p *crudProc) Decrement(bucket *mockdb.Bucket, pak *memd.Packet) *memd.Packet {
	return p.counter(bucket, pak, false)
}

func (p *crudProc) adjoin(bucket *mockdb.Bucket, pak *memd.Packet, isAppend bool) *memd.Packet {
	doc := &mockdb.Document{
		VbID:         uint(pak.Vbucket),
		CollectionID: uint(pak.CollectionID),
		Key:          pak.Key,
		Value:        pak.Value,
	}

	newDoc, err := bucket.Update(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if idoc == nil || idoc.IsDeleted {
				return nil, mockdb.ErrDocNotFound
			}

			if idoc.Cas != pak.Cas {
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
	} else if err == mockdb.ErrDocNotFound {
		return p.makeStatusReply(pak, memd.StatusKeyNotFound)
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in a APPEND/PREPEND.
		return p.makeStatusReply(pak, memd.StatusInternalError)
	}

	// TODO(brett19): Return mutation tokens with APPEND/PREPEND responses.
	return &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: pak.Command,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
		Cas:     newDoc.Cas,
	}
}

func (p *crudProc) Append(bucket *mockdb.Bucket, pak *memd.Packet) *memd.Packet {
	return p.adjoin(bucket, pak, true)
}

func (p *crudProc) Prepend(bucket *mockdb.Bucket, pak *memd.Packet) *memd.Packet {
	return p.adjoin(bucket, pak, false)
}

func (p *crudProc) GetAndTouch(bucket *mockdb.Bucket, pak *memd.Packet) *memd.Packet {
	// TODO(brett19): Implement GetAndTouch
	return p.makeStatusReply(pak, memd.StatusNotSupported)
}

func (p *crudProc) GetLocked(bucket *mockdb.Bucket, pak *memd.Packet) *memd.Packet {
	if len(pak.Extras) != 4 {
		return p.makeStatusReply(pak, memd.StatusInvalidArgs)
	}

	lockExpiry := binary.BigEndian.Uint32(pak.Extras[0:])
	if lockExpiry == 0 {
		// TODO(brett19): Confirm this is in fact the default...
		lockExpiry = 30
	}

	lockExpiryTime := bucket.Chrono().Now().Add(time.Duration(lockExpiry) * time.Second)

	lkpDoc := &mockdb.Document{
		VbID:         uint(pak.Vbucket),
		CollectionID: uint(pak.CollectionID),
		Key:          pak.Key,
	}
	doc, err := bucket.Update(
		lkpDoc.VbID, lkpDoc.CollectionID, lkpDoc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if idoc == nil || idoc.IsDeleted {
				return nil, mockdb.ErrDocNotFound
			}

			idoc.LockExpiry = lockExpiryTime

			return idoc, nil
		})

	if err == mockdb.ErrDocNotFound {
		return p.makeStatusReply(pak, memd.StatusKeyNotFound)
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in a GET.
		return p.makeStatusReply(pak, memd.StatusInternalError)
	}

	extrasBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extrasBuf[0:], doc.Flags)

	return &memd.Packet{
		Magic:    memd.CmdMagicRes,
		Command:  pak.Command,
		Opaque:   pak.Opaque,
		Status:   memd.StatusSuccess,
		Cas:      doc.Cas,
		Datatype: doc.Datatype,
		Value:    doc.Value,
		Extras:   extrasBuf,
	}
}

func (p *crudProc) Unlock(bucket *mockdb.Bucket, pak *memd.Packet) *memd.Packet {
	// TODO(brett19): Implement Unlock
	return p.makeStatusReply(pak, memd.StatusNotSupported)
}
