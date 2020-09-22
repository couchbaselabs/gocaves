package svcimpls

import (
	"encoding/binary"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/hooks"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mockdb"
)

type kvImplCrud struct {
}

func (x *kvImplCrud) Register(hooks *hooks.KvHookManager) {
	reqExpects := hooks.Expect().Magic(memd.CmdMagicReq)

	reqExpects.Cmd(memd.CmdSet).Handler(x.handleSetRequest)
	reqExpects.Cmd(memd.CmdGet).Handler(x.handleGetRequest)
}

func (x *kvImplCrud) ensureBucket(source mock.KvClient, pak *memd.Packet, bucket mock.Bucket) bool {
	if bucket == nil {
		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdGet,
			Opaque:  pak.Opaque,
			Status:  memd.StatusNoBucket,
		})
		return false
	}
	return true
}

func (x *kvImplCrud) handleSetRequest(source mock.KvClient, pak *memd.Packet, next func()) {
	selectedBucket := source.SelectedBucket()
	if !x.ensureBucket(source, pak, selectedBucket) {
		return
	}

	doc := &mockdb.Document{
		VbID:         uint(pak.Vbucket),
		CollectionID: uint(pak.CollectionID),
		Key:          pak.Key,
		Value:        pak.Value,
		Xattrs:       nil,
	}

	newDoc, err := selectedBucket.Store().Update(
		doc.VbID, doc.CollectionID, doc.Key,
		func(idoc *mockdb.Document) (*mockdb.Document, error) {
			if pak.Cas != 0 {
				if idoc == nil {
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

			// Otherwise we simply update the value
			idoc.Value = doc.Value
			return idoc, nil
		})

	if err == mockdb.ErrDocExists {
		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdSet,
			Opaque:  pak.Opaque,
			Status:  memd.StatusKeyExists,
		})
		return
	} else if err == mockdb.ErrDocNotFound {
		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdSet,
			Opaque:  pak.Opaque,
			Status:  memd.StatusKeyNotFound,
		})
		return
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in a SET.
		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdSet,
			Opaque:  pak.Opaque,
			Status:  memd.StatusInternalError,
		})
		return
	}

	// TODO(brett19): Return mutation tokens with Set responses.
	source.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdSet,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
		Cas:     newDoc.Cas,
	})
}

func (x *kvImplCrud) handleGetRequest(source mock.KvClient, pak *memd.Packet, next func()) {
	selectedBucket := source.SelectedBucket()
	if !x.ensureBucket(source, pak, selectedBucket) {
		return
	}

	doc, err := selectedBucket.Store().Get(0, uint(pak.Vbucket), uint(pak.CollectionID), pak.Key)
	if err == mockdb.ErrDocNotFound {
		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdGet,
			Opaque:  pak.Opaque,
			Status:  memd.StatusKeyNotFound,
		})
		return
	} else if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in a GET.
		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdGet,
			Opaque:  pak.Opaque,
			Status:  memd.StatusInternalError,
		})
		return
	}

	extrasBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extrasBuf[0:], doc.Flags)

	source.WritePacket(&memd.Packet{
		Magic:    memd.CmdMagicRes,
		Command:  memd.CmdGet,
		Opaque:   pak.Opaque,
		Status:   memd.StatusSuccess,
		Cas:      doc.Cas,
		Datatype: doc.Datatype,
		Value:    doc.Value,
		Extras:   extrasBuf,
	})
}
