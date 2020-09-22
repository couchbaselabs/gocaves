package mockimpl

import (
	"encoding/binary"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mockdb"
)

type kvImplCrud struct {
}

func (x *kvImplCrud) Register(hooks *KvHookManager) {
	reqExpects := hooks.Expect().Magic(memd.CmdMagicReq)

	reqExpects.Cmd(memd.CmdSet).Handler(x.handleSetRequest)
	reqExpects.Cmd(memd.CmdGet).Handler(x.handleGetRequest)
}

func (x *kvImplCrud) ensureBucket(source *KvClient, pak *memd.Packet, bucket *Bucket) bool {
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

func (x *kvImplCrud) handleSetRequest(source *KvClient, pak *memd.Packet, next func()) {
	selectedBucket := source.SelectedBucket()
	if !x.ensureBucket(source, pak, selectedBucket) {
		return
	}

	// TODO(brett19): The xattr behaviour here is wrong, it should be modifying the existing doc.
	doc := &mockdb.Document{
		VbID:         uint(pak.Vbucket),
		CollectionID: uint(pak.CollectionID),
		Key:          pak.Key,
		Value:        pak.Value,
		Xattrs:       nil,
	}
	newDoc, err := selectedBucket.Store().Set(doc)
	if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in a SET.
		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdSet,
			Opaque:  pak.Opaque,
			Status:  memd.StatusKeyExists,
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

func (x *kvImplCrud) handleGetRequest(source *KvClient, pak *memd.Packet, next func()) {
	selectedBucket := source.SelectedBucket()
	if !x.ensureBucket(source, pak, selectedBucket) {
		return
	}

	doc, err := selectedBucket.Store().Get(0, uint(pak.Vbucket), uint(pak.CollectionID), pak.Key)
	if err != nil {
		// TODO(brett19): Correctly handle the various errors which can occur in a GET.
		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdGet,
			Opaque:  pak.Opaque,
			Status:  memd.StatusKeyNotFound,
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
