package svcimpls

import (
	"encoding/binary"
	"log"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mockimpl/svcimpls/crudproc"
)

type kvImplCrud struct {
}

func (x *kvImplCrud) Register(h *hookHelper) {
	h.RegisterKvHandler(memd.CmdAdd, x.handleAddRequest)
	h.RegisterKvHandler(memd.CmdSet, x.handleSetRequest)
	h.RegisterKvHandler(memd.CmdReplace, x.handleReplaceRequest)
	h.RegisterKvHandler(memd.CmdGet, x.handleGetRequest)
	h.RegisterKvHandler(memd.CmdDelete, x.handleDeleteRequest)
	h.RegisterKvHandler(memd.CmdIncrement, x.handleIncrementRequest)
	h.RegisterKvHandler(memd.CmdDecrement, x.handleDecrementRequest)
	h.RegisterKvHandler(memd.CmdAppend, x.handleAppendRequest)
	h.RegisterKvHandler(memd.CmdPrepend, x.handlePrependRequest)
	h.RegisterKvHandler(memd.CmdTouch, x.handleTouchRequest)
	h.RegisterKvHandler(memd.CmdGAT, x.handleGATRequest)
	h.RegisterKvHandler(memd.CmdGetLocked, x.handleGetLockedRequest)
	h.RegisterKvHandler(memd.CmdUnlockKey, x.handleUnlockRequest)
}

func (x *kvImplCrud) writeStatusReply(source mock.KvClient, pak *memd.Packet, status memd.StatusCode) {
	source.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: pak.Command,
		Opaque:  pak.Opaque,
		Status:  status,
	})
}

// makeProc either writes a reply to the network, or returns a non-nil Engine to use.
func (x *kvImplCrud) makeProc(source mock.KvClient, pak *memd.Packet) *crudproc.Engine {
	selectedBucket := source.SelectedBucket()
	if selectedBucket == nil {
		x.writeStatusReply(source, pak, memd.StatusNoBucket)
		return nil
	}

	sourceNode := source.Source().Node()
	vbOwnership := selectedBucket.VbucketOwnership(sourceNode)

	return crudproc.New(selectedBucket.Store(), vbOwnership)
}

func (x *kvImplCrud) writeProcErr(source mock.KvClient, pak *memd.Packet, err error) {
	// TODO(brett19): Implement special handling for various errors on specific versions.

	switch err {
	case crudproc.ErrNotSupported:
		x.writeStatusReply(source, pak, memd.StatusNotSupported)
	case crudproc.ErrNotMyVbucket:
		x.writeStatusReply(source, pak, memd.StatusNotMyVBucket)
	case crudproc.ErrInternal:
		x.writeStatusReply(source, pak, memd.StatusInternalError)
	case crudproc.ErrDocExists:
		x.writeStatusReply(source, pak, memd.StatusKeyExists)
	case crudproc.ErrDocNotFound:
		x.writeStatusReply(source, pak, memd.StatusKeyNotFound)
	case crudproc.ErrCasMismatch:
		x.writeStatusReply(source, pak, memd.StatusTmpFail)
	case crudproc.ErrLocked:
		x.writeStatusReply(source, pak, memd.StatusLocked)
	case crudproc.ErrNotLocked:
		x.writeStatusReply(source, pak, memd.StatusTmpFail)
	default:
		log.Printf("Recieved unexpected crud proc error: %s", err)
		x.writeStatusReply(source, pak, memd.StatusInternalError)
	}
}

func (x *kvImplCrud) handleGetRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		resp, err := proc.Get(crudproc.GetOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		extrasBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(extrasBuf[0:], resp.Flags)

		source.WritePacket(&memd.Packet{
			Magic:    memd.CmdMagicRes,
			Command:  pak.Command,
			Opaque:   pak.Opaque,
			Status:   memd.StatusSuccess,
			Cas:      resp.Cas,
			Datatype: resp.Datatype,
			Value:    resp.Value,
			Extras:   extrasBuf,
		})
	}
}

func (x *kvImplCrud) handleGetReplicaRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		resp, err := proc.GetReplica(crudproc.GetOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		extrasBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(extrasBuf[0:], resp.Flags)

		source.WritePacket(&memd.Packet{
			Magic:    memd.CmdMagicRes,
			Command:  pak.Command,
			Opaque:   pak.Opaque,
			Status:   memd.StatusSuccess,
			Cas:      resp.Cas,
			Datatype: resp.Datatype,
			Value:    resp.Value,
			Extras:   extrasBuf,
		})
	}
}

func (x *kvImplCrud) handleAddRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 8 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		if pak.Cas != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		flags := binary.BigEndian.Uint32(pak.Extras[0:])
		expiry := binary.BigEndian.Uint32(pak.Extras[4:])

		resp, err := proc.Add(crudproc.StoreOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Datatype:     pak.Datatype,
			Value:        pak.Value,
			Flags:        flags,
			Expiry:       expiry,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
		})
	}
}

func (x *kvImplCrud) handleSetRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 8 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		flags := binary.BigEndian.Uint32(pak.Extras[0:])
		expiry := binary.BigEndian.Uint32(pak.Extras[4:])

		resp, err := proc.Set(crudproc.StoreOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
			Datatype:     pak.Datatype,
			Value:        pak.Value,
			Flags:        flags,
			Expiry:       expiry,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
		})
	}
}

func (x *kvImplCrud) handleReplaceRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 8 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		flags := binary.BigEndian.Uint32(pak.Extras[0:])
		expiry := binary.BigEndian.Uint32(pak.Extras[4:])

		resp, err := proc.Replace(crudproc.StoreOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
			Datatype:     pak.Datatype,
			Value:        pak.Value,
			Flags:        flags,
			Expiry:       expiry,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
		})
	}
}

func (x *kvImplCrud) handleDeleteRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		resp, err := proc.Delete(crudproc.DeleteOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
		})
	}
}

func (x *kvImplCrud) handleIncrementRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 20 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		delta := binary.BigEndian.Uint64(pak.Extras[0:])
		initial := binary.BigEndian.Uint64(pak.Extras[8:])
		expiry := binary.BigEndian.Uint32(pak.Extras[16:])

		resp, err := proc.Increment(crudproc.CounterOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
			Initial:      initial,
			Delta:        delta,
			Expiry:       expiry,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		valueBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(valueBuf[0:], resp.Value)

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Value:   valueBuf,
		})
	}
}

func (x *kvImplCrud) handleDecrementRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 20 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		delta := binary.BigEndian.Uint64(pak.Extras[0:])
		initial := binary.BigEndian.Uint64(pak.Extras[8:])
		expiry := binary.BigEndian.Uint32(pak.Extras[16:])

		resp, err := proc.Decrement(crudproc.CounterOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
			Initial:      initial,
			Delta:        delta,
			Expiry:       expiry,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		valueBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(valueBuf[0:], resp.Value)

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Value:   valueBuf,
		})
	}
}

func (x *kvImplCrud) handleAppendRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		resp, err := proc.Append(crudproc.StoreOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
			Expiry:       0,
			Value:        pak.Value,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
		})
	}
}

func (x *kvImplCrud) handlePrependRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		resp, err := proc.Prepend(crudproc.StoreOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
			Expiry:       0,
			Value:        pak.Value,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
		})
	}
}

func (x *kvImplCrud) handleTouchRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 4 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		expiry := binary.BigEndian.Uint32(pak.Extras[0:])

		resp, err := proc.Touch(crudproc.TouchOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Expiry:       expiry,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
		})
	}
}

func (x *kvImplCrud) handleGATRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 4 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		expiry := binary.BigEndian.Uint32(pak.Extras[0:])

		resp, err := proc.GetAndTouch(crudproc.GetAndTouchOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Expiry:       expiry,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		extrasBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(extrasBuf[0:], resp.Flags)

		source.WritePacket(&memd.Packet{
			Magic:    memd.CmdMagicRes,
			Command:  pak.Command,
			Opaque:   pak.Opaque,
			Status:   memd.StatusSuccess,
			Cas:      resp.Cas,
			Datatype: resp.Datatype,
			Value:    resp.Value,
			Extras:   extrasBuf,
		})
	}
}

func (x *kvImplCrud) handleGetLockedRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 4 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		lockTime := binary.BigEndian.Uint32(pak.Extras[0:])

		resp, err := proc.GetLocked(crudproc.GetLockedOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			LockTime:     lockTime,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		extrasBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(extrasBuf[0:], resp.Flags)

		source.WritePacket(&memd.Packet{
			Magic:    memd.CmdMagicRes,
			Command:  pak.Command,
			Opaque:   pak.Opaque,
			Status:   memd.StatusSuccess,
			Cas:      resp.Cas,
			Datatype: resp.Datatype,
			Value:    resp.Value,
			Extras:   extrasBuf,
		})
	}
}

func (x *kvImplCrud) handleUnlockRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		resp, err := proc.Unlock(crudproc.UnlockOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
		})
	}
}
