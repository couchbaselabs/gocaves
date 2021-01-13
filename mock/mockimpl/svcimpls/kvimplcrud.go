package svcimpls

import (
	"encoding/binary"
	"log"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockimpl/kvproc"
)

type kvImplCrud struct {
}

func (x *kvImplCrud) Register(h *hookHelper) {
	h.RegisterKvHandler(memd.CmdAdd, x.handleAddRequest)
	h.RegisterKvHandler(memd.CmdSet, x.handleSetRequest)
	h.RegisterKvHandler(memd.CmdReplace, x.handleReplaceRequest)
	h.RegisterKvHandler(memd.CmdGet, x.handleGetRequest)
	h.RegisterKvHandler(memd.CmdGetReplica, x.handleGetReplicaRequest)
	h.RegisterKvHandler(memd.CmdDelete, x.handleDeleteRequest)
	h.RegisterKvHandler(memd.CmdIncrement, x.handleIncrementRequest)
	h.RegisterKvHandler(memd.CmdDecrement, x.handleDecrementRequest)
	h.RegisterKvHandler(memd.CmdAppend, x.handleAppendRequest)
	h.RegisterKvHandler(memd.CmdPrepend, x.handlePrependRequest)
	h.RegisterKvHandler(memd.CmdTouch, x.handleTouchRequest)
	h.RegisterKvHandler(memd.CmdGAT, x.handleGATRequest)
	h.RegisterKvHandler(memd.CmdGetLocked, x.handleGetLockedRequest)
	h.RegisterKvHandler(memd.CmdUnlockKey, x.handleUnlockRequest)
	h.RegisterKvHandler(memd.CmdSubDocMultiLookup, x.handleMultiLookupRequest)
	h.RegisterKvHandler(memd.CmdSubDocMultiMutation, x.handleMultiMutateRequest)
	h.RegisterKvHandler(memd.CmdObserveSeqNo, x.handleObserveSeqNo)
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
func (x *kvImplCrud) makeProc(source mock.KvClient, pak *memd.Packet) *kvproc.Engine {
	selectedBucket := source.SelectedBucket()
	if selectedBucket == nil {
		x.writeStatusReply(source, pak, memd.StatusNoBucket)
		return nil
	}

	sourceNode := source.Source().Node()
	vbOwnership := selectedBucket.VbucketOwnership(sourceNode)

	return kvproc.New(selectedBucket.Store(), vbOwnership)
}

func (x *kvImplCrud) translateProcErr(err error) memd.StatusCode {
	// TODO(brett19): Implement special handling for various errors on specific versions.

	switch err {
	case nil:
		return memd.StatusSuccess
	case kvproc.ErrNotSupported:
		return memd.StatusNotSupported
	case kvproc.ErrNotMyVbucket:
		return memd.StatusNotMyVBucket
	case kvproc.ErrInternal:
		return memd.StatusInternalError
	case kvproc.ErrDocExists:
		return memd.StatusKeyExists
	case kvproc.ErrDocNotFound:
		return memd.StatusKeyNotFound
	case kvproc.ErrCasMismatch:
		return memd.StatusKeyExists
	case kvproc.ErrLocked:
		return memd.StatusLocked
	case kvproc.ErrNotLocked:
		return memd.StatusTmpFail
	case kvproc.ErrSdToManyTries:
		// TODO(brett19): Confirm too many sd retries is TMPFAIL.
		return memd.StatusTmpFail
	case kvproc.ErrSdNotJSON:
		return memd.StatusSubDocNotJSON
	case kvproc.ErrSdPathInvalid:
		return memd.StatusSubDocPathInvalid
	case kvproc.ErrSdPathMismatch:
		return memd.StatusSubDocPathMismatch
	case kvproc.ErrSdPathNotFound:
		return memd.StatusSubDocPathNotFound
	case kvproc.ErrSdPathExists:
		return memd.StatusSubDocPathExists
	case kvproc.ErrSdCantInsert:
		return memd.StatusSubDocCantInsert
	case kvproc.ErrSdBadCombo:
		return memd.StatusSubDocBadCombo
	case kvproc.ErrInvalidArgument:
		return memd.StatusInvalidArgs
	}

	log.Printf("Recieved unexpected crud proc error: %s", err)
	return memd.StatusInternalError
}

func (x *kvImplCrud) writeProcErr(source mock.KvClient, pak *memd.Packet, err error) {
	x.writeStatusReply(source, pak, x.translateProcErr(err))
}

func (x *kvImplCrud) handleGetRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		resp, err := proc.Get(kvproc.GetOptions{
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

		resp, err := proc.GetReplica(kvproc.GetOptions{
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

		resp, err := proc.Add(kvproc.StoreOptions{
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

		extrasBuf := make([]byte, 0)
		// TODO(brett19): Implement feature checking for mutation tokens.
		if true {
			mtBuf := make([]byte, 16)
			binary.BigEndian.PutUint64(mtBuf[0:], resp.VbUUID)
			binary.BigEndian.PutUint64(mtBuf[8:], resp.SeqNo)
			extrasBuf = append(extrasBuf, mtBuf...)
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Extras:  extrasBuf,
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

		resp, err := proc.Set(kvproc.StoreOptions{
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

		extrasBuf := make([]byte, 0)
		// TODO(brett19): Implement feature checking for mutation tokens.
		if true {
			mtBuf := make([]byte, 16)
			binary.BigEndian.PutUint64(mtBuf[0:], resp.VbUUID)
			binary.BigEndian.PutUint64(mtBuf[8:], resp.SeqNo)
			extrasBuf = append(extrasBuf, mtBuf...)
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Extras:  extrasBuf,
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

		resp, err := proc.Replace(kvproc.StoreOptions{
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

		extrasBuf := make([]byte, 0)
		// TODO(brett19): Implement feature checking for mutation tokens.
		if true {
			mtBuf := make([]byte, 16)
			binary.BigEndian.PutUint64(mtBuf[0:], resp.VbUUID)
			binary.BigEndian.PutUint64(mtBuf[8:], resp.SeqNo)
			extrasBuf = append(extrasBuf, mtBuf...)
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Extras:  extrasBuf,
		})
	}
}

func (x *kvImplCrud) handleDeleteRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		resp, err := proc.Delete(kvproc.DeleteOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		extrasBuf := make([]byte, 0)
		// TODO(brett19): Implement feature checking for mutation tokens.
		if true {
			mtBuf := make([]byte, 16)
			binary.BigEndian.PutUint64(mtBuf[0:], resp.VbUUID)
			binary.BigEndian.PutUint64(mtBuf[8:], resp.SeqNo)
			extrasBuf = append(extrasBuf, mtBuf...)
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Extras:  extrasBuf,
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

		resp, err := proc.Increment(kvproc.CounterOptions{
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

		extrasBuf := make([]byte, 0)
		// TODO(brett19): Implement feature checking for mutation tokens.
		if true {
			mtBuf := make([]byte, 16)
			binary.BigEndian.PutUint64(mtBuf[0:], resp.VbUUID)
			binary.BigEndian.PutUint64(mtBuf[8:], resp.SeqNo)
			extrasBuf = append(extrasBuf, mtBuf...)
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Value:   valueBuf,
			Extras:  extrasBuf,
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

		resp, err := proc.Decrement(kvproc.CounterOptions{
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

		extrasBuf := make([]byte, 0)
		// TODO(brett19): Implement feature checking for mutation tokens.
		if true {
			mtBuf := make([]byte, 16)
			binary.BigEndian.PutUint64(mtBuf[0:], resp.VbUUID)
			binary.BigEndian.PutUint64(mtBuf[8:], resp.SeqNo)
			extrasBuf = append(extrasBuf, mtBuf...)
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Value:   valueBuf,
			Extras:  extrasBuf,
		})
	}
}

func (x *kvImplCrud) handleAppendRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		resp, err := proc.Append(kvproc.StoreOptions{
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

		extrasBuf := make([]byte, 0)
		// TODO(brett19): Implement feature checking for mutation tokens.
		if true {
			mtBuf := make([]byte, 16)
			binary.BigEndian.PutUint64(mtBuf[0:], resp.VbUUID)
			binary.BigEndian.PutUint64(mtBuf[8:], resp.SeqNo)
			extrasBuf = append(extrasBuf, mtBuf...)
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Extras:  extrasBuf,
		})
	}
}

func (x *kvImplCrud) handlePrependRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		resp, err := proc.Prepend(kvproc.StoreOptions{
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

		extrasBuf := make([]byte, 0)
		// TODO(brett19): Implement feature checking for mutation tokens.
		if true {
			mtBuf := make([]byte, 16)
			binary.BigEndian.PutUint64(mtBuf[0:], resp.VbUUID)
			binary.BigEndian.PutUint64(mtBuf[8:], resp.SeqNo)
			extrasBuf = append(extrasBuf, mtBuf...)
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Extras:  extrasBuf,
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

		resp, err := proc.Touch(kvproc.TouchOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Expiry:       expiry,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		extrasBuf := make([]byte, 0)
		// TODO(brett19): Implement feature checking for mutation tokens.
		if true {
			mtBuf := make([]byte, 16)
			binary.BigEndian.PutUint64(mtBuf[0:], resp.VbUUID)
			binary.BigEndian.PutUint64(mtBuf[8:], resp.SeqNo)
			extrasBuf = append(extrasBuf, mtBuf...)
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Extras:  extrasBuf,
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

		resp, err := proc.GetAndTouch(kvproc.GetAndTouchOptions{
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

		resp, err := proc.GetLocked(kvproc.GetLockedOptions{
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

		resp, err := proc.Unlock(kvproc.UnlockOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		extrasBuf := make([]byte, 0)
		// TODO(brett19): Implement feature checking for mutation tokens.
		if true {
			mtBuf := make([]byte, 16)
			binary.BigEndian.PutUint64(mtBuf[0:], resp.VbUUID)
			binary.BigEndian.PutUint64(mtBuf[8:], resp.SeqNo)
			extrasBuf = append(extrasBuf, mtBuf...)
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Extras:  extrasBuf,
		})
	}
}

func (x *kvImplCrud) handleMultiLookupRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		var docFlags memd.SubdocDocFlag
		if len(pak.Extras) >= 1 {
			docFlags = memd.SubdocDocFlag(pak.Extras[0])
		}

		ops := make([]*kvproc.SubDocOp, 0)
		opData := pak.Value
		for byteIdx := 0; byteIdx < len(opData); byteIdx++ {
			opCode := memd.SubDocOpType(opData[byteIdx])

			switch opCode {
			case memd.SubDocOpGet:
				fallthrough
			case memd.SubDocOpExists:
				fallthrough
			case memd.SubDocOpGetCount:
				fallthrough
			case memd.SubDocOpGetDoc:
				if byteIdx+4 > len(opData) {
					log.Printf("not enough bytes 1")
					x.writeProcErr(source, pak, kvproc.ErrInternal)
					return
				}

				opFlags := memd.SubdocFlag(opData[byteIdx+1])
				pathLen := int(binary.BigEndian.Uint16(opData[byteIdx+2:]))
				if byteIdx+4+pathLen > len(opData) {
					log.Printf("not enough bytes 2 - %d - %d", byteIdx, pathLen)
					x.writeProcErr(source, pak, kvproc.ErrInternal)
					return
				}

				path := string(opData[byteIdx+4 : byteIdx+4+pathLen])

				ops = append(ops, &kvproc.SubDocOp{
					Op:          opCode,
					Path:        path,
					Value:       nil,
					IsXattrPath: opFlags&memd.SubdocFlagXattrPath != 0,
				})

				byteIdx += 4 + pathLen - 1

			default:
				log.Printf("unsupported op type")
				x.writeProcErr(source, pak, kvproc.ErrNotSupported)
				return
			}
		}

		resp, err := proc.MultiLookup(kvproc.MultiLookupOptions{
			Vbucket:       uint(pak.Vbucket),
			CollectionID:  uint(pak.CollectionID),
			Key:           pak.Key,
			AccessDeleted: docFlags&memd.SubdocDocFlagAccessDeleted != 0,
			Ops:           ops,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		valueBytes := make([]byte, 0)
		for _, opRes := range resp.Ops {
			opBytes := make([]byte, 6)
			resStatus := x.translateProcErr(opRes.Err)

			binary.BigEndian.PutUint16(opBytes[0:], uint16(resStatus))
			binary.BigEndian.PutUint32(opBytes[2:], uint32(len(opRes.Value)))
			opBytes = append(opBytes, opRes.Value...)

			valueBytes = append(valueBytes, opBytes...)
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Value:   valueBytes,
		})
	}
}

func (x *kvImplCrud) handleMultiMutateRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		var docFlags memd.SubdocDocFlag
		if len(pak.Extras) >= 1 {
			docFlags = memd.SubdocDocFlag(pak.Extras[0])
		}

		ops := make([]*kvproc.SubDocOp, 0)
		opData := pak.Value
		for byteIdx := 0; byteIdx < len(opData); byteIdx++ {
			opCode := memd.SubDocOpType(opData[byteIdx])

			switch opCode {
			case memd.SubDocOpDictAdd:
				fallthrough
			case memd.SubDocOpDictSet:
				fallthrough
			case memd.SubDocOpDelete:
				fallthrough
			case memd.SubDocOpReplace:
				fallthrough
			case memd.SubDocOpArrayPushLast:
				fallthrough
			case memd.SubDocOpArrayPushFirst:
				fallthrough
			case memd.SubDocOpArrayInsert:
				fallthrough
			case memd.SubDocOpArrayAddUnique:
				fallthrough
			case memd.SubDocOpCounter:
				fallthrough
			case memd.SubDocOpSetDoc:
				fallthrough
			case memd.SubDocOpAddDoc:
				fallthrough
			case memd.SubDocOpDeleteDoc:
				if byteIdx+8 > len(opData) {
					log.Printf("not enough bytes 11")
					x.writeProcErr(source, pak, kvproc.ErrInternal)
					return
				}

				opFlags := memd.SubdocFlag(opData[byteIdx+1])
				pathLen := int(binary.BigEndian.Uint16(opData[byteIdx+2:]))
				valueLen := int(binary.BigEndian.Uint32(opData[byteIdx+4:]))
				if byteIdx+8+pathLen+valueLen > len(opData) {
					log.Printf("not enough bytes 12 - %d - %d", byteIdx, pathLen)
					x.writeProcErr(source, pak, kvproc.ErrInternal)
					return
				}

				path := string(opData[byteIdx+8 : byteIdx+8+pathLen])
				value := opData[byteIdx+8+pathLen : byteIdx+8+pathLen+valueLen]

				ops = append(ops, &kvproc.SubDocOp{
					Op:           opCode,
					Path:         path,
					Value:        value,
					CreatePath:   opFlags&memd.SubdocFlagMkDirP != 0,
					IsXattrPath:  opFlags&memd.SubdocFlagXattrPath != 0,
					ExpandMacros: opFlags&memd.SubdocFlagExpandMacros != 0,
				})

				byteIdx += 8 + pathLen + valueLen - 1

			default:
				log.Printf("unsupported op type")
				x.writeProcErr(source, pak, kvproc.ErrInternal)
				return
			}
		}

		resp, err := proc.MultiMutate(kvproc.MultiMutateOptions{
			Vbucket:         uint(pak.Vbucket),
			CollectionID:    uint(pak.CollectionID),
			Key:             pak.Key,
			AccessDeleted:   docFlags&memd.SubdocDocFlagAccessDeleted != 0,
			CreateAsDeleted: docFlags&memd.SubdocDocFlagCreateAsDeleted != 0,
			CreateIfMissing: docFlags&memd.SubdocDocFlagMkDoc != 0,
			CreateOnly:      docFlags&memd.SubdocDocFlagAddDoc != 0,
			Ops:             ops,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		failedOpIdx := -1
		for opIdx, opRes := range resp.Ops {
			if opRes.Err != nil {
				failedOpIdx = opIdx
			}
		}

		if failedOpIdx >= 0 {
			resStatus := x.translateProcErr(resp.Ops[failedOpIdx].Err)

			valueBytes := make([]byte, 3)
			valueBytes[0] = uint8(failedOpIdx)
			binary.BigEndian.PutUint16(valueBytes[1:], uint16(resStatus))

			// TODO(brett19): Confirm that sub-document errors return 0 CAS.
			source.WritePacket(&memd.Packet{
				Magic:   memd.CmdMagicRes,
				Command: pak.Command,
				Opaque:  pak.Opaque,
				Status:  memd.StatusSubDocBadMulti,
				Cas:     0,
				Value:   valueBytes,
			})
			return
		}

		valueBytes := make([]byte, 0)
		for opIdx, opRes := range resp.Ops {
			if opRes.Err == nil {
				opBytes := make([]byte, 7)
				resStatus := x.translateProcErr(opRes.Err)

				opBytes[0] = uint8(opIdx)
				binary.BigEndian.PutUint16(opBytes[1:], uint16(resStatus))
				binary.BigEndian.PutUint32(opBytes[3:], uint32(len(opRes.Value)))
				opBytes = append(opBytes, opRes.Value...)

				valueBytes = append(valueBytes, opBytes...)
			}
		}

		extrasBuf := make([]byte, 0)
		// TODO(brett19): Implement feature checking for mutation tokens.
		if true {
			mtBuf := make([]byte, 16)
			binary.BigEndian.PutUint64(mtBuf[0:], resp.VbUUID)
			binary.BigEndian.PutUint64(mtBuf[8:], resp.SeqNo)
			extrasBuf = append(extrasBuf, mtBuf...)
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Value:   valueBytes,
			Extras:  extrasBuf,
		})
	}
}

func (x *kvImplCrud) handleObserveSeqNo(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Value) != 8 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		vbUUID := binary.BigEndian.Uint64(pak.Value)

		resp, err := proc.ObserveSeqNo(kvproc.ObserveSeqNoOptions{
			Vbucket: uint(pak.Vbucket),
			VbUUID:  vbUUID,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		// No support for failover variant yet...
		valueBuf := make([]byte, 27)
		valueBuf[0] = 0
		binary.BigEndian.PutUint16(valueBuf[1:], pak.Vbucket)
		binary.BigEndian.PutUint64(valueBuf[3:], resp.VbUUID)
		binary.BigEndian.PutUint64(valueBuf[11:], resp.PersistSeqNo)
		binary.BigEndian.PutUint64(valueBuf[19:], resp.CurrentSeqNo)

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Value:   valueBuf,
		})
	}
}
