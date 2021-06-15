package svcimpls

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
	"github.com/couchbaselabs/gocaves/mock/mockimpl/kvproc"
)

type kvImplCrud struct {
}

func (x *kvImplCrud) Register(h *hookHelper) {
	h.RegisterKvHandler(memd.CmdAdd, x.handleAddRequest)
	h.RegisterKvHandler(memd.CmdSet, x.handleSetRequest)
	h.RegisterKvHandler(memd.CmdReplace, x.handleReplaceRequest)
	h.RegisterKvHandler(memd.CmdGet, x.handleGetRequest)
	h.RegisterKvHandler(memd.CmdGetMeta, x.handleGetMetaRequest)
	h.RegisterKvHandler(memd.CmdGetRandom, x.handleGetRandomRequest)
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
	h.RegisterKvHandler(memd.CmdCollectionsGetManifest, x.handleManifestRequest)
	h.RegisterKvHandler(memd.CmdCollectionsGetID, x.handleGetCollectionIDRequest)
	h.RegisterKvHandler(memd.CmdStat, x.handleStatsRequest)
}

func (x *kvImplCrud) writeStatusReply(source mock.KvClient, pak *memd.Packet, status memd.StatusCode, start time.Time) {
	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: pak.Command,
		Opaque:  pak.Opaque,
		Status:  status,
	}, start)
}

// makeProc either writes a reply to the network, or returns a non-nil Engine to use.
func (x *kvImplCrud) makeProc(source mock.KvClient, pak *memd.Packet, permission mockauth.Permission, start time.Time) *kvproc.Engine {
	selectedBucket := source.SelectedBucket()
	if selectedBucket == nil {
		x.writeStatusReply(source, pak, memd.StatusNoBucket, start)
		return nil
	}

	sourceNode := source.Source().Node()
	vbOwnership := selectedBucket.VbucketOwnership(sourceNode)

	if !source.CheckAuthenticated(permission, pak.CollectionID) {
		// TODO(chvck): CheckAuthenticated needs to change, this could be actually be auth or access error depending on the user
		// access levels.
		x.writeStatusReply(source, pak, memd.StatusAuthError, start)
		return nil
	}

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
	case kvproc.ErrValueTooBig:
		return memd.StatusTooBig
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
	case kvproc.ErrSdInvalidXattr:
		return memd.StatusCode(0x87)
	case kvproc.ErrSdCannotModifyVattr:
		return memd.StatusSubDocXattrCannotModifyVAttr
	case kvproc.ErrSdInvalidFlagCombo:
		return memd.StatusSubDocXattrInvalidFlagCombo
	case kvproc.ErrUnknownXattrMacro:
		return memd.StatusSubDocXattrUnknownMacro
	case mock.ErrScopeNotFound:
		return memd.StatusScopeUnknown
	case mock.ErrCollectionNotFound:
		return memd.StatusCollectionUnknown
	}

	log.Printf("Recieved unexpected crud proc error: %s", err)
	return memd.StatusInternalError
}

func (x *kvImplCrud) writeProcErr(source mock.KvClient, pak *memd.Packet, err error, start time.Time) {
	x.writeStatusReply(source, pak, x.translateProcErr(err), start)
}

func (x *kvImplCrud) handleGetRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataRead, start); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
			return
		}

		resp, err := proc.Get(kvproc.GetOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
		})
		if err != nil {
			x.writeProcErr(source, pak, err, start)
			return
		}

		extrasBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(extrasBuf[0:], resp.Flags)

		writePacketToSource(source, &memd.Packet{
			Magic:    memd.CmdMagicRes,
			Command:  pak.Command,
			Opaque:   pak.Opaque,
			Status:   memd.StatusSuccess,
			Cas:      resp.Cas,
			Datatype: resp.Datatype,
			Value:    resp.Value,
			Extras:   extrasBuf,
		}, start)
	}
}

func (x *kvImplCrud) handleGetMetaRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataRead, start); proc != nil {
		if len(pak.Extras) != 1 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
			return
		}

		getFlags := pak.Extras[0]
		if getFlags != 2 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
			return
		}

		resp, err := proc.GetMeta(kvproc.GetMetaOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
		})
		if err != nil {
			x.writeProcErr(source, pak, err, start)
			return
		}

		extrasBuf := make([]byte, 21)
		if resp.IsDeleted {
			binary.BigEndian.PutUint32(extrasBuf[0:], 1)
		} else {
			binary.BigEndian.PutUint32(extrasBuf[0:], 0)
		}
		binary.BigEndian.PutUint32(extrasBuf[4:], resp.Flags)
		binary.BigEndian.PutUint32(extrasBuf[8:], uint32(resp.ExpTime.Unix()))
		binary.BigEndian.PutUint64(extrasBuf[12:], resp.SeqNo)
		extrasBuf[20] = resp.Datatype

		writePacketToSource(source, &memd.Packet{
			Magic:    memd.CmdMagicRes,
			Command:  pak.Command,
			Opaque:   pak.Opaque,
			Status:   memd.StatusSuccess,
			Cas:      resp.Cas,
			Datatype: 0,
			Value:    resp.Value,
			Extras:   extrasBuf,
		}, start)
	}
}

func (x *kvImplCrud) handleGetRandomRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataRead, start); proc != nil {
		var collectionID uint32
		if len(pak.Extras) == 4 {
			collectionID = binary.BigEndian.Uint32(pak.Extras)
		} else if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
			return
		}

		resp, err := proc.GetRandom(kvproc.GetRandomOptions{
			CollectionID: uint(collectionID),
		})
		if err != nil {
			x.writeProcErr(source, pak, err, start)
			return
		}

		extrasBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(extrasBuf[0:], resp.Flags)

		writePacketToSource(source, &memd.Packet{
			Magic:    memd.CmdMagicRes,
			Command:  pak.Command,
			Opaque:   pak.Opaque,
			Status:   memd.StatusSuccess,
			Cas:      resp.Cas,
			Datatype: resp.Datatype,
			Value:    resp.Value,
			Extras:   extrasBuf,
			Key:      resp.Key,
		}, start)
	}
}

func (x *kvImplCrud) handleGetReplicaRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataRead, start); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
			return
		}

		resp, err := proc.GetReplica(kvproc.GetOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
		})
		if err != nil {
			x.writeProcErr(source, pak, err, start)
			return
		}

		extrasBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(extrasBuf[0:], resp.Flags)

		writePacketToSource(source, &memd.Packet{
			Magic:    memd.CmdMagicRes,
			Command:  pak.Command,
			Opaque:   pak.Opaque,
			Status:   memd.StatusSuccess,
			Cas:      resp.Cas,
			Datatype: resp.Datatype,
			Value:    resp.Value,
			Extras:   extrasBuf,
		}, start)
	}
}

func (x *kvImplCrud) handleAddRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataWrite, start); proc != nil {
		if len(pak.Extras) != 8 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
			return
		}

		if pak.Cas != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
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
			x.writeProcErr(source, pak, err, start)
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

		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Extras:  extrasBuf,
		}, start)
	}
}

func (x *kvImplCrud) handleSetRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataWrite, start); proc != nil {
		if len(pak.Extras) != 8 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
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
			x.writeProcErr(source, pak, err, start)
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

		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Extras:  extrasBuf,
		}, start)
	}
}

func (x *kvImplCrud) handleReplaceRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataWrite, start); proc != nil {
		if len(pak.Extras) != 8 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
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
			x.writeProcErr(source, pak, err, start)
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

		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Extras:  extrasBuf,
		}, start)
	}
}

func (x *kvImplCrud) handleDeleteRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataWrite, start); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
			return
		}

		resp, err := proc.Delete(kvproc.DeleteOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
		})
		if err != nil {
			x.writeProcErr(source, pak, err, start)
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

		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Extras:  extrasBuf,
		}, start)
	}
}

func (x *kvImplCrud) handleIncrementRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataWrite, start); proc != nil {
		if len(pak.Extras) != 20 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
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
			x.writeProcErr(source, pak, err, start)
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

		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Value:   valueBuf,
			Extras:  extrasBuf,
		}, start)
	}
}

func (x *kvImplCrud) handleDecrementRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataWrite, start); proc != nil {
		if len(pak.Extras) != 20 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
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
			x.writeProcErr(source, pak, err, start)
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

		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Value:   valueBuf,
			Extras:  extrasBuf,
		}, start)
	}
}

func (x *kvImplCrud) handleAppendRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataWrite, start); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
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
			x.writeProcErr(source, pak, err, start)
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

		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Extras:  extrasBuf,
		}, start)
	}
}

func (x *kvImplCrud) handlePrependRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataWrite, start); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
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
			x.writeProcErr(source, pak, err, start)
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

		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Extras:  extrasBuf,
		}, start)
	}
}

func (x *kvImplCrud) handleTouchRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataWrite, start); proc != nil {
		if len(pak.Extras) != 4 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
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
			x.writeProcErr(source, pak, err, start)
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

		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Extras:  extrasBuf,
		}, start)
	}
}

func (x *kvImplCrud) handleGATRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataWrite, start); proc != nil {
		if len(pak.Extras) != 4 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
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
			x.writeProcErr(source, pak, err, start)
			return
		}

		extrasBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(extrasBuf[0:], resp.Flags)

		writePacketToSource(source, &memd.Packet{
			Magic:    memd.CmdMagicRes,
			Command:  pak.Command,
			Opaque:   pak.Opaque,
			Status:   memd.StatusSuccess,
			Cas:      resp.Cas,
			Datatype: resp.Datatype,
			Value:    resp.Value,
			Extras:   extrasBuf,
		}, start)
	}
}

func (x *kvImplCrud) handleGetLockedRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataRead, start); proc != nil {
		if len(pak.Extras) != 4 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
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
			x.writeProcErr(source, pak, err, start)
			return
		}

		extrasBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(extrasBuf[0:], resp.Flags)

		writePacketToSource(source, &memd.Packet{
			Magic:    memd.CmdMagicRes,
			Command:  pak.Command,
			Opaque:   pak.Opaque,
			Status:   memd.StatusSuccess,
			Cas:      resp.Cas,
			Datatype: resp.Datatype,
			Value:    resp.Value,
			Extras:   extrasBuf,
		}, start)
	}
}

func (x *kvImplCrud) handleUnlockRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataWrite, start); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
			return
		}

		resp, err := proc.Unlock(kvproc.UnlockOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
		})
		if err != nil {
			x.writeProcErr(source, pak, err, start)
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

		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Extras:  extrasBuf,
		}, start)
	}
}

func (x *kvImplCrud) handleMultiLookupRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataRead, start); proc != nil {
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
					x.writeProcErr(source, pak, kvproc.ErrInternal, start)
					return
				}

				opFlags := memd.SubdocFlag(opData[byteIdx+1])
				pathLen := int(binary.BigEndian.Uint16(opData[byteIdx+2:]))
				if byteIdx+4+pathLen > len(opData) {
					log.Printf("not enough bytes 2 - %d - %d", byteIdx, pathLen)
					x.writeProcErr(source, pak, kvproc.ErrInternal, start)
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
				x.writeProcErr(source, pak, kvproc.ErrNotSupported, start)
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
			x.writeProcErr(source, pak, err, start)
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

		status := memd.StatusSuccess
		if resp.IsDeleted {
			status = memd.StatusSubDocSuccessDeleted
		}

		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  status,
			Cas:     resp.Cas,
			Value:   valueBytes,
		}, start)
	}
}

func (x *kvImplCrud) handleMultiMutateRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataWrite, start); proc != nil {
		var docFlags memd.SubdocDocFlag
		var expiry uint32
		if len(pak.Extras) > 0 {
			if len(pak.Extras) == 1 {
				docFlags = memd.SubdocDocFlag(pak.Extras[0])
			} else {
				if len(pak.Extras) != 5 {
					x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
				}
				expiry = binary.BigEndian.Uint32(pak.Extras[0:])
				docFlags = memd.SubdocDocFlag(pak.Extras[4])
			}
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
					x.writeProcErr(source, pak, kvproc.ErrInternal, start)
					return
				}

				opFlags := memd.SubdocFlag(opData[byteIdx+1])
				pathLen := int(binary.BigEndian.Uint16(opData[byteIdx+2:]))
				valueLen := int(binary.BigEndian.Uint32(opData[byteIdx+4:]))
				if byteIdx+8+pathLen+valueLen > len(opData) {
					log.Printf("not enough bytes 12 - %d - %d", byteIdx, pathLen)
					x.writeProcErr(source, pak, kvproc.ErrInternal, start)
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
				x.writeProcErr(source, pak, kvproc.ErrInternal, start)
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
			Expiry:          expiry,
		})
		if err != nil {
			x.writeProcErr(source, pak, err, start)
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
			writePacketToSource(source, &memd.Packet{
				Magic:   memd.CmdMagicRes,
				Command: pak.Command,
				Opaque:  pak.Opaque,
				Status:  memd.StatusSubDocBadMulti,
				Cas:     0,
				Value:   valueBytes,
			}, start)
			return
		}

		valueBytes := make([]byte, 0)
		for opIdx, opRes := range resp.Ops {
			if opRes.Err == nil && len(opRes.Value) > 0 {
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

		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Value:   valueBytes,
			Extras:  extrasBuf,
		}, start)
	}
}

func (x *kvImplCrud) handleObserveSeqNo(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionDataRead, start); proc != nil {
		if len(pak.Value) != 8 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
			return
		}

		vbUUID := binary.BigEndian.Uint64(pak.Value)

		resp, err := proc.ObserveSeqNo(kvproc.ObserveSeqNoOptions{
			Vbucket: uint(pak.Vbucket),
			VbUUID:  vbUUID,
		})
		if err != nil {
			x.writeProcErr(source, pak, err, start)
			return
		}

		// No support for failover variant yet...
		valueBuf := make([]byte, 27)
		valueBuf[0] = 0
		binary.BigEndian.PutUint16(valueBuf[1:], pak.Vbucket)
		binary.BigEndian.PutUint64(valueBuf[3:], resp.VbUUID)
		binary.BigEndian.PutUint64(valueBuf[11:], resp.PersistSeqNo)
		binary.BigEndian.PutUint64(valueBuf[19:], resp.CurrentSeqNo)

		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Value:   valueBuf,
		}, start)
	}
}

func (x *kvImplCrud) handleStatsRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionStatsRead, start); proc != nil {
		if bytes.HasPrefix(pak.Key, []byte("key ")) {
			// TODO: handle key stats
		} else if bytes.Equal(pak.Key, []byte("uuid")) {
			writePacketToSource(source, &memd.Packet{
				Magic:   memd.CmdMagicRes,
				Command: pak.Command,
				Opaque:  pak.Opaque,
				Status:  memd.StatusSuccess,
				Key:     pak.Key,
				Value:   []byte(source.SelectedBucket().ID()),
			}, start)
		} else {
			stats, err := x.getStats(string(pak.Key))
			if err != nil {
				x.writeProcErr(source, pak, err, start)
				return
			}

			for k, v := range stats {
				writePacketToSource(source, &memd.Packet{
					Magic:   memd.CmdMagicRes,
					Command: memd.CmdStat,
					Opaque:  pak.Opaque,
					Status:  memd.StatusSuccess,
					Key:     []byte(k),
					Value:   []byte(v),
				}, start)
			}
		}

		// We have to write an empty key and empty value to signal end of stream.
		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
		}, start)
	}
}

func (x *kvImplCrud) getStats(key string) (map[string]string, error) {
	if key == "" {
		return x.defaultStats(), nil
	} else if key == "memory" {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return map[string]string{
			"mem_used": strconv.Itoa(int(m.Alloc)),
			"mem_max":  strconv.Itoa(int(m.Sys)),
		}, nil
	} else if key == "tap" {
		return map[string]string{
			"ep_tap_count": "0",
		}, nil
	} else if key == "config" {
		return map[string]string{
			"ep_dcp_conn_buffer_size": "10485760",
		}, nil
	}

	return nil, kvproc.ErrDocNotFound
}

func (x *kvImplCrud) defaultStats() map[string]string {
	return map[string]string{
		"pid":                 strconv.Itoa(os.Getpid()),
		"time":                time.Now().String(),
		"version":             "9.9.9",
		"uptime":              "15554",
		"accepting_conns":     "1",
		"auth_cmds":           "0",
		"auth_errors":         "0",
		"bucket_active_conns": "1",
		"bucket_conns":        "3",
		"bytes_read":          "1108621",
		"bytes_written":       "205374436",
		"cas_badval":          "0",
		"cas_hits":            "0",
		"cas_misses":          "0",
		"mem_used":            "100000000000000000000",
		"curr_connections":    "-1",
	}
}
