package svcimpls

import (
	"encoding/binary"
	"log"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockdb"
)

type dcpImpl struct{}

func (dcp *dcpImpl) Register(h *hookHelper) {
	h.RegisterKvHandler(memd.CmdDcpOpenConnection, dcp.handleOpenDCPConnection)
	h.RegisterKvHandler(memd.CmdDcpStreamReq, dcp.handleStreamRequest)
	h.RegisterKvHandler(memd.CmdDcpControl, dcp.handleDCPControl)
	h.RegisterKvHandler(memd.CmdDcpGetFailoverLog, dcp.handleFailoverLog)
}

func (dcp *dcpImpl) handleOpenDCPConnection(source mock.KvClient, pak *memd.Packet, start time.Time) {
	log.Printf("DCP Open Connection command")
	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdDcpOpenConnection,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
	}, start)
}

func (dcp *dcpImpl) handleStreamRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	log.Printf("DCP Stream Request")
	vbucket := source.SelectedBucket().Store().GetVbucket(uint(pak.Vbucket))

	flags := binary.BigEndian.Uint64(pak.Extras[0:])
	startSeqNo := binary.BigEndian.Uint64(pak.Extras[8:])
	endSeqNo := binary.BigEndian.Uint64(pak.Extras[16:])
	vbUUID := binary.BigEndian.Uint64(pak.Extras[24:])
	snapshotStartSeqNo := binary.BigEndian.Uint64(pak.Extras[32:])
	snapshotEndSeqNo := binary.BigEndian.Uint64(pak.Extras[40:])

	_ = flags
	_ = snapshotStartSeqNo
	_ = snapshotEndSeqNo

	// Decide on whether we should rollback
	rollbackRequired := false
	rollbackPoint := uint64(0)
	if startSeqNo == 0 && vbUUID != 0 {
		if vbucket.CurrentMetaState(0).VbUUID != vbUUID {
			rollbackRequired = true
			rollbackPoint = 0
		}
	}

	if vbucket.CurrentMetaState(0).VbUUID != vbUUID && startSeqNo != 0 {
		rollbackRequired = true
		rollbackPoint = 0
	}

	if rollbackRequired {
		streamReqValue := make([]byte, 8)
		binary.BigEndian.PutUint64(streamReqValue[0:], rollbackPoint)
		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdDcpStreamReq,
			Opaque:  pak.Opaque,
			Status:  memd.StatusRollback,
			Value:   streamReqValue,
		}, start)
		return // Can return as this will trigger another request with updated sequence
	} else {
		streamReqValue := make([]byte, 16)
		// These items are a 'failover log' these should be modified if this is properly implemented
		binary.BigEndian.PutUint64(streamReqValue[0:], vbucket.CurrentMetaState(0).VbUUID)
		binary.BigEndian.PutUint64(streamReqValue[8:], 0)
		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdDcpStreamReq,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Value:   streamReqValue,
		}, start)
	}

	// If the start sequence is not at the end of the previously sent snapshot then the snapshot failed to send
	// completely. Therefore, we need to re-send this snapshot.
	if startSeqNo != snapshotEndSeqNo {
		startSeqNo = snapshotStartSeqNo
	}

	if startSeqNo != endSeqNo {
		docs, _ := getDocumentFromVBucket(source.SelectedBucket(), uint(pak.Vbucket), startSeqNo, endSeqNo)
		sendSnapshotMarker(source, start, pak.Vbucket, pak.Opaque, startSeqNo, endSeqNo)
		for _, doc := range docs {
			if doc.SeqNo < startSeqNo {
				break
			}
			if doc.SeqNo > endSeqNo {
				break
			}
			sendMutation(source, start, pak.Opaque, doc)
		}
	}

	sendEndStream(source, start, pak.Vbucket, pak.Opaque)
}

func getDocumentFromVBucket(bucket mock.Bucket, vbIdx uint, startSeqNo, endSeqNo uint64) ([]*mockdb.Document, error) {
	vBucket := bucket.Store().GetVbucket(vbIdx)
	vbDocs, _, err := vBucket.GetAllWithin(0, startSeqNo, endSeqNo)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{})
	docs := make([]*mockdb.Document, 0)
	for i := len(vbDocs) - 1; i >= 0; i-- {
		_, ok := seen[string(vbDocs[i].Key)]
		if !ok {
			docs = append(docs, vbDocs[i])
			seen[string(vbDocs[i].Key)] = struct{}{}
		}
	}

	// DCP requires ordered docs so flip the above... again?
	flipped := make([]*mockdb.Document, len(docs))
	for idx, doc := range docs {
		flipped[len(docs)-idx-1] = doc
	}

	return flipped, nil
}

func (dcp *dcpImpl) handleDCPControl(source mock.KvClient, pak *memd.Packet, start time.Time) {
	log.Printf("DCP Control Request")
	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdDcpControl,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
	}, start)
}

func (dcp *dcpImpl) handleFailoverLog(source mock.KvClient, pak *memd.Packet, start time.Time) {
	failoverLog := getFailoverLog(source.SelectedBucket().Store().GetVbucket(uint(pak.Vbucket)))
	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdDcpGetFailoverLog,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
		Value:   failoverLog,
	}, start)

}

// These items are a 'failover log' these should be modified if this is properly implemented

func getFailoverLog(vbucket *mockdb.Vbucket) []byte {
	streamReqValue := make([]byte, 16)
	binary.BigEndian.PutUint64(streamReqValue[0:], vbucket.CurrentMetaState(0).VbUUID)
	binary.BigEndian.PutUint64(streamReqValue[8:], 0)
	return streamReqValue
}

func sendSnapshotMarker(source mock.KvClient, start time.Time, vbucket uint16, opaque uint32, startSeqNo, endSeqNo uint64) {
	extrasBuf := make([]byte, 20)
	binary.BigEndian.PutUint64(extrasBuf[0:], startSeqNo) // Start seqno
	binary.BigEndian.PutUint64(extrasBuf[8:], endSeqNo)   // End seqno
	binary.BigEndian.PutUint32(extrasBuf[16:], 1)         // Snapshot type

	// Snapshot Marker
	writePacketToSource(source, &memd.Packet{
		Magic:    memd.CmdMagicReq,
		Command:  memd.CmdDcpSnapshotMarker,
		Vbucket:  vbucket,
		Datatype: 0,
		Extras:   extrasBuf,
		Status:   memd.StatusSuccess,
		Opaque:   opaque,
	}, start)
}

func sendMutation(source mock.KvClient, start time.Time, opaque uint32, doc *mockdb.Document) {
	mutationExtrasBuf := make([]byte, 28)
	binary.BigEndian.PutUint64(mutationExtrasBuf[0:], doc.SeqNo) // by_seqno
	binary.BigEndian.PutUint64(mutationExtrasBuf[8:], 0)         // rev seqno
	binary.BigEndian.PutUint32(mutationExtrasBuf[16:], 0)        // flags
	binary.BigEndian.PutUint32(mutationExtrasBuf[20:], 0)        // expiration
	binary.BigEndian.PutUint32(mutationExtrasBuf[24:], 0)        // lock time
	// Metadata?

	dataType := doc.Datatype
	var value []byte

	if len(doc.Xattrs) > 0 {
		var xattrValues []byte
		for xattrK, xattrV := range doc.Xattrs {
			xattrChunk := []byte(xattrK)
			xattrChunk = append(xattrChunk, byte(0))
			xattrChunk = append(xattrChunk, xattrV...)
			xattrChunk = append(xattrChunk, byte(0))

			xattrChunkLen := make([]byte, 4)
			binary.BigEndian.PutUint32(xattrChunkLen[0:], uint32(len(xattrChunk)))
			xattrChunk = append(xattrChunkLen, xattrChunk...)

			xattrValues = append(xattrValues, xattrChunk...)
		}

		xattrsLen := len(xattrValues)

		value = make([]byte, 4)
		binary.BigEndian.PutUint32(value[0:], uint32(xattrsLen))
		value = append(value, xattrValues...)
		value = append(value, doc.Value...)

		dataType = dataType | uint8(memd.DatatypeFlagXattrs)
	} else {
		value = doc.Value
	}

	// Send mutation
	writePacketToSource(source, &memd.Packet{
		Magic:    memd.CmdMagicReq,
		Command:  memd.CmdDcpMutation,
		Datatype: dataType,
		Vbucket:  uint16(doc.VbID),
		Key:      doc.Key,
		Value:    value,
		Extras:   mutationExtrasBuf,
		Status:   memd.StatusSuccess,
		Opaque:   opaque,
	}, start)

}

func sendEndStream(source mock.KvClient, start time.Time, vbucket uint16, opaque uint32) {
	streamEndExtrasBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(streamEndExtrasBuf[0:], 0) // Flags 0 == OK

	// Stream End
	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicReq,
		Command: memd.CmdDcpStreamEnd,
		Vbucket: vbucket,
		Status:  memd.StatusSuccess,
		Opaque:  opaque,
		Extras:  streamEndExtrasBuf,
	}, start)
}
