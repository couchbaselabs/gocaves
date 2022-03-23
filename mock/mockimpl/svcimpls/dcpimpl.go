package svcimpls

import (
	"encoding/binary"
	"fmt"
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

// requireRollback calculates whether a rollback is required when a DCP feed is starting
// https://github.com/couchbase/kv_engine/blob/master/docs/dcp/documentation/rollback.md
func requireRollback(startSeqNo, consumerVBUUID, producerVBUUID uint64) (rollbackRequired bool, rollbacKPoint uint64) {
	// 1.a. --> Consumer has no history so no rollback required
	if startSeqNo == 0 && consumerVBUUID == 0 {
		rollbackRequired = false
		return
	}

	// 1.b. Consumer has history at seqNo 0
	// If so rollback required but run 3/4 to determine how far back
	if startSeqNo == 0 && consumerVBUUID != 0 {
		// 3 & 4 need to be checked...
	}

	// 3. Diverging history rollback to 0
	if consumerVBUUID != producerVBUUID {
		rollbackRequired = true
		rollbacKPoint = 0
		return
	}

	// 4. - Currently not supported as we don't have concept of storing 'old' vb uuids in a failover table
	return
}

func (dcp *dcpImpl) handleStreamRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	// log.Printf("DCP Stream Request")
	vbucket := source.SelectedBucket().Store().GetVbucket(uint(pak.Vbucket))

	if pak.Vbucket == 59 {
		fmt.Println("x")
	}

	flags := binary.BigEndian.Uint64(pak.Extras[0:])
	startSeqNo := binary.BigEndian.Uint64(pak.Extras[8:])
	endSeqNo := binary.BigEndian.Uint64(pak.Extras[16:])
	vbUUID := binary.BigEndian.Uint64(pak.Extras[24:])
	snapshotStartSeqNo := binary.BigEndian.Uint64(pak.Extras[32:])
	snapshotEndSeqNo := binary.BigEndian.Uint64(pak.Extras[40:])

	// Not currently used...
	_ = flags
	_ = snapshotStartSeqNo
	_ = snapshotEndSeqNo

	rollbackRequired, rollbackPoint := requireRollback(startSeqNo, vbUUID, vbucket.CurrentMetaState(0).VbUUID)

	if rollbackRequired && rollbackPoint == 0 && startSeqNo == 0 {
		rollbackRequired = false
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
	}

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

	// If the start sequence is not at the end of the previously sent snapshot then the snapshot failed to send
	// completely. Therefore, we need to re-send this snapshot.
	// Should probably get done as part of rollback calculation? ie. rollback with snapshotStartSeqNo as the rollback
	// point
	if startSeqNo != snapshotEndSeqNo {
		startSeqNo = snapshotStartSeqNo
	}

	if startSeqNo != endSeqNo {
		docs, highSeqno, _ := getDocumentFromVBucket(source.SelectedBucket(), uint(pak.Vbucket), startSeqNo, endSeqNo)
		endSeqNo = minUint64(highSeqno, endSeqNo)
		if endSeqNo == 0 {
			goto end
		}

		if len(docs) > 1 {
			fmt.Println("")
		}

		sendSnapshotMarker(source, start, pak.Vbucket, pak.Opaque, startSeqNo, endSeqNo)
		for _, doc := range docs {
			if string(doc.Key) == "TestImportDecimalScale0" {
				fmt.Println("x")
			}
			sendData(source, start, pak.Opaque, doc)
		}
	}
end:
	sendEndStream(source, start, pak.Vbucket, pak.Opaque)
}

func getDocumentFromVBucket(bucket mock.Bucket, vbIdx uint, startSeqNo, endSeqNo uint64) ([]*mockdb.Document, uint64, error) {
	vBucket := bucket.Store().GetVbucket(vbIdx)
	highSeqNo := vBucket.GetHighSeqNo()

	endSeqNo = minUint64(highSeqNo, endSeqNo)
	vbDocs, _, err := vBucket.GetAllWithin(0, startSeqNo, endSeqNo)
	if err != nil {
		return nil, 0, err
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

	return flipped, highSeqNo, nil
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

func sendData(source mock.KvClient, start time.Time, opaque uint32, doc *mockdb.Document) {
	if doc.IsDeleted {
		sendDeletion(source, start, opaque, doc)
	} else {
		sendMutation(source, start, opaque, doc)
	}
}

func sendDeletion(source mock.KvClient, start time.Time, opaque uint32, doc *mockdb.Document) {
	mutationExtrasBuf := make([]byte, 18)
	binary.BigEndian.PutUint64(mutationExtrasBuf[0:], doc.SeqNo) // by_seqno
	binary.BigEndian.PutUint64(mutationExtrasBuf[8:], 0)         // rev seqno
	binary.BigEndian.PutUint16(mutationExtrasBuf[16:], 0)
	// 16-18 Extended metadata

	dataType := uint8(0)
	var value []byte

	if len(doc.Xattrs) > 0 {
		_, value = encodeDocForDCP(doc, true)
		dataType = 0x04
	}

	writePacketToSource(source, &memd.Packet{
		Magic:    memd.CmdMagicReq,
		Command:  memd.CmdDcpDeletion,
		Datatype: dataType,
		Vbucket:  uint16(doc.VbID),
		Key:      doc.Key,
		Value:    value,
		Extras:   mutationExtrasBuf,
		Status:   memd.StatusSuccess,
		Opaque:   opaque,
		Cas:      doc.Cas,
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

	dataType, value := encodeDocForDCP(doc, false)

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
		Cas:      doc.Cas,
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

func minUint64(x, y uint64) uint64 {
	if x >= y {
		return y
	}
	return x
}

// TODO: Cleanup the whole skipDocBody thing, eg, handle when no xattr
func encodeDocForDCP(doc *mockdb.Document, skipDocBody bool) (dataType uint8, encodedVal []byte) {
	dataType = doc.Datatype
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

		encodedVal = make([]byte, 4)
		binary.BigEndian.PutUint32(encodedVal[0:], uint32(xattrsLen))
		encodedVal = append(encodedVal, xattrValues...)

		if !skipDocBody {
			encodedVal = append(encodedVal, doc.Value...)
		}

		dataType = dataType | uint8(memd.DatatypeFlagXattrs)
		return dataType, encodedVal
	}

	encodedVal = doc.Value
	return dataType, encodedVal
}
