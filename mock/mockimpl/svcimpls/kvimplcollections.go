package svcimpls

import (
	"encoding/binary"
	"encoding/json"
	"strings"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
)

func (x *kvImplCrud) handleManifestRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionBucketManage, start); proc != nil {
		manifest := source.SelectedBucket().CollectionManifest()
		uid, scopes := manifest.GetManifest()

		jsonMani := buildJSONManifest(uid, scopes)
		b, err := json.Marshal(jsonMani)
		if err != nil {
			x.writeProcErr(source, pak, err, start)
			return
		}

		writePacketToSource(source, &memd.Packet{
			Magic:    memd.CmdMagicRes,
			Command:  pak.Command,
			Opaque:   pak.Opaque,
			Status:   memd.StatusSuccess,
			Datatype: uint8(memd.DatatypeFlagJSON),
			Value:    b,
		}, start)
	}
}

func (x *kvImplCrud) handleGetCollectionIDRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if proc := x.makeProc(source, pak, mockauth.PermissionBucketManage, start); proc != nil {
		keyParts := strings.Split(string(pak.Key), ".")
		if len(keyParts) != 2 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
			return
		}
		manifest := source.SelectedBucket().CollectionManifest()
		uid, cid, err := manifest.GetByName(keyParts[0], keyParts[1])
		if err != nil {
			x.writeProcErr(source, pak, err, start)
		}

		extrasBuf := make([]byte, 12)
		binary.BigEndian.PutUint64(extrasBuf[0:], uid)
		binary.BigEndian.PutUint32(extrasBuf[8:], cid)

		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Extras:  extrasBuf,
		}, start)
	}
}
