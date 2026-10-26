package svcimpls

import (
	"log"
	"math/rand"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

// writePacketToSource writes a response as kv_engine's executors send one,
// through Connection::remapErrorCode: a connection without XERROR gets the
// statuses older clients know instead of some, and is disconnected instead
// of getting others.
func writePacketToSource(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if pak.Magic == memd.CmdMagicRes && !source.HasFeature(memd.FeatureXerror) {
		status, ok := remapStatus(source, pak)
		if !ok {
			if err := source.Close(); err != nil {
				log.Printf("failed to disconnect %s: %s", source.RemoteAddr(), err)
			}
			return
		}
		pak.Status = status
	}
	writePacketAsIs(source, pak, start)
}

// remapStatus is Connection::remapErrorCode for a connection without XERROR.
// It returns false for a status that disconnects the client.
func remapStatus(source mock.KvClient, pak *memd.Packet) (memd.StatusCode, bool) {
	switch pak.Status {
	case memd.StatusLocked:
		// GET_LOCKED, UNLOCK_KEY and sub-document commands fail with
		// locked_tmpfail, the others with locked.
		switch pak.Command {
		case memd.CmdGetLocked, memd.CmdUnlockKey, memd.CmdSubDocMultiMutation, memd.CmdSubDocMultiLookup:
			return memd.StatusTmpFail, true
		}
		return memd.StatusKeyExists, true
	case memd.StatusSyncWriteInProgress, memd.StatusSyncWriteReCommitInProgress:
		return memd.StatusTmpFail, true
	case memd.StatusCollectionUnknown, memd.StatusScopeUnknown:
		if !source.HasFeature(memd.FeatureCollections) {
			return memd.StatusInvalidArgs, true
		}
	case memd.StatusAccessError, memd.StatusNoBucket, memd.StatusAuthStale, memd.StatusDurabilityInvalidLevel,
		memd.StatusDurabilityImpossible, memd.StatusSyncWriteAmbiguous, memd.StatusDCPStreamIDInvalid:
		return pak.Status, false
	}
	return pak.Status, true
}

// writePacketAsIs writes a packet without remapping its status, as kv_engine
// sends the responses of its request validators.
func writePacketAsIs(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if source.HasFeature(memd.FeatureDurations) {
		// TODO (chvck): revisit this, for some reason Windows reports a server duration of 0.
		// Golang time accuracy in Windows is good enough that this shouldn't be the case and it seems pretty unlikely
		// that we've actually done the operation in no time.
		duration := time.Since(start)
		if duration < 1*time.Microsecond {
			duration = time.Duration(rand.Intn(200)+1) * time.Microsecond
		}
		pak.ServerDurationFrame = &memd.ServerDurationFrame{
			ServerDuration: duration,
		}
	}
	err := source.WritePacket(pak)
	if err != nil {
		log.Printf("failed to write %s response to %s: %s", pak.Command.Name(), source.RemoteAddr(), err)
	}
}
