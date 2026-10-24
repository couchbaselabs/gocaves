package svcimpls

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockdb"
	"github.com/couchbaselabs/gocaves/mock/mockimpl/servers"
)

// maxSyncWriteReplicas is kv_engine's default sync_writes_max_allowed_replicas.
const maxSyncWriteReplicas = 2

// defaultDurabilityTimeout is kv_engine's timeout for a synchronous write
// whose request does not name one.
const defaultDurabilityTimeout = 30 * time.Second

// durabilityPollInterval is how often a pending durable write rechecks its
// vbucket. Each check walks the vbucket's tracked durable writes under its
// lock.
const durabilityPollInterval = 10 * time.Millisecond

// durabilityCommands are the commands kv_engine accepts a durability frame on,
// those with Attribute::Durability.
var durabilityCommands = map[memd.CmdCode]bool{
	memd.CmdSet: true, memd.CmdAdd: true, memd.CmdReplace: true, memd.CmdDelete: true,
	memd.CmdIncrement: true, memd.CmdDecrement: true, memd.CmdAppend: true, memd.CmdPrepend: true,
	memd.CmdTouch: true, memd.CmdGAT: true,
	memd.CmdSubDocDictAdd: true, memd.CmdSubDocDictSet: true, memd.CmdSubDocDelete: true,
	memd.CmdSubDocReplace: true, memd.CmdSubDocArrayPushLast: true, memd.CmdSubDocArrayPushFirst: true,
	memd.CmdSubDocArrayInsert: true, memd.CmdSubDocArrayAddUnique: true, memd.CmdSubDocCounter: true,
	memd.CmdSubDocMultiMutation: true, memd.CmdSubDocReplaceBodyWithXattr: true,
}

// requestHeader is what kv_engine's request validator for a command requires
// of the request header, as it passes it to verify_header.
type requestHeader struct {
	extras    int
	anyExtras bool
	noValue   bool
	noCas     bool
	datatypes memd.DatatypeFlag
}

// durableCommandHeaders are the header requirements of the durable commands
// whose headers are checked before their frames. Each requires a key. The
// sub-document mutations check their extras after the frames.
var durableCommandHeaders = map[memd.CmdCode]requestHeader{
	memd.CmdSet:       {extras: 8, datatypes: memd.DatatypeFlagJSON | memd.DatatypeFlagCompressed},
	memd.CmdReplace:   {extras: 8, datatypes: memd.DatatypeFlagJSON | memd.DatatypeFlagCompressed},
	memd.CmdAdd:       {extras: 8, noCas: true, datatypes: memd.DatatypeFlagJSON | memd.DatatypeFlagCompressed},
	memd.CmdAppend:    {datatypes: memd.DatatypeFlagJSON | memd.DatatypeFlagCompressed},
	memd.CmdPrepend:   {datatypes: memd.DatatypeFlagJSON | memd.DatatypeFlagCompressed},
	memd.CmdDelete:    {noValue: true},
	memd.CmdIncrement: {extras: 20, noValue: true, noCas: true},
	memd.CmdDecrement: {extras: 20, noValue: true, noCas: true},
	memd.CmdTouch:     {extras: 4, noValue: true, noCas: true},
	memd.CmdGAT:       {extras: 4, noValue: true, noCas: true},

	memd.CmdSubDocDictAdd:              {anyExtras: true},
	memd.CmdSubDocDictSet:              {anyExtras: true},
	memd.CmdSubDocDelete:               {anyExtras: true},
	memd.CmdSubDocReplace:              {anyExtras: true},
	memd.CmdSubDocArrayPushLast:        {anyExtras: true},
	memd.CmdSubDocArrayPushFirst:       {anyExtras: true},
	memd.CmdSubDocArrayInsert:          {anyExtras: true},
	memd.CmdSubDocArrayAddUnique:       {anyExtras: true},
	memd.CmdSubDocCounter:              {anyExtras: true},
	memd.CmdSubDocMultiMutation:        {anyExtras: true},
	memd.CmdSubDocReplaceBodyWithXattr: {anyExtras: true},
}

// knownDatatypes are the datatype bits kv_engine defines.
const knownDatatypes = memd.DatatypeFlagJSON | memd.DatatypeFlagCompressed | memd.DatatypeFlagXattrs

// checkHeader checks the header of a request to a durable command as
// verify_header does before the frames are parsed, and returns the status and
// error context kv_engine sends when it refuses it, or success. The maximum key
// length, the datatypes the connection enabled, a compressed value and the
// xattr blob are not checked. A key is empty on the wire only when the
// connection does not encode collections into it.
func checkHeader(source mock.KvClient, pak *memd.Packet) (memd.StatusCode, string) {
	header, ok := durableCommandHeaders[pak.Command]
	if !ok {
		return memd.StatusSuccess, ""
	}
	datatype := memd.DatatypeFlag(pak.Datatype)
	if datatype&^knownDatatypes != 0 {
		return memd.StatusInvalidArgs, "Request datatype invalid"
	}
	if !header.anyExtras && len(pak.Extras) != header.extras {
		if header.extras == 0 {
			return memd.StatusInvalidArgs, "Request must not include extras"
		}
		return memd.StatusInvalidArgs, fmt.Sprintf("Request must include extras of length %d", header.extras)
	}
	collectionsEncoded := source.HasFeature(memd.FeatureCollections) && memd.IsCommandCollectionEncoded(pak.Command)
	if len(pak.Key) == 0 && !collectionsEncoded {
		return memd.StatusInvalidArgs, "Request must include key"
	}
	if header.noValue && len(pak.Value) > 0 {
		return memd.StatusInvalidArgs, "Request must not include value"
	}
	if header.noCas && pak.Cas != 0 {
		return memd.StatusInvalidArgs, "Request CAS must not be set"
	}
	if datatype&^header.datatypes != 0 {
		return memd.StatusInvalidArgs, "Request datatype invalid"
	}
	return memd.StatusSuccess, ""
}

// statusUnknownFrameInfo is kv_engine's Status::UnknownFrameInfo, which memd
// does not name.
const statusUnknownFrameInfo = memd.StatusCode(0x80)

// The frame ids kv_engine defines (cb::mcbp::request::FrameInfoId).
const (
	frameBarrier                        = 0
	frameDurabilityRequirement          = 1
	frameDcpStreamID                    = 2
	frameVBucketUUID                    = 3
	frameImpersonate                    = 4
	framePreserveTTL                    = 5
	frameImpersonateExtraPrivilege      = 6
	frameImpersonateWithTokenAuthDataID = 7
)

// checkFrames checks the frames of a request one by one, in the order sent, as
// kv_engine's parseFrameExtras does, and returns the status and the error
// context of the first it refuses, or success. The reader lists every frame in
// Packet.UnsupportedFrames. kv_engine answers durability_invalid_level for any
// durability frame of the right size it cannot parse into a requirement: level
// None, an undefined level, or a timeout of 0 or 0xffff. Its protocol
// documentation names invalid arguments for those timeouts, but the frame
// parser does not return that. Framing extras that end inside a frame are
// invalid arguments once the frames before it have passed.
func checkFrames(pak *memd.Packet) (memd.StatusCode, string) {
	impersonated := false
	for _, frame := range pak.UnsupportedFrames {
		if servers.IsInvalidFrameEncoding(frame) {
			return memd.StatusInvalidArgs, "Invalid encoding in FrameExtras"
		}
		if id := uint8(frame.Type); id == frameImpersonate || id == frameImpersonateWithTokenAuthDataID {
			if impersonated {
				return memd.StatusInvalidArgs, "Only one impersonate frame may be present"
			}
			impersonated = true
		}
		if status, context := checkFrame(pak.Command, uint8(frame.Type), frame.Data); status != memd.StatusSuccess {
			return status, context
		}
	}
	return memd.StatusSuccess, ""
}

// vbucketUUIDCommands are the commands named in gocbcore's memd package that
// kv_engine's opcode.cc gives Attribute::VBucketUuid. kv_engine accepts the
// frame on them and does not enforce it.
var vbucketUUIDCommands = map[memd.CmdCode]bool{
	memd.CmdSet: true, memd.CmdAdd: true, memd.CmdReplace: true, memd.CmdDelete: true,
	memd.CmdIncrement: true, memd.CmdDecrement: true, memd.CmdAppend: true, memd.CmdPrepend: true,
	memd.CmdSetMeta: true, memd.CmdDelMeta: true,
}

func checkFrame(command memd.CmdCode, id uint8, data []byte) (memd.StatusCode, string) {
	switch id {
	case frameBarrier:
		if len(data) > 0 {
			return memd.StatusInvalidArgs, "Barrier should not contain value"
		}
	case frameDurabilityRequirement:
		return checkDurabilityFrame(command, data)
	case frameDcpStreamID:
		if len(data) != 2 {
			return memd.StatusInvalidArgs, fmt.Sprintf("DcpStreamId invalid size:%d", len(data))
		}
	case frameVBucketUUID:
		if len(data) != 8 {
			return memd.StatusInvalidArgs, fmt.Sprintf("VBucketUuid invalid size:%d", len(data))
		}
		if !vbucketUUIDCommands[command] {
			return memd.StatusNotSupported, "This command does not support VBucketUuid"
		}
	case frameImpersonate:
		if len(data) == 0 {
			return memd.StatusInvalidArgs, "Impersonated user must be set"
		}
	case framePreserveTTL:
		if len(data) > 0 {
			return memd.StatusInvalidArgs, "PreserveTtl should not contain value"
		}
		switch command {
		case memd.CmdSet, memd.CmdReplace, memd.CmdIncrement, memd.CmdDecrement,
			memd.CmdAppend, memd.CmdPrepend, memd.CmdSubDocMultiMutation:
		default:
			return memd.StatusNotSupported, "This command does not support PreserveTtl"
		}
	case frameImpersonateExtraPrivilege:
		if len(data) == 0 {
			return memd.StatusInvalidArgs, "Privilege name must be set"
		}
	case frameImpersonateWithTokenAuthDataID:
		if len(data) != 2 {
			return memd.StatusInvalidArgs, fmt.Sprintf("ImpersonateWithTokenAuthDataId invalid size:%d", len(data))
		}
	default:
		return statusUnknownFrameInfo, ""
	}
	return memd.StatusSuccess, ""
}

// checkDurabilityFrame checks a durability frame as kv_engine's
// cb::durability::Requirements parses it.
func checkDurabilityFrame(command memd.CmdCode, data []byte) (memd.StatusCode, string) {
	if len(data) != 1 && len(data) != 3 {
		return memd.StatusInvalidArgs, fmt.Sprintf("Invalid sized buffer provided: %d", len(data))
	}
	// kv_engine constructs the timeout before it validates the level.
	if len(data) == 3 {
		switch binary.BigEndian.Uint16(data[1:]) {
		case 0:
			return memd.StatusDurabilityInvalidLevel, "Timeout(): Cannot specify bucket default timeout"
		case 0xffff:
			return memd.StatusDurabilityInvalidLevel, "Timeout(): Cannot specify an infinite timeout"
		}
	}
	switch memd.DurabilityLevel(data[0]) {
	case memd.DurabilityLevelMajority, memd.DurabilityLevelMajorityAndPersistOnMaster, memd.DurabilityLevelPersistToMajority:
	default:
		return memd.StatusDurabilityInvalidLevel, "Content represents an invalid requirement specification"
	}
	if !durabilityCommands[command] {
		return memd.StatusInvalidArgs, "The requested command does not support durability requirements"
	}
	return memd.StatusSuccess, ""
}

// durabilityError is the status kv_engine answers a durable write with when
// its requirements cannot be met.
type durabilityError memd.StatusCode

func (e durabilityError) Error() string {
	return fmt.Sprintf("durability requirements refused with status 0x%02x", uint16(e))
}

// checkDurability returns the status kv_engine's engine answers a durable write
// with when its requirements cannot be met, or success. kvproc reports it where
// the engine checks it (Engine.SetDurabilityError). A memcached bucket refuses
// durability with not_supported. A vbucket owned elsewhere is left for the
// engine to answer with not-my-vbucket. An ephemeral bucket cannot
// persist, so it refuses the persistence levels. A vbucket this node is master
// of must have at most two assigned replicas, and enough to form a majority.
func checkDurability(bucket mock.Bucket, source mock.KvClient, pak *memd.Packet, vbOwnership []int) memd.StatusCode {
	if pak.DurabilityLevelFrame == nil {
		return memd.StatusSuccess
	}

	// A memcached bucket does not support durability.
	if bucket.BucketType() == mock.BucketTypeMemcached {
		return memd.StatusNotSupported
	}

	if int(pak.Vbucket) >= len(vbOwnership) || vbOwnership[pak.Vbucket] != 0 {
		return memd.StatusSuccess
	}

	level := pak.DurabilityLevelFrame.DurabilityLevel
	if bucket.BucketType() == mock.BucketTypeEphemeral &&
		(level == memd.DurabilityLevelMajorityAndPersistOnMaster || level == memd.DurabilityLevelPersistToMajority) {
		return memd.StatusDurabilityInvalidLevel
	}
	assignedReplicas := uint(0)
	for _, node := range source.Source().Node().Cluster().Nodes() {
		ownership := bucket.VbucketOwnership(node)
		if int(pak.Vbucket) < len(ownership) && ownership[pak.Vbucket] > 0 {
			assignedReplicas++
		}
	}
	// kv_engine refuses sync writes when the vbucket's replication chain has
	// more assigned replicas than sync_writes_max_allowed_replicas, 2 by
	// default. Replicas configured but not assigned to a node do not count.
	if assignedReplicas > maxSyncWriteReplicas {
		return memd.StatusDurabilityImpossible
	}
	if assignedReplicas < majorityReplicas(bucket) {
		return memd.StatusDurabilityImpossible
	}
	return memd.StatusSuccess
}

// syncWrite describes the durable write a request from source makes, or is nil
// for a request without a durability level. Its deadline is the durability
// timeout, or kv_engine's default, from now on the bucket's mock clock, which
// replication and persistence run on.
func syncWrite(source mock.KvClient, pak *memd.Packet, bucket mock.Bucket) *mockdb.SyncWrite {
	if pak.DurabilityLevelFrame == nil {
		return nil
	}
	timeout := defaultDurabilityTimeout
	if pak.DurabilityTimeoutFrame != nil {
		timeout = pak.DurabilityTimeoutFrame.DurabilityTimeout
	}
	level := pak.DurabilityLevelFrame.DurabilityLevel
	return &mockdb.SyncWrite{
		PersistActive:   level != memd.DurabilityLevelMajority,
		PersistReplicas: level == memd.DurabilityLevelPersistToMajority,
		Replicas:        majorityReplicas(bucket),
		Deadline:        bucket.Store().Chrono().Now().Add(timeout),
		Client:          source,
	}
}

// majorityReplicas is the number of replicas that, with the active copy, make
// a majority: more than half of the active copy and its replicas.
func majorityReplicas(bucket mock.Bucket) uint {
	return (bucket.NumReplicas() + 1) / 2
}

// waitForDurability waits until a durable write is committed or aborted, as
// kv_engine's durability monitor completes it (mockdb.Vbucket.SyncWriteStateAt),
// and reports whether it was committed. The state is polled on the bucket's
// mock clock, and a write committed by its deadline is durable even when the
// poll that sees it comes later. The write is identified by its vbucket UUID
// and sequence number, so a flush or rollback that discards it ends the wait,
// not durable; so does one that lands after the write committed but before the
// poll that would see it, at most a poll interval later. The wait also ends,
// not durable, once ended reports the connection closed or the bucket removed.
func waitForDurability(store *mockdb.Bucket, vbID uint, vbUUID, seqNo uint64, ended func() bool) bool {
	vbucket := store.GetVbucket(vbID)
	chrono := store.Chrono()

	for {
		// kvService.Close waits for this goroutine, and on an ordered
		// connection so does every later request.
		if ended() {
			return false
		}

		switch vbucket.SyncWriteStateAt(vbUUID, seqNo, chrono.Now()) {
		case mockdb.SyncWriteCommitted:
			// The wait may have ended while the vbucket was read.
			return !ended()
		case mockdb.SyncWritePending:
			time.Sleep(durabilityPollInterval)
		default:
			return false
		}
	}
}
