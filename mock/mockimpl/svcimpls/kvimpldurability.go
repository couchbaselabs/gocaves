package svcimpls

import (
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

// Frame types memd leaves in Packet.UnsupportedFrames when it cannot decode
// them: a barrier frame that carries data, and a durability frame that is
// neither 1 nor 3 bytes long.
const (
	unsupportedBarrierFrame    = 0
	unsupportedDurabilityFrame = 1
)

// checkFrames checks the frames of a request as kv_engine's request
// validator parses them, and returns the status and the error context kv_engine
// sends when it refuses them, or success. A barrier frame with data, or a
// durability frame of the wrong size, is invalid arguments. kv_engine answers
// durability_invalid_level for any durability frame of the right size it cannot
// parse into a requirement: level None, an undefined level, or a timeout of 0
// or 0xffff. Its protocol documentation names invalid arguments for those
// timeouts, but the frame parser does not return that. A durability frame on a
// command without durability support is invalid arguments.
//
// Framing extras that end inside a frame are invalid arguments. The frames
// decoded before that point are checked first, as kv_engine checks each frame
// as it parses it.
func checkFrames(pak *memd.Packet) (memd.StatusCode, string) {
	if status, context := checkDecodedFrames(pak); status != memd.StatusSuccess {
		return status, context
	}
	if status, context := checkPreserveTTLFrame(pak); status != memd.StatusSuccess {
		return status, context
	}
	for _, frame := range pak.UnsupportedFrames {
		if servers.IsInvalidFrameEncoding(frame) {
			return memd.StatusInvalidArgs, "Invalid encoding in FrameExtras"
		}
	}
	return memd.StatusSuccess, ""
}

func checkDecodedFrames(pak *memd.Packet) (memd.StatusCode, string) {
	for _, frame := range pak.UnsupportedFrames {
		if servers.IsInvalidFrameEncoding(frame) {
			continue
		}
		switch frame.Type {
		case unsupportedBarrierFrame:
			return memd.StatusInvalidArgs, "Barrier should not contain value"
		case unsupportedDurabilityFrame:
			return memd.StatusInvalidArgs, fmt.Sprintf("Invalid sized buffer provided: %d", len(frame.Data))
		}
	}
	if pak.DurabilityLevelFrame == nil {
		return memd.StatusSuccess, ""
	}
	// kv_engine constructs the timeout before it validates the level.
	if frame := pak.DurabilityTimeoutFrame; frame != nil {
		switch frame.DurabilityTimeout / time.Millisecond {
		case 0:
			return memd.StatusDurabilityInvalidLevel, "Timeout(): Cannot specify bucket default timeout"
		case 0xffff:
			return memd.StatusDurabilityInvalidLevel, "Timeout(): Cannot specify an infinite timeout"
		}
	}
	switch pak.DurabilityLevelFrame.DurabilityLevel {
	case memd.DurabilityLevelMajority, memd.DurabilityLevelMajorityAndPersistOnMaster, memd.DurabilityLevelPersistToMajority:
	default:
		return memd.StatusDurabilityInvalidLevel, "Content represents an invalid requirement specification"
	}
	if !durabilityCommands[pak.Command] {
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
