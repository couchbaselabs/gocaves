package svcimpls

import (
	"crypto/rand"
	"fmt"
	"log"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
)

// insertOrUpsert stands for kv_engine's requireInsertOrUpsert: Insert, or
// else Upsert.
const insertOrUpsert = "Insert|Upsert"

// kvPrivilegeChains are the privileges kv_engine's McbpPrivilegeChains
// require of each command, in order, with that the STAT executor requires.
var kvPrivilegeChains = map[memd.CmdCode][]string{
	memd.CmdGet:                 {"Read"},
	memd.CmdGetReplica:          {"Read"},
	memd.CmdGetLocked:           {"Read"},
	memd.CmdUnlockKey:           {"Read"},
	memd.CmdGetRandom:           {"Read"},
	memd.CmdSubDocMultiLookup:   {"Read"},
	memd.CmdSet:                 {"Upsert"},
	memd.CmdReplace:             {"Upsert"},
	memd.CmdAppend:              {"Upsert"},
	memd.CmdPrepend:             {"Upsert"},
	memd.CmdTouch:               {"Upsert"},
	memd.CmdAdd:                 {insertOrUpsert},
	memd.CmdDelete:              {"Delete"},
	memd.CmdIncrement:           {"Read", "Upsert"},
	memd.CmdDecrement:           {"Read", "Upsert"},
	memd.CmdGAT:                 {"Read", "Upsert"},
	memd.CmdSubDocMultiMutation: {"Read", "Upsert"},
	memd.CmdGetMeta:             {"MetaRead"},
	memd.CmdObserveSeqNo:        {"MetaRead"},
	memd.CmdStat:                {"SimpleStats"},
}

// kvCommandNames are kv_engine's names of the commands, from
// cb::mcbp::to_string.
var kvCommandNames = map[memd.CmdCode]string{
	memd.CmdGet:                 "GET",
	memd.CmdGetReplica:          "GET_REPLICA",
	memd.CmdGetLocked:           "GET_LOCKED",
	memd.CmdUnlockKey:           "UNLOCK_KEY",
	memd.CmdGetRandom:           "GET_RANDOM_KEY",
	memd.CmdSubDocMultiLookup:   "SUBDOC_MULTI_LOOKUP",
	memd.CmdSet:                 "SET",
	memd.CmdReplace:             "REPLACE",
	memd.CmdAppend:              "APPEND",
	memd.CmdPrepend:             "PREPEND",
	memd.CmdTouch:               "TOUCH",
	memd.CmdAdd:                 "ADD",
	memd.CmdDelete:              "DELETE",
	memd.CmdIncrement:           "INCREMENT",
	memd.CmdDecrement:           "DECREMENT",
	memd.CmdGAT:                 "GAT",
	memd.CmdSubDocMultiMutation: "SUBDOC_MULTI_MUTATION",
	memd.CmdGetMeta:             "GET_META",
	memd.CmdObserveSeqNo:        "OBSERVE_SEQNO",
	memd.CmdStat:                "STAT",
	memd.CmdGetClusterConfig:    "GET_CLUSTER_CONFIG",
}

// checkKvPrivileges checks the privileges the command requires as kv_engine
// does before it executes it, and answers it when the user lacks one.
//
// A connection that has not authenticated has none. Without a bucket the
// command goes on to fail with no_bucket. A user with privileges on scopes or
// collections of the bucket has the scope of the request's collection looked
// up first, as kv_engine's validator does, and is checked against it; any
// other user is checked against the bucket alone. A privilege the user lacks
// is no_access, unless the user has no privilege on the scope or collection,
// which is unknown_collection.
func (x *kvImplCrud) checkKvPrivileges(source mock.KvClient, pak *memd.Packet, start time.Time) bool {
	chain, ok := kvPrivilegeChains[pak.Command]
	if !ok {
		return true
	}
	cluster := source.Source().Node().Cluster()
	user := cluster.Users().GetUser(source.AuthenticatedUserName())
	bucket := source.SelectedBucket()
	if bucket == nil {
		if source.AuthenticatedUserName() != "" {
			return true
		}
		writeAccessError(source, pak, chain[0], start)
		return false
	}

	var privileges *mockauth.Privileges
	if user != nil {
		privileges = user.BucketPrivileges(mock.ClusterResources{Cluster: cluster}, bucket.Name())
	}
	scoped := false
	scope, collection := "", ""
	var manifestUID uint64
	if memd.IsCommandCollectionEncoded(pak.Command) && bucket.BucketType() != mock.BucketTypeMemcached &&
		privileges.HasScopePrivileges() {
		scoped = true
		scope, collection = "_default", "_default"
		if cid := pak.CollectionID; cid != 0 && source.HasFeature(memd.FeatureCollections) {
			manifest := bucket.CollectionManifest()
			// kv_engine's get_scope_id reports no manifest uid for an
			// unknown collection.
			if scope, collection = manifest.GetByID(cid); scope == "" {
				x.writeUnknownCollectionReply(source, pak, memd.StatusCollectionUnknown, 0, start)
				return false
			}
			manifestUID = manifest.UID()
		}
	}

	for _, privilege := range chain {
		access := privileges.Check(privilege, scoped, scope, collection)
		if privilege == insertOrUpsert {
			privilege = "Upsert"
			if access = privileges.Check("Insert", scoped, scope, collection); access != mockauth.AccessOk {
				access = privileges.Check(privilege, scoped, scope, collection)
			}
		}
		switch access {
		case mockauth.AccessFail:
			writeAccessError(source, pak, privilege, start)
			return false
		case mockauth.AccessFailNoPrivileges:
			// execute_client_request_packet remaps the status only to learn
			// whether to disconnect, which unknown_collection never does,
			// and sends it as it is.
			writePacketAsIs(source, unknownCollectionResponse(pak, memd.StatusCollectionUnknown, manifestUID), start)
			return false
		}
	}
	return true
}

// newErrorRef returns the reference of an error response: the event id of
// kv_engine's cookie, which cb::uuid::random of platform 7.0.5 makes a random
// UUID marked as version 4, without the variant marked.
func newErrorRef() string {
	ref := make([]byte, 16)
	if _, err := rand.Read(ref); err != nil {
		log.Printf("failed to generate an error reference: %s", err)
	}
	ref[6] = ref[6]&0x0f | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", ref[0:4], ref[4:6], ref[6:8], ref[8:10], ref[10:16])
}

// errorRefResponse answers pak with status and an error reference.
func errorRefResponse(pak *memd.Packet, status memd.StatusCode) *memd.Packet {
	return &memd.Packet{
		Magic:    memd.CmdMagicRes,
		Command:  pak.Command,
		Opaque:   pak.Opaque,
		Status:   status,
		Datatype: uint8(memd.DatatypeFlagJSON),
		Value:    []byte(fmt.Sprintf(`{"error":{"ref":"%s"}}`, newErrorRef())),
	}
}

// writeAccessError answers a command with no_access and the error context
// kv_engine's Cookie::checkPrivilege sets, with a reference for the audit
// event it logs.
func writeAccessError(source mock.KvClient, pak *memd.Packet, privilege string, start time.Time) {
	var body struct {
		Error struct {
			Context string `json:"context"`
			Ref     string `json:"ref"`
		} `json:"error"`
	}
	body.Error.Context = fmt.Sprintf("Authorization failure: can't execute %s operation without the %s privilege",
		kvCommandNames[pak.Command], privilege)
	body.Error.Ref = newErrorRef()
	value := marshalJSON(body)
	writePacketToSource(source, &memd.Packet{
		Magic:    memd.CmdMagicRes,
		Command:  pak.Command,
		Opaque:   pak.Opaque,
		Status:   memd.StatusAccessError,
		Datatype: uint8(memd.DatatypeFlagJSON),
		Value:    value,
	}, start)
}
