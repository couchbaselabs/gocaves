package svcimpls

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
)

// memcachedBucketManifest is the manifest kv_engine's default_engine, the
// memcached bucket, always answers.
const memcachedBucketManifest = `{"uid" : "0",
        "scopes" : [{"name":"_default", "uid":"0",
        "collections" : [{"name":"_default","uid":"0"}]}]}`

func (x *kvImplCrud) handleManifestRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	// kv_engine requires no privilege, and leaves out what the user cannot see.
	bucket := source.SelectedBucket()
	if bucket == nil {
		x.writeStatusReply(source, pak, memd.StatusNoBucket, start)
		return
	}

	value := []byte(memcachedBucketManifest)
	if bucket.BucketType() != mock.BucketTypeMemcached {
		uid, scopes := bucket.CollectionManifest().GetManifest()
		var err error
		// Manifest::toJson walks its scope map and leaves out what the user
		// cannot see.
		scopes = visibleScopes(kvPrivileges(source, bucket), kvEngineScopeOrder(scopes))
		value, err = json.Marshal(buildKvJSONManifest(uid, scopes))
		if err != nil {
			x.writeProcErr(source, pak, err, start)
			return
		}
	}

	writePacketToSource(source, &memd.Packet{
		Magic:    memd.CmdMagicRes,
		Command:  pak.Command,
		Opaque:   pak.Opaque,
		Status:   memd.StatusSuccess,
		Datatype: uint8(memd.DatatypeFlagJSON),
		Value:    value,
	}, start)
}

// buildKvJSONManifest builds the manifest of kv_engine's Manifest::toJson:
// keys in nlohmann::json's sorted order, and maxTTL only for a collection
// that has one.
func buildKvJSONManifest(uid uint64, scopes []mock.CollectionManifestScope) kvJSONManifest {
	jsonMani := kvJSONManifest{
		Scopes: make([]kvJSONScope, 0, len(scopes)),
		UID:    strconv.FormatUint(uid, 16),
	}
	for _, scop := range scopes {
		jsonScop := kvJSONScope{
			Collections: make([]kvJSONCollection, len(scop.Collections)),
			Name:        scop.Name,
			UID:         strconv.FormatUint(uint64(scop.UID), 16),
		}
		for j, col := range scop.Collections {
			jsonScop.Collections[j] = kvJSONCollection{
				MaxTTL: col.MaxTTL,
				Name:   col.Name,
				UID:    strconv.FormatUint(uint64(col.UID), 16),
			}
		}
		jsonMani.Scopes = append(jsonMani.Scopes, jsonScop)
	}
	return jsonMani
}

type kvJSONManifest struct {
	Scopes []kvJSONScope `json:"scopes"`
	UID    string        `json:"uid"`
}

type kvJSONScope struct {
	Collections []kvJSONCollection `json:"collections"`
	Name        string             `json:"name"`
	UID         string             `json:"uid"`
}

type kvJSONCollection struct {
	MaxTTL uint32 `json:"maxTTL,omitempty"`
	Name   string `json:"name"`
	UID    string `json:"uid"`
}

// kvEngineScopeOrder orders scopes as kv_engine 7.0.5 iterates
// Manifest::scopes, a libstdc++ std::unordered_map keyed by scope id with an
// identity hash, which it fills in the order ns_server lists the scopes. It
// follows libstdc++'s _Hashtable: a node goes at the head of its bucket, or of
// the list when its bucket is empty, and _Prime_rehash_policy sets the bucket
// count.
func kvEngineScopeOrder(scopes []mock.CollectionManifestScope) []mock.CollectionManifestScope {
	var nodes []mock.CollectionManifestScope
	bucketCount, nextResize := uint64(1), uint64(0)
	link := func(scope mock.CollectionManifestScope) {
		bucket := uint64(scope.UID) % bucketCount
		for i, node := range nodes {
			if uint64(node.UID)%bucketCount == bucket {
				nodes = append(nodes[:i], append([]mock.CollectionManifestScope{scope}, nodes[i:]...)...)
				return
			}
		}
		nodes = append([]mock.CollectionManifestScope{scope}, nodes...)
	}
	for _, scope := range scopes {
		elements := uint64(len(nodes)) + 1
		if elements > nextResize {
			minBuckets := elements
			if nextResize == 0 && minBuckets < 11 {
				minBuckets = 11
			}
			if minBuckets >= bucketCount {
				bucketCount, nextResize = libstdcxxNextBucketCount(max(minBuckets+1, bucketCount*2))
				old := nodes
				nodes = nil
				for _, node := range old {
					link(node)
				}
			} else {
				nextResize = bucketCount
			}
		}
		link(scope)
	}
	return nodes
}

// libstdcxxNextBucketCount returns the bucket count, and the element count
// that triggers the next rehash, that libstdc++'s
// _Prime_rehash_policy::_M_next_bkt picks for at least n buckets.
func libstdcxxNextBucketCount(n uint64) (uint64, uint64) {
	fastBuckets := []uint64{2, 2, 2, 3, 5, 5, 7, 7, 11, 11, 11, 11, 13, 13}
	if n < uint64(len(fastBuckets)) {
		return fastBuckets[n], fastBuckets[n]
	}
	last := len(libstdcxxPrimes) - 1
	i := 6 + sort.Search(last-6, func(i int) bool { return libstdcxxPrimes[6+i] >= n })
	if i == last {
		return libstdcxxPrimes[last], math.MaxUint64
	}
	return libstdcxxPrimes[i], libstdcxxPrimes[i]
}

// libstdcxxPrimes is libstdc++'s __prime_list for a 64-bit unsigned long,
// without its closing repeat of the last prime.
var libstdcxxPrimes = []uint64{
	2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97,
	103, 109, 113, 127, 137, 139, 149, 157, 167, 179, 193, 199, 211, 227, 241, 257, 277, 293, 313,
	337, 359, 383, 409, 439, 467, 503, 541, 577, 619, 661, 709, 761, 823, 887, 953, 1031, 1109, 1193,
	1289, 1381, 1493, 1613, 1741, 1879, 2029, 2179, 2357, 2549, 2753, 2971, 3209, 3469, 3739, 4027,
	4349, 4703, 5087, 5503, 5953, 6427, 6949, 7517, 8123, 8783, 9497, 10273, 11113, 12011, 12983,
	14033, 15173, 16411, 17749, 19183, 20753, 22447, 24281, 26267, 28411, 30727, 33223, 35933, 38873,
	42043, 45481, 49201, 53201, 57557, 62233, 67307, 72817, 78779, 85229, 92203, 99733, 107897,
	116731, 126271, 136607, 147793, 159871, 172933, 187091, 202409, 218971, 236897, 256279, 277261,
	299951, 324503, 351061, 379787, 410857, 444487, 480881, 520241, 562841, 608903, 658753, 712697,
	771049, 834181, 902483, 976369, 1056323, 1142821, 1236397, 1337629, 1447153, 1565659, 1693859,
	1832561, 1982627, 2144977, 2320627, 2510653, 2716249, 2938679, 3179303, 3439651, 3721303, 4026031,
	4355707, 4712381, 5098259, 5515729, 5967347, 6456007, 6984629, 7556579, 8175383, 8844859, 9569143,
	10352717, 11200489, 12117689, 13109983, 14183539, 15345007, 16601593, 17961079, 19431899,
	21023161, 22744717, 24607243, 26622317, 28802401, 31160981, 33712729, 36473443, 39460231,
	42691603, 46187573, 49969847, 54061849, 58488943, 63278561, 68460391, 74066549, 80131819,
	86693767, 93793069, 101473717, 109783337, 118773397, 128499677, 139022417, 150406843, 162723577,
	176048909, 190465427, 206062531, 222936881, 241193053, 260944219, 282312799, 305431229, 330442829,
	357502601, 386778277, 418451333, 452718089, 489790921, 529899637, 573292817, 620239453, 671030513,
	725980837, 785430967, 849749479, 919334987, 994618837, 1076067617, 1164186217, 1259520799,
	1362662261, 1474249943, 1594975441, 1725587117, 1866894511, 2019773507, 2185171673, 2364114217,
	2557710269, 2767159799, 2993761039, 3238918481, 3504151727, 3791104843, 4101556399, 4294967291,
	6442450933, 8589934583, 12884901857, 17179869143, 25769803693, 34359738337, 51539607367,
	68719476731, 103079215087, 137438953447, 206158430123, 274877906899, 412316860387, 549755813881,
	824633720731, 1099511627689, 1649267441579, 2199023255531, 3298534883309, 4398046511093,
	6597069766607, 8796093022151, 13194139533241, 17592186044399, 26388279066581, 35184372088777,
	52776558133177, 70368744177643, 105553116266399, 140737488355213, 211106232532861,
	281474976710597, 562949953421231, 1125899906842597, 2251799813685119, 4503599627370449,
	9007199254740881, 18014398509481951, 36028797018963913, 72057594037927931, 144115188075855859,
	288230376151711717, 576460752303423433, 1152921504606846883, 2305843009213693951,
	4611686018427387847, 9223372036854775783, 18446744073709551557,
}

func (x *kvImplCrud) handleGetCollectionIDRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	// kv_engine requires no privilege, and answers a collection the user
	// cannot see as unknown.
	bucket := source.SelectedBucket()
	if bucket == nil {
		x.writeStatusReply(source, pak, memd.StatusNoBucket, start)
		return
	}

	path := pak.Key
	if len(path) == 0 {
		path = pak.Value
	}
	if strings.Count(string(path), ".") != 1 {
		x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
		return
	}
	if bucket.BucketType() == mock.BucketTypeMemcached {
		x.writeMemcachedBucketCollectionID(source, pak, string(path), start)
		return
	}

	scope, collection, _ := strings.Cut(string(path), ".")
	// kv_engine takes an empty scope or collection as _default.
	if scope == "" {
		scope = "_default"
	}
	if collection == "" {
		collection = "_default"
	}
	if !validScopeOrCollectionName(scope) {
		x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
		return
	}
	uid, cid, err := bucket.CollectionManifest().GetByName(scope, collection)
	// kv_engine checks the collection name once it has found the scope.
	if err != mock.ErrScopeNotFound && !validScopeOrCollectionName(collection) {
		x.writeStatusReply(source, pak, memd.StatusInvalidArgs, start)
		return
	}
	if err != nil {
		// kv_engine's get_collection_id sets the unknown collection error
		// context for an unknown scope as well.
		x.writeUnknownCollectionReply(source, pak, x.translateProcErr(err), uid, start)
		return
	}
	if privileges := kvPrivileges(source, bucket); privileges == nil || !privileges.CanRead(scope, collection) {
		x.writeUnknownCollectionReply(source, pak, memd.StatusCollectionUnknown, uid, start)
		return
	}

	x.writeCollectionID(source, pak, uid, cid, start)
}

// writeMemcachedBucketCollectionID answers GET_COLLECTION_ID for a memcached
// bucket as kv_engine's default_engine does: it knows the default collection
// only, at manifest uid 0, and does not check names.
func (x *kvImplCrud) writeMemcachedBucketCollectionID(source mock.KvClient, pak *memd.Packet, path string, start time.Time) {
	switch {
	case path == "_default._default" || path == "._default" || path == "." || path == "_default.":
		x.writeCollectionID(source, pak, 0, 0, start)
	case strings.HasPrefix(path, "_default."):
		x.writeUnknownCollectionReply(source, pak, memd.StatusCollectionUnknown, 0, start)
	default:
		x.writeUnknownCollectionReply(source, pak, memd.StatusScopeUnknown, 0, start)
	}
}

func (x *kvImplCrud) writeCollectionID(source mock.KvClient, pak *memd.Packet, uid uint64, cid uint32, start time.Time) {
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

// kvPrivileges returns the privileges of the connection's user on bucket, or
// nil when it has none.
func kvPrivileges(source mock.KvClient, bucket mock.Bucket) *mockauth.Privileges {
	cluster := source.Source().Node().Cluster()
	user := cluster.Users().GetUser(source.AuthenticatedUserName())
	if user == nil {
		return nil
	}
	return user.BucketPrivileges(mock.ClusterResources{Cluster: cluster}, bucket.Name())
}

// visibleScopes filters scopes as kv_engine's get_collection_manifest does: a
// collection is listed if the user can see it, and a scope if the user can see
// it or a collection in it.
func visibleScopes(privileges *mockauth.Privileges, scopes []mock.CollectionManifestScope) []mock.CollectionManifestScope {
	if privileges == nil {
		return nil
	}
	var visible []mock.CollectionManifestScope
	for _, scope := range scopes {
		var collections []mock.CollectionManifestCollection
		for _, collection := range scope.Collections {
			if privileges.CanRead(scope.Name, collection.Name) {
				collections = append(collections, collection)
			}
		}
		if len(collections) > 0 || privileges.CanRead(scope.Name, "") {
			scope.Collections = collections
			visible = append(visible, scope)
		}
	}
	return visible
}

// validScopeOrCollectionName reports whether kv_engine's Manifest::validName
// accepts a scope or collection name.
func validScopeOrCollectionName(name string) bool {
	if name == "" || len(name) > 251 || name[0] == '$' {
		return false
	}
	for _, c := range []byte(name) {
		if !('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' ||
			c == '_' || c == '-' || c == '%' || c == '$') {
			return false
		}
	}
	return true
}
