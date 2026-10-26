package mockimpl

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

// testManifest is the collections manifest JSON of ns_server and kv_engine.
type testManifest struct {
	UID    string `json:"uid"`
	Scopes []struct {
		UID         string `json:"uid"`
		Name        string `json:"name"`
		Collections []struct {
			UID  string `json:"uid"`
			Name string `json:"name"`
		} `json:"collections"`
	} `json:"scopes"`
}

// uids maps "scope" and "scope.collection" to their uids.
func (m testManifest) uids() map[string]string {
	uids := make(map[string]string)
	for _, scope := range m.Scopes {
		uids[scope.Name] = scope.UID
		for _, collection := range scope.Collections {
			uids[scope.Name+"."+collection.Name] = collection.UID
		}
	}
	return uids
}

// addTestCollections adds scope "s" and collections "c0" to "c<n-1>" in it,
// n+1 manifest changes.
func addTestCollections(t *testing.T, manifest *mock.CollectionManifest, n int) {
	t.Helper()
	if _, err := manifest.AddScope("s"); err != nil {
		t.Fatalf("failed to add scope: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err := manifest.AddCollection("s", fmt.Sprintf("c%d", i), 0); err != nil {
			t.Fatalf("failed to add collection: %v", err)
		}
	}
}

// testCollectionUids returns the uids of the default scope and collection,
// scope "s" and collections "c0" to "c<n-1>" when they were added first.
func testCollectionUids(n int) map[string]string {
	uids := map[string]string{
		"_default":          "0",
		"_default._default": "0",
		"s":                 "8",
	}
	for i := 0; i < n; i++ {
		uids[fmt.Sprintf("s.c%d", i)] = fmt.Sprintf("%x", i+8)
	}
	return uids
}

func newTestMgmtCluster(t *testing.T) (mock.Cluster, string) {
	cluster, err := NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })

	return cluster, fmt.Sprintf("http://127.0.0.1:%d/pools/default",
		cluster.Nodes()[0].MgmtService().ListenPort())
}

func testMgmtRequest(t *testing.T, baseURL, method, path string, form url.Values) []byte {
	t.Helper()

	req, err := http.NewRequest(method, baseURL+path, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("failed to build request: %v", err)
	}
	req.SetBasicAuth("Administrator", "password")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s failed: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s %s: expected status 200, got %d with body %s", method, path, resp.StatusCode, body)
	}
	return body
}

func getTestManifest(t *testing.T, baseURL string) testManifest {
	t.Helper()
	body := testMgmtRequest(t, baseURL, http.MethodGet, "/buckets/default/scopes", nil)
	var manifest testManifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		t.Fatalf("manifest %s is not JSON: %v", body, err)
	}
	return manifest
}

// TestCollectionsRESTChangeUidsAreHex checks the uid each scope and
// collection create and drop answers: the uid of the manifest it creates, as
// collections:convert_uid_to_memcached/1 formats it.
func TestCollectionsRESTChangeUidsAreHex(t *testing.T) {
	_, baseURL := newTestMgmtCluster(t)

	change := func(method, path string, form url.Values, expected string) {
		t.Helper()
		body := testMgmtRequest(t, baseURL, method, path, form)
		var parsed map[string]interface{}
		if err := json.Unmarshal(body, &parsed); err != nil {
			t.Fatalf("body %s is not JSON: %v", body, err)
		}
		if !reflect.DeepEqual(parsed, map[string]interface{}{"uid": expected}) {
			t.Fatalf("%s %s: expected uid %q, got %s", method, path, expected, body)
		}
	}

	change(http.MethodPost, "/buckets/default/scopes", url.Values{"name": {"s"}}, "1")
	for i := 0; i < 12; i++ {
		change(http.MethodPost, "/buckets/default/scopes/s/collections",
			url.Values{"name": {fmt.Sprintf("c%d", i)}}, fmt.Sprintf("%x", i+2))
	}
	change(http.MethodDelete, "/buckets/default/scopes/s/collections/c11", nil, "e")
	change(http.MethodPost, "/buckets/default/scopes", url.Values{"name": {"t"}}, "f")
	change(http.MethodDelete, "/buckets/default/scopes/s", nil, "10")
}

// TestCollectionsRESTManifestUids checks the uids of GET .../scopes against
// ns_server, whose default_manifest/0 starts scope and collection ids at 8,
// and which does not reuse the id of a dropped scope or collection.
func TestCollectionsRESTManifestUids(t *testing.T) {
	cluster, baseURL := newTestMgmtCluster(t)
	manifest := cluster.GetBucket("default").CollectionManifest()
	addTestCollections(t, manifest, 12)

	got := getTestManifest(t, baseURL)
	if got.UID != "d" {
		t.Fatalf("expected manifest uid \"d\", got %q", got.UID)
	}
	if uids := got.uids(); !reflect.DeepEqual(uids, testCollectionUids(12)) {
		t.Fatalf("expected uids %v, got %v", testCollectionUids(12), uids)
	}

	if _, err := manifest.DropCollection("s", "c11"); err != nil {
		t.Fatalf("failed to drop collection: %v", err)
	}
	if _, err := manifest.AddCollection("s", "c12", 0); err != nil {
		t.Fatalf("failed to add collection: %v", err)
	}
	if uid := getTestManifest(t, baseURL).uids()["s.c12"]; uid != "14" {
		t.Fatalf("expected uid \"14\" for s.c12, got %q", uid)
	}

	if _, err := manifest.DropScope("s"); err != nil {
		t.Fatalf("failed to drop scope: %v", err)
	}
	if _, err := manifest.AddScope("t"); err != nil {
		t.Fatalf("failed to add scope: %v", err)
	}
	got = getTestManifest(t, baseURL)
	if got.UID != "11" {
		t.Fatalf("expected manifest uid \"11\", got %q", got.UID)
	}
	expected := map[string]string{"_default": "0", "_default._default": "0", "t": "9"}
	if uids := got.uids(); !reflect.DeepEqual(uids, expected) {
		t.Fatalf("expected uids %v, got %v", expected, uids)
	}
}

// TestCollectionsBucketConfigManifestUidIsHex checks collectionsManifestUid,
// which ns_server's bucket_info_cache takes from collections:uid/2.
func TestCollectionsBucketConfigManifestUidIsHex(t *testing.T) {
	cluster, baseURL := newTestMgmtCluster(t)
	addTestCollections(t, cluster.GetBucket("default").CollectionManifest(), 9)

	for _, path := range []string{"/buckets/default", "/b/default"} {
		var config struct {
			CollectionsManifestUID string `json:"collectionsManifestUid"`
		}
		body := testMgmtRequest(t, baseURL, http.MethodGet, path, nil)
		if err := json.Unmarshal(body, &config); err != nil {
			t.Fatalf("config %s is not JSON: %v", body, err)
		}
		if config.CollectionsManifestUID != "a" {
			t.Fatalf("%s: expected collectionsManifestUid \"a\", got %q", path, config.CollectionsManifestUID)
		}
	}
}

// TestCollectionsKvGetManifest checks the uids of GET_COLLECTIONS_MANIFEST,
// which kv_engine's Manifest::toJson formats in hex.
func TestCollectionsKvGetManifest(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureCollections)
	addTestCollections(t, c.cluster.GetBucket("default").CollectionManifest(), 12)

	resp := c.mustSucceed(&memd.Packet{Command: memd.CmdCollectionsGetManifest})
	var got testManifest
	if err := json.Unmarshal(resp.Value, &got); err != nil {
		t.Fatalf("manifest %s is not JSON: %v", resp.Value, err)
	}
	if got.UID != "d" {
		t.Fatalf("expected manifest uid \"d\", got %q", got.UID)
	}
	if uids := got.uids(); !reflect.DeepEqual(uids, testCollectionUids(12)) {
		t.Fatalf("expected uids %v, got %v", testCollectionUids(12), uids)
	}
}

// TestCollectionsKvGetCollectionID checks the manifest uid and collection id
// GET_COLLECTION_ID answers, and serves data operations on a collection whose
// id takes two LEB128 bytes.
func TestCollectionsKvGetCollectionID(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureCollections)
	// Ids 8 to 0x87, manifest uid 0x81.
	addTestCollections(t, c.cluster.GetBucket("default").CollectionManifest(), 128)

	for path, expected := range map[string]uint32{"s.c11": 0x13, "s.c120": 0x80} {
		resp := c.mustSucceed(&memd.Packet{Command: memd.CmdCollectionsGetID, Value: []byte(path)})
		if len(resp.Extras) != 12 {
			t.Fatalf("%s: expected 12 bytes of extras, got %d", path, len(resp.Extras))
		}
		if uid := binary.BigEndian.Uint64(resp.Extras[0:]); uid != 0x81 {
			t.Fatalf("%s: expected manifest uid 0x81, got 0x%x", path, uid)
		}
		if cid := binary.BigEndian.Uint32(resp.Extras[8:]); cid != expected {
			t.Fatalf("%s: expected collection id 0x%x, got 0x%x", path, expected, cid)
		}
	}

	for _, cid := range []uint32{0x13, 0x80} {
		c.mustSucceed(&memd.Packet{
			Command:      memd.CmdSet,
			Vbucket:      c.vbID,
			CollectionID: cid,
			Key:          []byte("doc"),
			Value:        []byte(fmt.Sprintf(`{"cid":%d}`, cid)),
			Extras:       make([]byte, 8),
		})
	}
	for _, cid := range []uint32{0x13, 0x80} {
		resp := c.mustSucceed(&memd.Packet{
			Command:      memd.CmdGet,
			Vbucket:      c.vbID,
			CollectionID: cid,
			Key:          []byte("doc"),
		})
		if value := fmt.Sprintf(`{"cid":%d}`, cid); string(resp.Value) != value {
			t.Fatalf("collection 0x%x: expected %s, got %s", cid, value, resp.Value)
		}
	}
}

// TestCollectionsKvReservedIDsUnknown checks that ids 1 to 7, which
// ns_server never assigns, are unknown collections.
func TestCollectionsKvReservedIDsUnknown(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureCollections)
	addTestCollections(t, c.cluster.GetBucket("default").CollectionManifest(), 12)

	const unknown = `{"error":{"context":"Unknown scope or collection in operation"},"manifest_uid":"d"}`
	for _, cid := range []uint32{1, 7, 0x14} {
		resp := c.Do(&memd.Packet{
			Command:      memd.CmdGet,
			Vbucket:      c.vbID,
			CollectionID: cid,
			Key:          []byte("doc"),
		})
		if resp.Status != memd.StatusCollectionUnknown || string(resp.Value) != unknown {
			t.Fatalf("collection 0x%x: expected status 0x%02x with %s, got 0x%02x with %s",
				cid, uint16(memd.StatusCollectionUnknown), unknown, uint16(resp.Status), resp.Value)
		}
	}
}

// kvCollectionsCase is a GET_COLLECTION_ID or GET_COLLECTIONS_MANIFEST request
// and kv_engine's answer to it.
type kvCollectionsCase struct {
	name string
	// noCollections leaves the collections feature off the connection.
	noCollections bool
	req           memd.Packet
	status        memd.StatusCode
	// body is the expected error body, empty for none.
	body string
	// cid is the collection id a successful GET_COLLECTION_ID answers.
	cid uint32
}

func testErrorContext(msg string) string {
	return fmt.Sprintf(`{"error":{"context":%q}}`, msg)
}

// runKvCollectionsCases sends each request on a connection of its own to
// bucket, which Administrator selects.
func runKvCollectionsCases(t *testing.T, cluster mock.Cluster, bucket string, cases []kvCollectionsCase) {
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			features := []memd.HelloFeature{memd.FeatureAltRequests, memd.FeaturePreserveExpiry}
			if !tc.noCollections {
				features = append(features, memd.FeatureCollections)
			}
			c := connectTestKv(t, cluster, features...)
			if bucket != "default" {
				c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte(bucket)})
			}
			req := tc.req
			resp := c.Do(&req)
			if resp.Status != tc.status {
				t.Fatalf("expected status 0x%02x, got 0x%02x with body %s",
					uint16(tc.status), uint16(resp.Status), resp.Value)
			}
			if tc.status == memd.StatusSuccess {
				if req.Command == memd.CmdCollectionsGetID {
					if cid := binary.BigEndian.Uint32(resp.Extras[8:]); cid != tc.cid {
						t.Fatalf("expected collection id 0x%x, got 0x%x", tc.cid, cid)
					}
				}
				return
			}
			if tc.body == "" {
				if len(resp.Value) != 0 {
					t.Fatalf("expected no body, got %s", resp.Value)
				}
				return
			}
			var got, expected interface{}
			if err := json.Unmarshal(resp.Value, &got); err != nil {
				t.Fatalf("error body %q is not JSON: %v", resp.Value, err)
			}
			if err := json.Unmarshal([]byte(tc.body), &expected); err != nil {
				t.Fatalf("expected body %s is not JSON: %v", tc.body, err)
			}
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("expected body %s, got %s", tc.body, resp.Value)
			}
		})
	}
}

func getCollectionID(path string) memd.Packet {
	return memd.Packet{Command: memd.CmdCollectionsGetID, Value: []byte(path)}
}

// TestCollectionsKvGetCollectionIDPath checks how GET_COLLECTION_ID reads its
// path against kv_engine's collections_get_id_validator, verify_header,
// collections_get_collection_id_executor and Collections::Manager.
func TestCollectionsKvGetCollectionIDPath(t *testing.T) {
	cluster, _ := newTestMgmtCluster(t)
	addTestCollections(t, cluster.GetBucket("default").CollectionManifest(), 12)

	const unknown = `{"error":{"context":"Unknown scope or collection in operation"},"manifest_uid":"d"}`
	withKey := func(req memd.Packet, key string) memd.Packet {
		req.Key = []byte(key)
		req.Value = nil
		return req
	}
	preserveTTL := func(req memd.Packet) memd.Packet {
		req.PreserveExpiryFrame = &memd.PreserveExpiryFrame{}
		return req
	}
	req := getCollectionID("s.c0")
	cas, vbucket, extras, datatype := req, req, req, req
	cas.Cas = 1
	vbucket.Vbucket = 1
	extras.Extras = []byte{0}
	datatype.Datatype = uint8(memd.DatatypeFlagJSON)
	keyAndValue := req
	keyAndValue.Key = []byte("s.c0")

	runKvCollectionsCases(t, cluster, "default", []kvCollectionsCase{
		{name: "path in value", req: getCollectionID("s.c11"), cid: 0x13},
		{name: "path in key", req: withKey(req, "s.c11"), cid: 0x13},
		{name: "default scope and collection", req: getCollectionID("."), cid: 0},
		{name: "default scope", req: getCollectionID("._default"), cid: 0},
		{name: "default collection", req: getCollectionID("_default."), cid: 0},
		{name: "default collection missing in scope", req: getCollectionID("s."),
			status: memd.StatusCollectionUnknown, body: unknown},
		{name: "unknown collection in default scope", req: getCollectionID(".c0"),
			status: memd.StatusCollectionUnknown, body: unknown},
		{name: "unknown collection", req: getCollectionID("s.missing"),
			status: memd.StatusCollectionUnknown, body: unknown},
		{name: "unknown scope", req: getCollectionID("missing.c0"),
			status: memd.StatusScopeUnknown, body: unknown},
		{name: "invalid collection name in unknown scope", req: getCollectionID("missing.$c"),
			status: memd.StatusScopeUnknown, body: unknown},
		{name: "invalid collection name", req: getCollectionID("s.$c"), status: memd.StatusInvalidArgs},
		{name: "invalid character", req: getCollectionID("s.c 0"), status: memd.StatusInvalidArgs},
		{name: "invalid scope name", req: getCollectionID("$s.c0"), status: memd.StatusInvalidArgs},
		{name: "empty path", req: getCollectionID(""), status: memd.StatusInvalidArgs},
		{name: "no dot", req: getCollectionID("s"), status: memd.StatusInvalidArgs},
		{name: "two dots", req: getCollectionID("s.c0."), status: memd.StatusInvalidArgs},
		{name: "key and value", req: keyAndValue,
			status: memd.StatusInvalidArgs, body: testErrorContext("Cannot set both key and value")},
		{name: "vbucket", req: vbucket,
			status: memd.StatusInvalidArgs, body: testErrorContext("Request vbucket id must be 0")},
		{name: "extras", req: extras,
			status: memd.StatusInvalidArgs, body: testErrorContext("Request must not include extras")},
		{name: "cas", req: cas,
			status: memd.StatusInvalidArgs, body: testErrorContext("Request CAS must not be set")},
		{name: "datatype", req: datatype,
			status: memd.StatusInvalidArgs, body: testErrorContext("Request datatype invalid")},
		{name: "key length", req: withKey(req, strings.Repeat("k", 252)),
			status: memd.StatusInvalidArgs, body: testErrorContext("Key length exceeds 251")},
		{name: "key length without collections", noCollections: true, req: withKey(req, strings.Repeat("k", 251)),
			status: memd.StatusInvalidArgs, body: testErrorContext("Key length exceeds 250")},
		// verify_header checks the header before the frames, and
		// collections_get_id_validator checks key, value and vbucket after.
		{name: "cas before frames", req: preserveTTL(cas),
			status: memd.StatusInvalidArgs, body: testErrorContext("Request CAS must not be set")},
		{name: "frames before key and value", req: preserveTTL(keyAndValue),
			status: memd.StatusNotSupported, body: testErrorContext("This command does not support PreserveTtl")},
		{name: "frames before vbucket", req: preserveTTL(vbucket),
			status: memd.StatusNotSupported, body: testErrorContext("This command does not support PreserveTtl")},
	})
}

// TestCollectionsKvGetManifestHeader checks GET_COLLECTIONS_MANIFEST against
// kv_engine's collections_get_manifest_validator and verify_header.
func TestCollectionsKvGetManifestHeader(t *testing.T) {
	cluster, _ := newTestMgmtCluster(t)

	req := memd.Packet{Command: memd.CmdCollectionsGetManifest}
	key, value, extras, cas, datatype, vbucket := req, req, req, req, req, req
	key.Key = []byte("k")
	value.Value = []byte("v")
	extras.Extras = []byte{0}
	cas.Cas = 1
	datatype.Datatype = uint8(memd.DatatypeFlagJSON)
	vbucket.Vbucket = 1
	casWithFrame, vbucketWithFrame := cas, vbucket
	casWithFrame.PreserveExpiryFrame = &memd.PreserveExpiryFrame{}
	vbucketWithFrame.PreserveExpiryFrame = &memd.PreserveExpiryFrame{}

	runKvCollectionsCases(t, cluster, "default", []kvCollectionsCase{
		{name: "valid", req: req},
		{name: "key", req: key, status: memd.StatusInvalidArgs, body: testErrorContext("Request must not include key")},
		{name: "value", req: value, status: memd.StatusInvalidArgs, body: testErrorContext("Request must not include value")},
		{name: "extras", req: extras, status: memd.StatusInvalidArgs, body: testErrorContext("Request must not include extras")},
		{name: "cas", req: cas, status: memd.StatusInvalidArgs, body: testErrorContext("Request CAS must not be set")},
		{name: "datatype", req: datatype, status: memd.StatusInvalidArgs, body: testErrorContext("Request datatype invalid")},
		{name: "vbucket", req: vbucket, status: memd.StatusInvalidArgs, body: testErrorContext("Request vbucket id must be 0")},
		{name: "cas before frames", req: casWithFrame,
			status: memd.StatusInvalidArgs, body: testErrorContext("Request CAS must not be set")},
		{name: "frames before vbucket", req: vbucketWithFrame,
			status: memd.StatusNotSupported, body: testErrorContext("This command does not support PreserveTtl")},
	})
}

// TestCollectionsKvMemcachedBucket checks GET_COLLECTIONS_MANIFEST and
// GET_COLLECTION_ID on a memcached bucket against kv_engine's default_engine,
// which knows the default collection only, at manifest uid 0, and does not
// check names.
func TestCollectionsKvMemcachedBucket(t *testing.T) {
	cluster, _ := newTestMgmtCluster(t)

	const unknown = `{"error":{"context":"Unknown scope or collection in operation"},"manifest_uid":"0"}`
	runKvCollectionsCases(t, cluster, "memd", []kvCollectionsCase{
		{name: "default", req: getCollectionID("_default._default")},
		{name: "default scope and collection", req: getCollectionID(".")},
		{name: "default scope", req: getCollectionID("._default")},
		{name: "default collection", req: getCollectionID("_default.")},
		{name: "unknown collection", req: getCollectionID("_default.c"),
			status: memd.StatusCollectionUnknown, body: unknown},
		{name: "collection in empty scope", req: getCollectionID(".c"),
			status: memd.StatusScopeUnknown, body: unknown},
		{name: "invalid scope name", req: getCollectionID("$x.y"),
			status: memd.StatusScopeUnknown, body: unknown},
		{name: "two dots", req: getCollectionID("_default._default."), status: memd.StatusInvalidArgs},
	})

	c := connectTestKv(t, cluster, memd.FeatureCollections)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("memd")})
	resp := c.mustSucceed(&memd.Packet{Command: memd.CmdCollectionsGetManifest})
	const manifest = `{"uid" : "0",
        "scopes" : [{"name":"_default", "uid":"0",
        "collections" : [{"name":"_default","uid":"0"}]}]}`
	if string(resp.Value) != manifest {
		t.Fatalf("expected manifest %s, got %s", manifest, resp.Value)
	}
}

// TestCollectionsManifestMaxTTL checks maxTTL in the manifest: ns_server's
// REST API gives every collection one, 0 when it has none
// (default_collection_props), while kv_engine's Manifest::toJson gives it only
// for a collection that has one.
func TestCollectionsManifestMaxTTL(t *testing.T) {
	cluster, baseURL := newTestMgmtCluster(t)
	manifest := cluster.GetBucket("default").CollectionManifest()
	if _, err := manifest.AddCollection("_default", "c0", 0); err != nil {
		t.Fatalf("failed to add collection: %v", err)
	}
	if _, err := manifest.AddCollection("_default", "c1", 30); err != nil {
		t.Fatalf("failed to add collection: %v", err)
	}
	// ns_server leaves out a maxTTL of 0 given on create (remove_defaults).
	testMgmtRequest(t, baseURL, http.MethodPost, "/buckets/default/scopes/_default/collections",
		url.Values{"name": {"c2"}, "maxTTL": {"0"}})

	maxTTLs := func(body []byte) map[string]interface{} {
		t.Helper()
		var parsed struct {
			Scopes []struct {
				Collections []map[string]interface{} `json:"collections"`
			} `json:"scopes"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			t.Fatalf("manifest %s is not JSON: %v", body, err)
		}
		ttls := make(map[string]interface{})
		for _, scope := range parsed.Scopes {
			for _, collection := range scope.Collections {
				ttls[collection["name"].(string)] = collection["maxTTL"]
			}
		}
		return ttls
	}

	body := testMgmtRequest(t, baseURL, http.MethodGet, "/buckets/default/scopes", nil)
	expected := map[string]interface{}{"_default": float64(0), "c0": float64(0), "c1": float64(30), "c2": float64(0)}
	if ttls := maxTTLs(body); !reflect.DeepEqual(ttls, expected) {
		t.Fatalf("REST: expected maxTTLs %v, got %v in %s", expected, ttls, body)
	}

	c := connectTestKv(t, cluster, memd.FeatureCollections)
	resp := c.mustSucceed(&memd.Packet{Command: memd.CmdCollectionsGetManifest})
	expected = map[string]interface{}{"_default": nil, "c0": nil, "c1": float64(30), "c2": nil}
	if ttls := maxTTLs(resp.Value); !reflect.DeepEqual(ttls, expected) {
		t.Fatalf("GET_COLLECTIONS_MANIFEST: expected maxTTLs %v, got %v in %s", expected, ttls, resp.Value)
	}
}

// TestCollectionsManifestConcurrentReads reads the manifest over REST and KV
// while collections are added.
func TestCollectionsManifestConcurrentReads(t *testing.T) {
	cluster, baseURL := newTestMgmtCluster(t)
	manifest := cluster.GetBucket("default").CollectionManifest()
	c := connectTestKv(t, cluster, memd.FeatureCollections)

	stop := make(chan struct{})
	done := make(chan error)
	t.Cleanup(func() {
		close(stop)
		if err := <-done; err != nil {
			t.Errorf("failed to add collection: %v", err)
		}
	})
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			if _, err := manifest.AddCollection("_default", fmt.Sprintf("c%d", i), 0); err != nil {
				done <- err
				return
			}
		}
	}()

	for i := 0; i < 100; i++ {
		c.mustSucceed(&memd.Packet{Command: memd.CmdCollectionsGetManifest})
		testMgmtRequest(t, baseURL, http.MethodGet, "/buckets/default/scopes", nil)
	}
}

// TestCollectionsManifestMatchesServer replays operations on Couchbase Server
// 7.0.5 and compares each answer with the server's, byte for byte. ns_server
// lists scopes and collections newest first. kv_engine sorts the keys of each
// object and lists the scopes in the order its std::unordered_map iterates
// them.
func TestCollectionsManifestMatchesServer(t *testing.T) {
	cluster, baseURL := newTestMgmtCluster(t)

	const restManifest = `{"uid":"25","scopes":[{"name":"x19","uid":"1e","collections":[]},{"name":"x18","uid":"1d","collections":[]},{"name":"x17","uid":"1c","collections":[]},{"name":"x16","uid":"1b","collections":[]},{"name":"x15","uid":"1a","collections":[]},{"name":"x14","uid":"19","collections":[]},{"name":"x13","uid":"18","collections":[]},{"name":"x12","uid":"17","collections":[]},{"name":"x11","uid":"16","collections":[]},{"name":"x10","uid":"15","collections":[]},{"name":"x9","uid":"14","collections":[]},{"name":"x8","uid":"13","collections":[]},{"name":"x7","uid":"12","collections":[]},{"name":"x6","uid":"11","collections":[]},{"name":"x5","uid":"10","collections":[]},{"name":"x4","uid":"f","collections":[]},{"name":"x3","uid":"e","collections":[]},{"name":"x2","uid":"d","collections":[]},{"name":"x1","uid":"c","collections":[]},{"name":"x0","uid":"b","collections":[]},{"name":"u","uid":"a","collections":[]},{"name":"t","uid":"9","collections":[]},{"name":"s","uid":"8","collections":[{"name":"c10","uid":"12","maxTTL":0},{"name":"c9","uid":"11","maxTTL":0},{"name":"c8","uid":"10","maxTTL":0},{"name":"c7","uid":"f","maxTTL":0},{"name":"c6","uid":"e","maxTTL":0},{"name":"c5","uid":"d","maxTTL":0},{"name":"c4","uid":"c","maxTTL":0},{"name":"c3","uid":"b","maxTTL":0},{"name":"c2","uid":"a","maxTTL":0},{"name":"c1","uid":"9","maxTTL":30},{"name":"c0","uid":"8","maxTTL":0}]},{"name":"_default","uid":"0","collections":[{"name":"d0","uid":"14","maxTTL":0},{"name":"_default","uid":"0","maxTTL":0}]}]}`
	const kvManifest = `{"scopes":[{"collections":[{"name":"c10","uid":"12"},{"name":"c9","uid":"11"},{"name":"c8","uid":"10"},{"name":"c7","uid":"f"},{"name":"c6","uid":"e"},{"name":"c5","uid":"d"},{"name":"c4","uid":"c"},{"name":"c3","uid":"b"},{"name":"c2","uid":"a"},{"maxTTL":30,"name":"c1","uid":"9"},{"name":"c0","uid":"8"}],"name":"s","uid":"8"},{"collections":[],"name":"t","uid":"9"},{"collections":[],"name":"u","uid":"a"},{"collections":[],"name":"x0","uid":"b"},{"collections":[],"name":"x1","uid":"c"},{"collections":[],"name":"x2","uid":"d"},{"collections":[],"name":"x3","uid":"e"},{"collections":[],"name":"x4","uid":"f"},{"collections":[],"name":"x5","uid":"10"},{"collections":[],"name":"x6","uid":"11"},{"collections":[],"name":"x19","uid":"1e"},{"collections":[{"name":"d0","uid":"14"},{"name":"_default","uid":"0"}],"name":"_default","uid":"0"},{"collections":[],"name":"x18","uid":"1d"},{"collections":[],"name":"x17","uid":"1c"},{"collections":[],"name":"x16","uid":"1b"},{"collections":[],"name":"x15","uid":"1a"},{"collections":[],"name":"x14","uid":"19"},{"collections":[],"name":"x13","uid":"18"},{"collections":[],"name":"x12","uid":"17"},{"collections":[],"name":"x11","uid":"16"},{"collections":[],"name":"x10","uid":"15"},{"collections":[],"name":"x9","uid":"14"},{"collections":[],"name":"x8","uid":"13"},{"collections":[],"name":"x7","uid":"12"}],"uid":"25"}`

	uid := 0
	change := func(method, path string, form url.Values) {
		t.Helper()
		uid++
		req, err := http.NewRequest(method, baseURL+path, strings.NewReader(form.Encode()))
		if err != nil {
			t.Fatalf("failed to build request: %v", err)
		}
		req.SetBasicAuth("Administrator", "password")
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s failed: %v", method, path, err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("failed to read body: %v", err)
		}
		if expected := fmt.Sprintf(`{"uid":"%x"}`, uid); resp.StatusCode != http.StatusOK || string(body) != expected {
			t.Fatalf("%s %s: expected 200 with %s, got %d with %s", method, path, expected, resp.StatusCode, body)
		}
		if contentType := resp.Header.Get("Content-Type"); contentType != "application/json" {
			t.Fatalf("%s %s: expected Content-Type application/json, got %q", method, path, contentType)
		}
	}

	change(http.MethodPost, "/buckets/default/scopes", url.Values{"name": {"s"}})
	for i := 0; i < 12; i++ {
		form := url.Values{"name": {fmt.Sprintf("c%d", i)}}
		if i == 1 {
			form.Set("maxTTL", "30")
		}
		change(http.MethodPost, "/buckets/default/scopes/s/collections", form)
	}
	change(http.MethodPost, "/buckets/default/scopes", url.Values{"name": {"t"}})
	change(http.MethodPost, "/buckets/default/scopes", url.Values{"name": {"u"}})
	change(http.MethodPost, "/buckets/default/scopes/_default/collections", url.Values{"name": {"d0"}})
	change(http.MethodDelete, "/buckets/default/scopes/s/collections/c11", nil)
	for i := 0; i < 20; i++ {
		change(http.MethodPost, "/buckets/default/scopes", url.Values{"name": {fmt.Sprintf("x%d", i)}})
	}

	req, err := http.NewRequest(http.MethodGet, baseURL+"/buckets/default/scopes", nil)
	if err != nil {
		t.Fatalf("failed to build request: %v", err)
	}
	req.SetBasicAuth("Administrator", "password")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET scopes failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if string(body) != restManifest {
		t.Fatalf("expected manifest %s, got %s", restManifest, body)
	}
	if contentType := resp.Header.Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("expected Content-Type application/json, got %q", contentType)
	}

	c := connectTestKv(t, cluster, memd.FeatureCollections)
	if resp := c.mustSucceed(&memd.Packet{Command: memd.CmdCollectionsGetManifest}); string(resp.Value) != kvManifest {
		t.Fatalf("expected manifest %s, got %s", kvManifest, resp.Value)
	}
}
