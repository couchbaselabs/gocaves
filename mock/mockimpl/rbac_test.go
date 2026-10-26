package mockimpl

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
)

// connectTestKvAs opens a connection to the first node of cluster that
// negotiates collections and XERROR and authenticates as username, with the
// password "password", unless username is empty. It selects no bucket.
func connectTestKvAs(t *testing.T, cluster mock.Cluster, username string) *testKvConn {
	return dialTestKv(t, cluster, username, memd.FeatureCollections, memd.FeatureXerror)
}

// dialTestKv opens a connection as connectTestKvAs does, negotiating
// features, and JSON, as SDKs do.
func dialTestKv(t *testing.T, cluster mock.Cluster, username string, features ...memd.HelloFeature) *testKvConn {
	features = append([]memd.HelloFeature{memd.FeatureJSON}, features...)
	netConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", cluster.Nodes()[0].KvService().ListenPort()))
	if err != nil {
		t.Fatalf("failed to dial kv service: %v", err)
	}
	t.Cleanup(func() { _ = netConn.Close() })
	c := &testKvConn{t: t, cluster: cluster, netConn: netConn, conn: memd.NewConn(netConn)}

	hello := make([]byte, 2*len(features))
	for i, feature := range features {
		binary.BigEndian.PutUint16(hello[2*i:], uint16(feature))
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdHello, Value: hello})
	for _, feature := range features {
		c.conn.EnableFeature(feature)
	}
	if username != "" {
		c.mustSucceed(&memd.Packet{
			Command: memd.CmdSASLAuth,
			Key:     []byte("PLAIN"),
			Value:   []byte("\x00" + username + "\x00password"),
		})
	}
	return c
}

func addTestUser(t *testing.T, cluster mock.Cluster, username string, roles ...string) {
	t.Helper()
	err := cluster.Users().UpsertUser(mockauth.UpsertUserOptions{
		Username: username,
		Password: "password",
		Roles:    roles,
	})
	if err != nil {
		t.Fatalf("failed to add user: %v", err)
	}
}

// TestCollectionsKvVisibility checks SELECT_BUCKET, GET_COLLECTIONS_MANIFEST
// and GET_COLLECTION_ID for users with ns_server roles against Couchbase
// Server 7.0.5. kv_engine lets a user select a bucket it has any privilege
// in, from the privileges ns_server's memcached_permissions derives from its
// roles, and shows a scope or collection unless the user has no privilege
// there and none on the collections of the bucket.
func TestCollectionsKvVisibility(t *testing.T) {
	cluster, baseURL := newTestMgmtCluster(t)
	testMgmtRequest(t, baseURL, http.MethodPost, "/buckets/default/scopes", url.Values{"name": {"s"}})
	testMgmtRequest(t, baseURL, http.MethodPost, "/buckets/default/scopes/s/collections", url.Values{"name": {"c0"}})
	testMgmtRequest(t, baseURL, http.MethodPost, "/buckets/default/scopes/s/collections", url.Values{"name": {"c1"}})
	testMgmtRequest(t, baseURL, http.MethodPost, "/buckets/default/scopes", url.Values{"name": {"t"}})
	testMgmtRequest(t, baseURL, http.MethodPost, "/buckets/default/scopes/t/collections", url.Values{"name": {"d"}})

	const (
		all = `{"scopes":[{"collections":[{"name":"_default","uid":"0"}],"name":"_default","uid":"0"},{"collections":[{"name":"c1","uid":"9"},{"name":"c0","uid":"8"}],"name":"s","uid":"8"},{"collections":[{"name":"d","uid":"a"}],"name":"t","uid":"9"}],"uid":"5"}`
		sc0 = `{"scopes":[{"collections":[{"name":"c0","uid":"8"}],"name":"s","uid":"8"}],"uid":"5"}`
		s   = `{"scopes":[{"collections":[{"name":"c1","uid":"9"},{"name":"c0","uid":"8"}],"name":"s","uid":"8"}],"uid":"5"}`
		sc1 = `{"scopes":[{"collections":[{"name":"c1","uid":"9"}],"name":"s","uid":"8"}],"uid":"5"}`
		td  = `{"scopes":[{"collections":[{"name":"d","uid":"a"}],"name":"t","uid":"9"}],"uid":"5"}`
	)
	paths := []string{"s.c0", "s.c1", "t.d", "_default._default"}
	tests := []struct {
		roles    []string
		manifest string
		// visible are the paths GET_COLLECTION_ID finds.
		visible []string
		// memd tells whether the user may select the memcached bucket.
		memd bool
	}{
		{roles: []string{"data_reader[default:s:c0]"}, manifest: sc0, visible: []string{"s.c0"}},
		{roles: []string{"bucket_admin[default]"}, manifest: all, visible: paths},
		{roles: []string{"ro_admin"}, manifest: all, visible: paths, memd: true},
		{roles: []string{"cluster_admin"}, manifest: all, visible: paths, memd: true},
		{roles: []string{"data_writer[default:s]"}, manifest: s, visible: []string{"s.c0", "s.c1"}},
		{roles: []string{"views_reader[default]"}, manifest: all, visible: paths},
		{roles: []string{"query_select[default:t:d]"}, manifest: td, visible: []string{"t.d"}},
		{roles: []string{"data_monitoring[default:s]"}, manifest: s, visible: []string{"s.c0", "s.c1"}},
		{roles: []string{"data_monitoring[default:s:c1]"}, manifest: sc1, visible: []string{"s.c1"}},
		{roles: []string{"fts_searcher[default:s:c1]"}},
		{roles: []string{"analytics_reader"}},
		{roles: []string{"data_reader[default:s:c0]", "data_writer[default:t]"},
			manifest: `{"scopes":[{"collections":[{"name":"c0","uid":"8"}],"name":"s","uid":"8"},{"collections":[{"name":"d","uid":"a"}],"name":"t","uid":"9"}],"uid":"5"}`,
			visible:  []string{"s.c0", "t.d"}},
	}
	for i, tt := range tests {
		t.Run(strings.Join(tt.roles, ","), func(t *testing.T) {
			username := fmt.Sprintf("user%d", i)
			addTestUser(t, cluster, username, tt.roles...)
			c := connectTestKvAs(t, cluster, username)

			resp := c.Do(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("memd")})
			if (resp.Status == memd.StatusSuccess) != tt.memd {
				t.Fatalf("SELECT_BUCKET memd: got status 0x%02x", uint16(resp.Status))
			}
			resp = c.Do(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})
			if tt.manifest == "" {
				if resp.Status != memd.StatusAccessError {
					t.Fatalf("SELECT_BUCKET default: expected status 0x%02x, got 0x%02x",
						uint16(memd.StatusAccessError), uint16(resp.Status))
				}
				return
			}
			if resp.Status != memd.StatusSuccess {
				t.Fatalf("SELECT_BUCKET default failed with status 0x%02x", uint16(resp.Status))
			}

			resp = c.mustSucceed(&memd.Packet{Command: memd.CmdCollectionsGetManifest})
			if string(resp.Value) != tt.manifest {
				t.Fatalf("expected manifest %s, got %s", tt.manifest, resp.Value)
			}
			visible := make(map[string]bool)
			for _, path := range tt.visible {
				visible[path] = true
			}
			const unknown = `{"error":{"context":"Unknown scope or collection in operation"},"manifest_uid":"5"}`
			for _, path := range paths {
				resp := c.Do(&memd.Packet{Command: memd.CmdCollectionsGetID, Value: []byte(path)})
				switch {
				case visible[path] && resp.Status != memd.StatusSuccess:
					t.Fatalf("%s: expected success, got status 0x%02x", path, uint16(resp.Status))
				case !visible[path] && (resp.Status != memd.StatusCollectionUnknown || string(resp.Value) != unknown):
					t.Fatalf("%s: expected status 0x%02x with %s, got 0x%02x with %s",
						path, uint16(memd.StatusCollectionUnknown), unknown, uint16(resp.Status), resp.Value)
				}
			}
		})
	}

	// A role on a collection ends with it: the user has no privilege left,
	// even once a collection of the same name is created.
	t.Run("role on a dropped collection", func(t *testing.T) {
		addTestUser(t, cluster, "dropped", "data_reader[default:t:d]")
		testMgmtRequest(t, baseURL, http.MethodDelete, "/buckets/default/scopes/t/collections/d", nil)
		testMgmtRequest(t, baseURL, http.MethodPost, "/buckets/default/scopes/t/collections", url.Values{"name": {"d"}})
		c := connectTestKvAs(t, cluster, "dropped")
		if resp := c.Do(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")}); resp.Status != memd.StatusAccessError {
			t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusAccessError), uint16(resp.Status))
		}
	})
}

// TestCollectionsKvManifestFilteredInMapOrder checks that GET_COLLECTIONS_MANIFEST
// lists the scopes a user can see in the order kv_engine's scope map holds all
// of them, as Manifest::toJson filters while it walks the map. On Couchbase
// Server 7.0.5 a user seeing scopes 0x13, 0x1e and _default of scopes 8 to
// 0x1e gets them as 0x1e, 0 and 0x13.
func TestCollectionsKvManifestFilteredInMapOrder(t *testing.T) {
	cluster, _ := newTestMgmtCluster(t)
	manifest := cluster.GetBucket("default").CollectionManifest()
	for id := 8; id <= 0x1e; id++ {
		if _, err := manifest.AddScope(fmt.Sprintf("y%d", id)); err != nil {
			t.Fatalf("failed to add scope: %v", err)
		}
	}
	addTestUser(t, cluster, "some", "data_reader[default:y19]", "data_reader[default:y30]", "data_reader[default:_default]")
	c := connectTestKvAs(t, cluster, "some")
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})

	resp := c.mustSucceed(&memd.Packet{Command: memd.CmdCollectionsGetManifest})
	const expected = `{"scopes":[{"collections":[],"name":"y30","uid":"1e"},{"collections":[{"name":"_default","uid":"0"}],"name":"_default","uid":"0"},{"collections":[],"name":"y19","uid":"13"}],"uid":"17"}`
	if string(resp.Value) != expected {
		t.Fatalf("expected manifest %s, got %s", expected, resp.Value)
	}
}

// TestCollectionsRESTManifestPermissions checks GET .../scopes for users with
// different roles against Couchbase Server 7.0.5: menelaus_web requires
// collections read on some scope or collection of the bucket, and
// collections:manifest_json/3 lists the scopes and collections the user can
// read. A memcached bucket answers ns_server's default manifest.
func TestCollectionsRESTManifestPermissions(t *testing.T) {
	cluster, baseURL := newTestMgmtCluster(t)

	admin := func(method, path string, form url.Values) {
		t.Helper()
		testMgmtRequest(t, baseURL, method, path, form)
	}
	admin(http.MethodPost, "/buckets/default/scopes", url.Values{"name": {"s"}})
	admin(http.MethodPost, "/buckets/default/scopes/s/collections", url.Values{"name": {"c0"}})
	admin(http.MethodPost, "/buckets/default/scopes/s/collections", url.Values{"name": {"c1"}, "maxTTL": {"0"}})
	admin(http.MethodPost, "/buckets/default/scopes/s/collections", url.Values{"name": {"c2"}, "maxTTL": {"30"}})
	admin(http.MethodPost, "/buckets/default/scopes", url.Values{"name": {"t"}})
	admin(http.MethodPost, "/buckets/default/scopes/t/collections", url.Values{"name": {"d"}})

	const (
		all       = `{"uid":"6","scopes":[{"name":"t","uid":"9","collections":[{"name":"d","uid":"b","maxTTL":0}]},{"name":"s","uid":"8","collections":[{"name":"c2","uid":"a","maxTTL":30},{"name":"c1","uid":"9","maxTTL":0},{"name":"c0","uid":"8","maxTTL":0}]},{"name":"_default","uid":"0","collections":[{"name":"_default","uid":"0","maxTTL":0}]}]}`
		forbidden = `{"message":"Forbidden. User needs the following permissions","permissions":["cluster.collection[%s:.:.].collections!read"]}`
	)
	tests := []struct {
		role   string
		bucket string
		status int
		body   string
	}{
		{role: "admin", bucket: "memd", status: 200,
			body: `{"uid":"0","scopes":[{"name":"_default","uid":"0","collections":[{"name":"_default","uid":"0","maxTTL":0}]}]}`},
		{role: "ro_admin", bucket: "default", status: 200, body: all},
		{role: "bucket_admin[default]", bucket: "default", status: 200, body: all},
		{role: "fts_admin[default]", bucket: "default", status: 200, body: all},
		{role: "data_monitoring[default]", bucket: "default", status: 200, body: all},
		{role: "data_monitoring[*]", bucket: "default", status: 200, body: all},
		{role: "scope_admin[default:s]", bucket: "default", status: 200,
			body: `{"uid":"6","scopes":[{"name":"s","uid":"8","collections":[{"name":"c2","uid":"a","maxTTL":30},{"name":"c1","uid":"9","maxTTL":0},{"name":"c0","uid":"8","maxTTL":0}]}]}`},
		{role: "query_manage_functions[default:t]", bucket: "default", status: 200,
			body: `{"uid":"6","scopes":[{"name":"t","uid":"9","collections":[{"name":"d","uid":"b","maxTTL":0}]}]}`},
		{role: "data_monitoring[default:s:c0]", bucket: "default", status: 200,
			body: `{"uid":"6","scopes":[{"name":"s","uid":"8","collections":[{"name":"c0","uid":"8","maxTTL":0}]}]}`},
		{role: "data_monitoring[default:s:c0]", bucket: "memd", status: 403, body: fmt.Sprintf(forbidden, "memd")},
		{role: "data_reader[default]", bucket: "default", status: 403, body: fmt.Sprintf(forbidden, "default")},
		{role: "views_admin[default]", bucket: "default", status: 403, body: fmt.Sprintf(forbidden, "default")},
	}
	for i, tt := range tests {
		t.Run(tt.role+" on "+tt.bucket, func(t *testing.T) {
			username := fmt.Sprintf("user%d", i)
			err := cluster.Users().UpsertUser(mockauth.UpsertUserOptions{
				Username: username,
				Password: "password",
				Roles:    []string{tt.role},
			})
			if err != nil {
				t.Fatalf("failed to add user: %v", err)
			}
			req, err := http.NewRequest(http.MethodGet, baseURL+"/buckets/"+tt.bucket+"/scopes", nil)
			if err != nil {
				t.Fatalf("failed to build request: %v", err)
			}
			req.SetBasicAuth(username, "password")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read body: %v", err)
			}
			if resp.StatusCode != tt.status || string(body) != tt.body {
				t.Fatalf("expected %d with %s, got %d with %s", tt.status, tt.body, resp.StatusCode, body)
			}
			if contentType := resp.Header.Get("Content-Type"); contentType != "application/json" {
				t.Fatalf("expected Content-Type application/json, got %q", contentType)
			}
		})
	}

}

// newRBACTestCluster creates a default cluster whose bucket "default" has
// scope s with collections c0 (id 8) and c1 (id 9), and scope t with
// collection d (id 0xa), at manifest uid 5.
func newRBACTestCluster(t *testing.T) (mock.Cluster, string) {
	cluster, baseURL := newTestMgmtCluster(t)
	testMgmtRequest(t, baseURL, http.MethodPost, "/buckets/default/scopes", url.Values{"name": {"s"}})
	testMgmtRequest(t, baseURL, http.MethodPost, "/buckets/default/scopes/s/collections", url.Values{"name": {"c0"}})
	testMgmtRequest(t, baseURL, http.MethodPost, "/buckets/default/scopes/s/collections", url.Values{"name": {"c1"}})
	testMgmtRequest(t, baseURL, http.MethodPost, "/buckets/default/scopes", url.Values{"name": {"t"}})
	testMgmtRequest(t, baseURL, http.MethodPost, "/buckets/default/scopes/t/collections", url.Values{"name": {"d"}})
	return cluster, baseURL
}

// accessErrorRef matches the version 4 UUID of an error reference.
var accessErrorRef = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// expectAccessError checks that resp is kv_engine's no_access for a missing
// privilege, with its error context and a reference.
func expectAccessError(t *testing.T, resp *memd.Packet, context string) {
	t.Helper()
	if resp.Status != memd.StatusAccessError {
		t.Fatalf("expected status 0x%02x, got 0x%02x with %s", uint16(memd.StatusAccessError), uint16(resp.Status), resp.Value)
	}
	var body struct {
		Error struct {
			Context string `json:"context"`
			Ref     string `json:"ref"`
		} `json:"error"`
	}
	if err := json.Unmarshal(resp.Value, &body); err != nil {
		t.Fatalf("error body %q is not JSON: %v", resp.Value, err)
	}
	if body.Error.Context != context || !accessErrorRef.MatchString(body.Error.Ref) {
		t.Fatalf("expected context %q and a reference, got %s", context, resp.Value)
	}
}

// TestKvPrivileges checks commands of users with different roles against
// Couchbase Server 7.0.5. kv_engine checks the privileges a command needs
// against the bucket, or, for a user with privileges on scopes or
// collections, against the scope of the request's collection, which it looks
// up first. A missing privilege is no_access, unless the user has no
// privilege there, which is unknown_collection.
func TestKvPrivileges(t *testing.T) {
	cluster, _ := newRBACTestCluster(t)
	addTestUser(t, cluster, "reader", "data_reader[default:s:c0]")
	addTestUser(t, cluster, "writer", "data_writer[default:s]")
	addTestUser(t, cluster, "badmin", "bucket_admin[default]")

	get := func(cid uint32) *memd.Packet {
		return &memd.Packet{Command: memd.CmdGet, CollectionID: cid, Key: []byte("k")}
	}
	set := func(command memd.CmdCode, cid uint32) *memd.Packet {
		return &memd.Packet{Command: command, CollectionID: cid, Key: []byte("k"), Value: []byte("1"), Extras: make([]byte, 8)}
	}
	unknown := func(uid string) string {
		return `{"error":{"context":"Unknown scope or collection in operation"},"manifest_uid":"` + uid + `"}`
	}
	type step struct {
		req    *memd.Packet
		status memd.StatusCode
		// body is the expected error body of unknown_collection, context the
		// error context of no_access.
		body, context string
	}
	tests := []struct {
		user  string
		steps []step
	}{
		{user: "reader", steps: []step{
			{req: get(8), status: memd.StatusKeyNotFound},
			{req: set(memd.CmdSet, 8), status: memd.StatusAccessError,
				context: "Authorization failure: can't execute SET operation without the Upsert privilege"},
			{req: get(9), status: memd.StatusCollectionUnknown, body: unknown("5")},
			{req: get(0), status: memd.StatusCollectionUnknown, body: unknown("0")},
			{req: get(0x63), status: memd.StatusCollectionUnknown, body: unknown("0")},
			{req: &memd.Packet{Command: memd.CmdStat}, status: memd.StatusAccessError,
				context: "Authorization failure: can't execute STAT operation without the SimpleStats privilege"},
		}},
		{user: "writer", steps: []step{
			{req: get(8), status: memd.StatusAccessError,
				context: "Authorization failure: can't execute GET operation without the Read privilege"},
			{req: set(memd.CmdAdd, 8), status: memd.StatusSuccess},
			{req: set(memd.CmdSet, 8), status: memd.StatusSuccess},
			{req: &memd.Packet{Command: memd.CmdIncrement, CollectionID: 8, Key: []byte("n"), Extras: make([]byte, 20)},
				status:  memd.StatusAccessError,
				context: "Authorization failure: can't execute INCREMENT operation without the Read privilege"},
			{req: get(0xa), status: memd.StatusCollectionUnknown, body: unknown("5")},
		}},
		{user: "badmin", steps: []step{
			{req: get(0), status: memd.StatusAccessError,
				context: "Authorization failure: can't execute GET operation without the Read privilege"},
			{req: set(memd.CmdSet, 0), status: memd.StatusAccessError,
				context: "Authorization failure: can't execute SET operation without the Upsert privilege"},
			{req: get(0x63), status: memd.StatusAccessError,
				context: "Authorization failure: can't execute GET operation without the Read privilege"},
		}},
		{user: "Administrator", steps: []step{
			{req: get(0x63), status: memd.StatusCollectionUnknown, body: unknown("5")},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.user, func(t *testing.T) {
			c := connectTestKvAs(t, cluster, tt.user)
			c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})
			for _, step := range tt.steps {
				step.req.Vbucket = c.masterVbucket(cluster.Nodes()[0])
				resp := c.Do(step.req)
				switch {
				case step.context != "":
					expectAccessError(t, resp, step.context)
				case resp.Status != step.status || string(resp.Value) != step.body:
					t.Fatalf("%s: expected status 0x%02x with %q, got 0x%02x with %q",
						step.req.Command.Name(), uint16(step.status), step.body, uint16(resp.Status), resp.Value)
				}
			}
		})
	}
}

// TestKvPrivilegesWithoutBucket checks commands before SELECT_BUCKET against
// Couchbase Server 7.0.5: a connection that has not authenticated has no
// privilege, an authenticated one gets no_bucket, and GET_CLUSTER_CONFIG
// needs the global SystemSettings privilege, which a scope_admin lacks.
func TestKvPrivilegesWithoutBucket(t *testing.T) {
	cluster, _ := newRBACTestCluster(t)
	addTestUser(t, cluster, "reader", "data_reader[default:s:c0]")
	addTestUser(t, cluster, "sadmin", "scope_admin[default:s]")

	c := connectTestKvAs(t, cluster, "")
	expectAccessError(t, c.Do(&memd.Packet{Command: memd.CmdGetClusterConfig}),
		"Authorization failure: can't execute GET_CLUSTER_CONFIG operation without the SystemSettings privilege")
	expectAccessError(t, c.Do(&memd.Packet{Command: memd.CmdGet, Key: []byte("k")}),
		"Authorization failure: can't execute GET operation without the Read privilege")

	c = connectTestKvAs(t, cluster, "reader")
	if resp := c.Do(&memd.Packet{Command: memd.CmdGet, Key: []byte("k")}); resp.Status != memd.StatusNoBucket {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusNoBucket), uint16(resp.Status))
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdGetClusterConfig})

	c = connectTestKvAs(t, cluster, "sadmin")
	expectAccessError(t, c.Do(&memd.Packet{Command: memd.CmdGetClusterConfig}),
		"Authorization failure: can't execute GET_CLUSTER_CONFIG operation without the SystemSettings privilege")
	if resp := c.Do(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")}); resp.Status != memd.StatusAccessError {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusAccessError), uint16(resp.Status))
	}
}

// kv_engine's select_bucket refuses a user dropped since it authenticated
// with no_access and no error context, as createContext finds no such user.
func TestSelectBucketByDroppedUser(t *testing.T) {
	cluster, _ := newRBACTestCluster(t)
	addTestUser(t, cluster, "reader", "data_reader[default:s:c0]")
	c := connectTestKvAs(t, cluster, "reader")
	if err := cluster.Users().DropUser("reader"); err != nil {
		t.Fatalf("failed to drop user: %v", err)
	}
	resp := c.Do(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})
	if resp.Status != memd.StatusAccessError || len(resp.Value) != 0 {
		t.Fatalf("expected status 0x%02x with no body, got 0x%02x with %q",
			uint16(memd.StatusAccessError), uint16(resp.Status), resp.Value)
	}
}

func testHTTPRequest(t *testing.T, method, url, username string, form url.Values) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("failed to build request: %v", err)
	}
	if username != "" {
		req.SetBasicAuth(username, "password")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s failed: %v", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	return resp, body
}

// TestMgmtPermissions checks management endpoints for users with different
// roles against Couchbase Server 7.0.5: menelaus_web asks for credentials
// without them, and answers forbidden_response to a user lacking the
// permission of the endpoint.
func TestMgmtPermissions(t *testing.T) {
	cluster, _ := newRBACTestCluster(t)
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", cluster.Nodes()[0].MgmtService().ListenPort())
	addTestUser(t, cluster, "sadmin", "scope_admin[default:s]")
	addTestUser(t, cluster, "roadmin", "ro_admin")
	addTestUser(t, cluster, "reader", "data_reader[default:s:c0]")

	const forbidden = `{"message":"Forbidden. User needs the following permissions","permissions":["%s"]}`
	tests := []struct {
		name, method, path, user string
		form                     url.Values
		status                   int
		body                     string
	}{
		{name: "pools without credentials", method: http.MethodGet, path: "/pools", status: 401},
		{name: "scope_admin creates a collection in its scope", method: http.MethodPost, path: "/pools/default/buckets/default/scopes/s/collections",
			user: "sadmin", form: url.Values{"name": {"x"}}, status: 200, body: `{"uid":"6"}`},
		{name: "scope_admin creates a scope", method: http.MethodPost, path: "/pools/default/buckets/default/scopes",
			user: "sadmin", form: url.Values{"name": {"x"}}, status: 403,
			body: fmt.Sprintf(forbidden, "cluster.bucket[default].collections!write")},
		{name: "scope_admin drops another scope", method: http.MethodDelete, path: "/pools/default/buckets/default/scopes/t",
			user: "sadmin", status: 403, body: fmt.Sprintf(forbidden, "cluster.collection[default:t:*].collections!write")},
		{name: "ro_admin upserts a user", method: http.MethodPut, path: "/settings/rbac/users/local/u",
			user: "roadmin", form: url.Values{"password": {"password"}, "roles": {"admin"}}, status: 403,
			body: fmt.Sprintf(forbidden, "cluster.admin.security!write")},
		{name: "ro_admin lists users", method: http.MethodGet, path: "/settings/rbac/users/local", user: "roadmin", status: 200},
		{name: "data_reader lists buckets", method: http.MethodGet, path: "/pools/default/buckets", user: "reader", status: 200},
		{name: "data_reader reads the bucket", method: http.MethodGet, path: "/pools/default/buckets/default", user: "reader", status: 200},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, body := testHTTPRequest(t, tt.method, baseURL+tt.path, tt.user, tt.form)
			if resp.StatusCode != tt.status {
				t.Fatalf("expected status %d, got %d with %s", tt.status, resp.StatusCode, body)
			}
			switch tt.status {
			case 401:
				if auth := resp.Header.Get("WWW-Authenticate"); auth != `Basic realm="Couchbase Server Admin / REST"` || len(body) != 0 {
					t.Fatalf("expected a Basic challenge and no body, got %q with %s", auth, body)
				}
			case 403:
				if string(body) != tt.body || resp.Header.Get("Content-Type") != "application/json" {
					t.Fatalf("expected application/json %s, got %s %s", tt.body, resp.Header.Get("Content-Type"), body)
				}
			default:
				if tt.body != "" && string(body) != tt.body {
					t.Fatalf("expected %s, got %s", tt.body, body)
				}
			}
		})
	}
}

// TestViewPermissions checks bucket database requests against Couchbase
// Server 7.0.5, which capi_frontend:verify_bucket_auth/3 answers, with the
// newline couch_httpd ends a JSON body with. couch_httpd splits the path as
// sent, without empty tokens, and decodes each, so an encoded "/" names a
// document or a vbucket database.
func TestViewPermissions(t *testing.T) {
	cluster, _ := newRBACTestCluster(t)
	addTestUser(t, cluster, "reader", "data_reader[default:s:c0]")
	addTestUser(t, cluster, "vr", "views_reader[default]")
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", cluster.Nodes()[0].ViewService().ListenPort())

	tests := []struct {
		name, path, user, auth string
		status                 int
		body                   string
	}{
		{name: "missing bucket", path: "/missing/_design/x", user: "Administrator", status: 404,
			body: "{\"error\":\"not_found\",\"reason\":\"missing\"}\n"},
		{name: "without credentials", path: "/default/_design/x", status: 401, auth: `Basic realm="capi"`,
			body: "{\"error\":\"unauthorized\",\"reason\":\"password required\"}\n"},
		{name: "data_reader", path: "/default/_design/x", user: "reader", status: 403,
			body: `{"message":"Forbidden. User needs the following permissions","permissions":["cluster.bucket[default].views!read"]}` + "\n"},
		{name: "empty tokens", path: "//default//_design/x/", user: "reader", status: 403,
			body: `{"message":"Forbidden. User needs the following permissions","permissions":["cluster.bucket[default].views!read"]}` + "\n"},
		{name: "encoded slash in the document", path: "/default/_design%2Fx", user: "vr", status: 403,
			body: `{"message":"Forbidden. User needs the following permissions","permissions":["cluster.bucket[default].data.couchdb!read"]}` + "\n"},
		{name: "vbucket database", path: "/default%2F0/_design/x", user: "reader", status: 403,
			body: `{"message":"Forbidden. User needs the following permissions","permissions":["cluster.bucket[default].views!read"]}` + "\n"},
		{name: "bucket database", path: "/default", user: "vr", status: 403,
			body: `{"message":"Forbidden. User needs the following permissions","permissions":["cluster.bucket[default].data.couchdb!read"]}` + "\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, body := testHTTPRequest(t, http.MethodGet, baseURL+tt.path, tt.user, nil)
			if resp.StatusCode != tt.status || string(body) != tt.body ||
				resp.Header.Get("Content-Type") != "text/plain;charset=utf-8" || resp.Header.Get("WWW-Authenticate") != tt.auth {
				t.Fatalf("expected %d with %s, got %d with %s and headers %v", tt.status, tt.body, resp.StatusCode, body, resp.Header)
			}
		})
	}
}

// TestUpsertUserRoleValidation checks the roles PUT .../users refuses
// against Couchbase Server 7.0.5, and the order it lists them in.
func TestUpsertUserRoleValidation(t *testing.T) {
	cluster, _ := newRBACTestCluster(t)
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", cluster.Nodes()[0].MgmtService().ListenPort())
	const prefix = "Cannot assign roles to user because the following roles are unknown, malformed or role parameters are undefined: "
	tests := []struct {
		roles   string
		refused string
	}{
		{"data_reader[nope],bogus,data_reader[default:s:nope],admin[default]",
			"[bogus,admin[default],data_reader[default:s:nope],data_reader[nope]]"},
		{"data_reader[nope],data_reader[default:s:nope]", "[data_reader[nope],data_reader[default:s:nope]]"},
		{"data_reader[default:s:nope],data_reader[nope]", "[data_reader[nope],data_reader[default:s:nope]]"},
		{"admin[default],data_reader[nope]", "[data_reader[nope],admin[default]]"},
		{"bogus", "[bogus]"},
		{"data_reader[default:s:c0:x]", "[data_reader[default:s:c0:x]]"},
		{"data_reader[]", "[data_reader[]]"},
		{"bucket_admin", "[bucket_admin]"},
		{"data_reader[default:*]", "[data_reader[default:*]]"},
		{"data_reader[memd:_default]", "[data_reader[memd:_default]]"},
		{"data_reader[*]", ""},
		{"data_reader[any]", "[data_reader[any]]"},
		{"data_reader[default:any]", "[data_reader[default:any]]"},
		{"Admin,DATA_READER[default],Data_Reader[default:_default]", ""},
		{"DATA_READER[nope]", "[data_reader[nope]]"},
		{"ADMİN", "[ADMİN]"},
	}
	for _, tt := range tests {
		t.Run(tt.roles, func(t *testing.T) {
			resp, body := testHTTPRequest(t, http.MethodPut, baseURL+"/settings/rbac/users/local/u", "Administrator",
				url.Values{"password": {"password"}, "roles": {tt.roles}})
			if tt.refused == "" {
				if resp.StatusCode != 200 {
					t.Fatalf("expected status 200, got %d with %s", resp.StatusCode, body)
				}
				return
			}
			expected, _ := json.Marshal(map[string]map[string]string{"errors": {"roles": prefix + tt.refused}})
			if resp.StatusCode != 400 || string(body) != string(expected) {
				t.Fatalf("expected 400 with %s, got %d with %s", expected, resp.StatusCode, body)
			}
		})
	}
}

// TestRolesChangeWhileReading drops and creates collections that a user and
// a group have roles on, and changes their roles, while the user reads the
// manifest and the users and groups are listed.
func TestRolesChangeWhileReading(t *testing.T) {
	cluster, baseURL := newRBACTestCluster(t)
	addTestUser(t, cluster, "reader", "data_reader[default:s:c0]", "data_reader[default:s:c1]")
	c := connectTestKvAs(t, cluster, "reader")
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			name := fmt.Sprintf("x%d", i)
			testMgmtRequest(t, baseURL, http.MethodPost, "/buckets/default/scopes/s/collections", url.Values{"name": {name}})
			addTestUser(t, cluster, "reader", "data_reader[default:s:c0]", "data_reader[default:s:"+name+"]")
			if err := cluster.Users().UpsertGroup(mockauth.UpsertGroupOptions{Name: "g", Roles: []string{"data_reader[default:s:" + name + "]"}}); err != nil {
				t.Errorf("failed to upsert group: %v", err)
			}
			testMgmtRequest(t, baseURL, http.MethodDelete, "/buckets/default/scopes/s/collections/"+name, nil)
		}
	}()
	rootURL := fmt.Sprintf("http://127.0.0.1:%d", cluster.Nodes()[0].MgmtService().ListenPort())
	for running := true; running; {
		select {
		case <-done:
			running = false
		default:
		}
		c.mustSucceed(&memd.Packet{Command: memd.CmdCollectionsGetManifest})
		testHTTPRequest(t, http.MethodGet, rootURL+"/settings/rbac/users", "Administrator", nil)
		testHTTPRequest(t, http.MethodGet, rootURL+"/settings/rbac/groups", "Administrator", nil)
	}
}

// TestBucketListReadableBuckets checks GET /pools/default/buckets against
// Couchbase Server 7.0.5: menelaus_web_buckets:handle_bucket_list lists, by
// name, the buckets whose settings the user may read.
func TestBucketListReadableBuckets(t *testing.T) {
	cluster, _ := newRBACTestCluster(t)
	if _, err := cluster.AddBucket(mock.NewBucketOptions{Name: "alpha", Type: mock.BucketTypeCouchbase}); err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	addTestUser(t, cluster, "reader", "data_reader[alpha]", "data_reader[memd]")
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", cluster.Nodes()[0].MgmtService().ListenPort())

	for user, expected := range map[string][]string{
		"Administrator": {"alpha", "default", "memd"},
		"reader":        {"alpha", "memd"},
	} {
		resp, body := testHTTPRequest(t, http.MethodGet, baseURL+"/pools/default/buckets", user, nil)
		var buckets []struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(body, &buckets); err != nil {
			t.Fatalf("%s: %d %s is not JSON: %v", user, resp.StatusCode, body, err)
		}
		var names []string
		for _, bucket := range buckets {
			names = append(names, bucket.Name)
		}
		if strings.Join(names, ",") != strings.Join(expected, ",") {
			t.Fatalf("%s: expected buckets %v, got %v", user, expected, names)
		}
	}
}

var passwordChangeDate = regexp.MustCompile(`"password_change_date":"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.000Z"`)

// TestUsersAndGroupsREST replays user and group requests recorded on
// Couchbase Server 7.0.5 and compares the answers: roles kept sorted and
// unique, users with the roles of their groups and where each comes from,
// and ns_server's validation errors.
func TestUsersAndGroupsREST(t *testing.T) {
	cluster, _ := newRBACTestCluster(t)
	if _, err := cluster.AddBucket(mock.NewBucketOptions{Name: "zeta", Type: mock.BucketTypeCouchbase}); err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	baseURL := fmt.Sprintf("http://127.0.0.1:%d/settings/rbac", cluster.Nodes()[0].MgmtService().ListenPort())

	tests := []struct {
		method, path string
		form         url.Values
		status       int
		body         string
	}{
		{http.MethodPut, "/groups/g1", url.Values{"roles": {"data_reader[default],bucket_admin[zeta]"}, "description": {"first"}}, 200, `""`},
		{http.MethodPut, "/groups/g2", url.Values{"roles": {"data_writer[default:s]"}}, 200, `""`},
		{http.MethodGet, "/groups/g1", nil, 200, `{"id":"g1","roles":[{"role":"data_reader","bucket_name":"default","scope_name":"*","collection_name":"*"},{"role":"bucket_admin","bucket_name":"zeta"}],"description":"first"}`},
		{http.MethodGet, "/groups/nope", nil, 404, `"Unknown group."`},
		{http.MethodPut, "/groups/g3", url.Values{"roles": {"bogus"}}, 400,
			`{"errors":{"roles":"Cannot assign roles to user because the following roles are unknown, malformed or role parameters are undefined: [bogus]"}}`},
		{http.MethodPut, "/groups/g4", url.Values{"ldap_group_ref": {"x"}}, 400,
			`{"errors":{"ldap_group_ref":"Should be valid LDAP distinguished name: expecting_equal_sign","roles":"The value must be supplied"}}`},
		{http.MethodPut, "/groups/g5", url.Values{"roles": {"bucket_admin[zeta],ro_admin,data_reader[default],data_reader[default]"}}, 200, `""`},
		{http.MethodGet, "/groups/g5", nil, 200, `{"id":"g5","roles":[{"role":"data_reader","bucket_name":"default","scope_name":"*","collection_name":"*"},{"role":"bucket_admin","bucket_name":"zeta"},{"role":"ro_admin"}]}`},
		{http.MethodPut, "/users/local/u1", url.Values{"password": {"password"}, "roles": {"data_reader[default],ro_admin,data_reader[default],bucket_admin[zeta]"},
			"groups": {"g1,g2"}, "name": {"User1"}}, 200, `""`},
		{http.MethodGet, "/users/local/u1", nil, 200, `{"id":"u1","domain":"local","roles":[{"role":"data_writer","bucket_name":"default","scope_name":"s","collection_name":"*","origins":[{"type":"group","name":"g2"}]},{"role":"data_reader","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"group","name":"g1"},{"type":"user"}]},{"role":"bucket_admin","bucket_name":"zeta","origins":[{"type":"group","name":"g1"},{"type":"user"}]},{"role":"ro_admin","origins":[{"type":"user"}]}],"groups":["g1","g2"],"external_groups":[],"name":"User1",DATE}`},
		{http.MethodPut, "/users/local/u2", url.Values{"password": {"password"}, "groups": {"nope,g1,zz"}}, 400, `{"errors":{"groups":"Groups do not exist: nope,zz"}}`},
		{http.MethodPut, "/users/local/u3", url.Values{"password": {"password"}}, 200, `""`},
		{http.MethodGet, "/users/local/u3", nil, 200, `{"id":"u3","domain":"local","roles":[],"groups":[],"external_groups":[],DATE}`},
		{http.MethodPut, "/users/external/e1", url.Values{"roles": {"ro_admin"}}, 200, `""`},
		{http.MethodGet, "/users/external/e1", nil, 200, `{"id":"e1","domain":"external","roles":[{"role":"ro_admin","origins":[{"type":"user"}]}],"groups":[],"external_groups":[]}`},
		{http.MethodPut, "/users/external/e2", url.Values{"password": {"x"}, "roles": {"ro_admin"}}, 400, `{"errors":{"password":"Unsupported key"}}`},
		{http.MethodPut, "/users/local/u5", url.Values{"password": {"password"}, "groups": {"g2,g1"}, "roles": {"data_writer[default:s]"}}, 200, `""`},
		{http.MethodGet, "/users/local/u5", nil, 200, `{"id":"u5","domain":"local","roles":[{"role":"data_writer","bucket_name":"default","scope_name":"s","collection_name":"*","origins":[{"type":"group","name":"g2"},{"type":"user"}]},{"role":"data_reader","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"group","name":"g1"}]},{"role":"bucket_admin","bucket_name":"zeta","origins":[{"type":"group","name":"g1"}]}],"groups":["g2","g1"],"external_groups":[],DATE}`},
		{http.MethodDelete, "/groups/g2", nil, 200, `""`},
		{http.MethodGet, "/users/local/u5", nil, 200, `{"id":"u5","domain":"local","roles":[{"role":"data_writer","bucket_name":"default","scope_name":"s","collection_name":"*","origins":[{"type":"user"}]},{"role":"data_reader","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"group","name":"g1"}]},{"role":"bucket_admin","bucket_name":"zeta","origins":[{"type":"group","name":"g1"}]}],"groups":["g1"],"external_groups":[],DATE}`},
		{http.MethodDelete, "/groups/g2", nil, 404, `"Group was not found."`},
		{http.MethodDelete, "/users/local/u3", nil, 200, `""`},
		{http.MethodDelete, "/users/local/u3", nil, 404, `"User was not found."`},
		{http.MethodDelete, "/users/bogus/u3", nil, 404, `"Unknown user domain."`},
		{http.MethodGet, "/users/bogus/u3", nil, 404, `"Unknown user domain."`},
		{http.MethodPut, "/users/local/y3", url.Values{"password": {"abc"}}, 400, `{"errors":{"password":"The password must be at least 6 characters long."}}`},
		{http.MethodPut, "/users/local/y3", url.Values{"foo": {"1"}, "roles": {"bogus"}, "groups": {"nope"}}, 400,
			`{"errors":{"foo":"Unsupported key","roles":"Cannot assign roles to user because the following roles are unknown, malformed or role parameters are undefined: [bogus]","groups":"Groups do not exist: nope"}}`},
		{http.MethodPut, "/users/local/a%20b", url.Values{"password": {"password"}}, 400,
			`{"errors":{"_":"Username must not contain spaces, control or any of ()<>,;:\\\"/[]?={} characters and must be valid utf8"}}`},
		{http.MethodPut, "/users/local/y4", url.Values{"roles": {"ro_admin"}}, 400, `{"errors":{"password":"Password is required for new user."}}`},
		{http.MethodPut, "/users/local/@x", url.Values{"password": {"password"}}, 400, `{"errors":{"_":"Username cannot start with '@'"}}`},
		{http.MethodPut, "/groups/a%20b", url.Values{"roles": {"ro_admin"}}, 400,
			`{"errors":{"_":"Group name must not contain spaces, control or any of ()<>,;:\\\"/[]?={} characters and must be valid utf8"}}`},
		{http.MethodPut, "/groups/g6", url.Values{"roles": {"ro_admin"}, "foo": {"1"}, "description": {"d"}}, 400, `{"errors":{"foo":"Unsupported key"}}`},
		{http.MethodPut, "/users/local/u6", url.Values{"password": {"pass\x01word"}}, 400, `{"errors":{"password":"The password must not contain control characters"}}`},
		{http.MethodPut, "/users/external/e6", url.Values{"name": {""}, "roles": {"ro_admin"}}, 200, `""`},
		{http.MethodGet, "/users/external/e6", nil, 200, `{"id":"e6","domain":"external","roles":[{"role":"ro_admin","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"name":""}`},
		{http.MethodPut, "/groups/g7", url.Values{"roles": {""}, "ldap_group_ref": {"cn=a,dc=b"}}, 200, `""`},
		{http.MethodGet, "/groups/g7", nil, 200, `{"id":"g7","roles":[],"ldap_group_ref":"cn=a,dc=b"}`},
	}
	for _, tt := range tests {
		req, err := http.NewRequest(tt.method, baseURL+tt.path, strings.NewReader(tt.form.Encode()))
		if err != nil {
			t.Fatalf("failed to build request: %v", err)
		}
		req.SetBasicAuth("Administrator", "password")
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s failed: %v", tt.method, tt.path, err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatalf("failed to read body: %v", err)
		}
		got := passwordChangeDate.ReplaceAllString(string(body), "DATE")
		if resp.StatusCode != tt.status || got != tt.body || resp.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("%s %s: expected %d with %s, got %d with %s (%s)", tt.method, tt.path, tt.status, tt.body,
				resp.StatusCode, body, resp.Header.Get("Content-Type"))
		}
	}

	// Unsupported keys are listed in the order the request sends them.
	req, err := http.NewRequest(http.MethodPut, baseURL+"/users/local/y5", strings.NewReader("zz=1&password=password&aa=2"))
	if err != nil {
		t.Fatalf("failed to build request: %v", err)
	}
	req.SetBasicAuth("Administrator", "password")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT /users/local/y5 failed: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if expected := `{"errors":{"zz":"Unsupported key","aa":"Unsupported key"}}`; resp.StatusCode != 400 || string(body) != expected {
		t.Fatalf("expected 400 with %s, got %d with %s", expected, resp.StatusCode, body)
	}
}

// TestUserSecurityAndDomainChecks checks users and groups requests by
// security admins against Couchbase Server 7.0.5: a security role needs
// [admin, security, admin], a user's domain [admin, security, local] or
// external, and lists leave out what the requester may not read.
func TestUserSecurityAndDomainChecks(t *testing.T) {
	cluster, _ := newRBACTestCluster(t)
	addTestUser(t, cluster, "local_admin", "security_admin_local")
	addTestUser(t, cluster, "external_admin", "security_admin_external")
	addTestUser(t, cluster, "roadmin", "ro_admin")
	addTestUser(t, cluster, "admin2", "admin")
	addTestUser(t, cluster, "plain", "data_reader[default]")
	if err := cluster.Users().UpsertUser(mockauth.UpsertUserOptions{Username: "e1", Domain: "external", Roles: []string{"ro_admin"}}); err != nil {
		t.Fatalf("failed to add user: %v", err)
	}
	if err := cluster.Users().UpsertGroup(mockauth.UpsertGroupOptions{Name: "secure", Roles: []string{"ro_admin"}}); err != nil {
		t.Fatalf("failed to add group: %v", err)
	}
	if err := cluster.Users().UpsertGroup(mockauth.UpsertGroupOptions{Name: "readers", Roles: []string{"data_reader[default]"}}); err != nil {
		t.Fatalf("failed to add group: %v", err)
	}
	baseURL := fmt.Sprintf("http://127.0.0.1:%d/settings/rbac", cluster.Nodes()[0].MgmtService().ListenPort())

	const forbidden = `{"message":"Forbidden. User needs the following permissions","permissions":["%s"]}`
	tests := []struct {
		user, method, path string
		form               url.Values
		status             int
		body               string
	}{
		{"external_admin", http.MethodPut, "/users/local/x1", url.Values{"password": {"password"}, "roles": {"ro_admin"}}, 403,
			fmt.Sprintf(forbidden, "cluster.admin.security.admin!write")},
		{"local_admin", http.MethodPut, "/users/external/x2", url.Values{"roles": {"ro_admin"}}, 403,
			fmt.Sprintf(forbidden, "cluster.admin.security.admin!write")},
		{"local_admin", http.MethodPut, "/users/local/x4", url.Values{"password": {"password"}, "roles": {"data_reader[default]"}}, 200, `""`},
		{"local_admin", http.MethodPut, "/users/local/admin2", url.Values{"password": {"password"}, "roles": {"ro_admin"}}, 403,
			fmt.Sprintf(forbidden, "cluster.admin.security.admin!write")},
		{"local_admin", http.MethodGet, "/users/local/admin2", nil, 403, fmt.Sprintf(forbidden, "cluster.admin.security.admin!read")},
		{"external_admin", http.MethodPut, "/users/local/y1", url.Values{"password": {"password"}, "roles": {"data_reader[default]"}}, 403,
			fmt.Sprintf(forbidden, "cluster.admin.security.local!write")},
		{"local_admin", http.MethodPut, "/users/external/y2", url.Values{"roles": {"data_reader[default]"}}, 403,
			fmt.Sprintf(forbidden, "cluster.admin.security.external!write")},
		{"local_admin", http.MethodDelete, "/users/external/e1", nil, 403, fmt.Sprintf(forbidden, "cluster.admin.security.admin!write")},
		{"local_admin", http.MethodDelete, "/users/local/nouser", nil, 404, `"User was not found."`},
		{"local_admin", http.MethodPut, "/groups/g9", url.Values{"roles": {"ro_admin"}}, 403, fmt.Sprintf(forbidden, "cluster.admin.security.admin!write")},
		{"local_admin", http.MethodDelete, "/groups/secure", nil, 403, fmt.Sprintf(forbidden, "cluster.admin.security.admin!write")},
		{"local_admin", http.MethodGet, "/groups/secure", nil, 403, fmt.Sprintf(forbidden, "cluster.admin.security.admin!read")},
		{"local_admin", http.MethodGet, "/groups", nil, 200, `[{"id":"readers","roles":[{"role":"data_reader","bucket_name":"default","scope_name":"*","collection_name":"*"}]}]`},
	}
	for _, tt := range tests {
		resp, body := testHTTPRequest(t, tt.method, baseURL+tt.path, tt.user, tt.form)
		if resp.StatusCode != tt.status || string(body) != tt.body {
			t.Fatalf("%s %s %s: expected %d with %s, got %d with %s", tt.user, tt.method, tt.path, tt.status, tt.body, resp.StatusCode, body)
		}
	}

	ids := func(user string) string {
		_, body := testHTTPRequest(t, http.MethodGet, baseURL+"/users", user, nil)
		var users []struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(body, &users); err != nil {
			t.Fatalf("%s is not JSON: %v", body, err)
		}
		var names []string
		for _, u := range users {
			names = append(names, u.ID)
		}
		sort.Strings(names)
		return strings.Join(names, ",")
	}
	if got := ids("local_admin"); got != "plain,x4" {
		t.Fatalf("expected the local users without security roles, got %s", got)
	}
	if got := ids("external_admin"); got != "" {
		t.Fatalf("expected no users, got %s", got)
	}
}

// TestKvWithoutXerror checks the answers of kv_engine's
// Connection::remapErrorCode to a client that did not negotiate XERROR,
// against Couchbase Server 7.0.5: some statuses become ones older clients
// know, and others disconnect the client.
func TestKvWithoutXerror(t *testing.T) {
	cluster, _ := newRBACTestCluster(t)
	addTestUser(t, cluster, "reader", "data_reader[default:s:c0]", "data_writer[default:s:c0]")
	addTestUser(t, cluster, "writer", "data_writer[default:s:c0]")

	expectDisconnect := func(c *testKvConn, req *memd.Packet) {
		t.Helper()
		c.opaque++
		req.Magic = memd.CmdMagicReq
		req.Opaque = c.opaque
		if err := c.conn.WritePacket(req); err != nil {
			t.Fatalf("failed to write %s: %v", req.Command.Name(), err)
		}
		if resp, _, err := c.conn.ReadPacket(); err == nil {
			t.Fatalf("%s: expected a disconnect, got status 0x%02x", req.Command.Name(), uint16(resp.Status))
		}
	}

	expectDisconnect(dialTestKv(t, cluster, "", memd.FeatureCollections), &memd.Packet{Command: memd.CmdGetClusterConfig})
	expectDisconnect(dialTestKv(t, cluster, "", memd.FeatureCollections), &memd.Packet{Command: memd.CmdGet, Key: []byte("k")})
	expectDisconnect(dialTestKv(t, cluster, "", memd.FeatureCollections), &memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})
	expectDisconnect(dialTestKv(t, cluster, "Administrator", memd.FeatureCollections), &memd.Packet{Command: memd.CmdGet, Key: []byte("k")})
	expectDisconnect(dialTestKv(t, cluster, "reader", memd.FeatureCollections), &memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("memd")})

	c := dialTestKv(t, cluster, "reader", memd.FeatureCollections)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})
	if resp := c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.masterVbucket(cluster.Nodes()[0]), CollectionID: 0x63, Key: []byte("k")}); resp.Status != memd.StatusCollectionUnknown {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusCollectionUnknown), uint16(resp.Status))
	}
	if resp := c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.masterVbucket(cluster.Nodes()[0]), CollectionID: 0xa, Key: []byte("k")}); resp.Status != memd.StatusCollectionUnknown {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusCollectionUnknown), uint16(resp.Status))
	}
	// A user without privileges on the default collection gets
	// unknown_collection as it is, without collections too.
	c = dialTestKv(t, cluster, "reader")
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})
	resp := c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.masterVbucket(cluster.Nodes()[0]), Key: []byte("k")})
	if noPrivileges := `{"error":{"context":"Unknown scope or collection in operation"},"manifest_uid":"0"}`; resp.Status != memd.StatusCollectionUnknown || string(resp.Value) != noPrivileges {
		t.Fatalf("expected status 0x%02x with %s, got 0x%02x with %s", uint16(memd.StatusCollectionUnknown), noPrivileges, uint16(resp.Status), resp.Value)
	}
	c = dialTestKv(t, cluster, "writer", memd.FeatureCollections)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})
	expectDisconnect(c, &memd.Packet{Command: memd.CmdGet, Vbucket: c.masterVbucket(cluster.Nodes()[0]), CollectionID: 8, Key: []byte("k")})

	c = dialTestKv(t, cluster, "Administrator")
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})
	resp = c.Do(&memd.Packet{Command: memd.CmdCollectionsGetID, Value: []byte("s.nope")})
	const unknown = `{"error":{"context":"Unknown scope or collection in operation"},"manifest_uid":"5"}`
	if resp.Status != memd.StatusInvalidArgs || string(resp.Value) != unknown {
		t.Fatalf("expected status 0x%02x with %s, got 0x%02x with %s", uint16(memd.StatusInvalidArgs), unknown, uint16(resp.Status), resp.Value)
	}

	vbID := c.masterVbucket(cluster.Nodes()[0])
	key := []byte("locked")
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: vbID, Key: key, Value: []byte("1"), Extras: make([]byte, 8)})
	c.mustSucceed(&memd.Packet{Command: memd.CmdGetLocked, Vbucket: vbID, Key: key, Extras: []byte{0, 0, 0, 10}})
	steps := []struct {
		req    *memd.Packet
		status memd.StatusCode
	}{
		{&memd.Packet{Command: memd.CmdGetLocked, Key: key, Extras: []byte{0, 0, 0, 10}}, memd.StatusTmpFail},
		{&memd.Packet{Command: memd.CmdTouch, Key: key, Extras: make([]byte, 4)}, memd.StatusKeyExists},
		{&memd.Packet{Command: memd.CmdGAT, Key: key, Extras: make([]byte, 4)}, memd.StatusKeyExists},
		{&memd.Packet{Command: memd.CmdDelete, Key: key}, memd.StatusKeyExists},
		{&memd.Packet{Command: memd.CmdAppend, Key: key, Value: []byte("1")}, memd.StatusKeyExists},
		{&memd.Packet{Command: memd.CmdReplace, Key: key, Value: []byte("1"), Extras: make([]byte, 8)}, memd.StatusKeyExists},
		{&memd.Packet{Command: memd.CmdSet, Key: key, Value: []byte("1"), Extras: make([]byte, 8)}, memd.StatusKeyExists},
		{&memd.Packet{Command: memd.CmdIncrement, Key: key, Extras: make([]byte, 20)}, memd.StatusKeyExists},
		{&memd.Packet{Command: memd.CmdUnlockKey, Key: key, Cas: 1}, memd.StatusTmpFail},
		{&memd.Packet{Command: memd.CmdSubDocMultiMutation, Key: key,
			Value: encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Path: "a", Value: "1"})}, memd.StatusTmpFail},
	}
	for _, step := range steps {
		step.req.Vbucket = vbID
		if resp := c.Do(step.req); resp.Status != step.status {
			t.Fatalf("%s: expected status 0x%02x, got 0x%02x", step.req.Command.Name(), uint16(step.status), uint16(resp.Status))
		}
	}
}

// TestUserPathWithoutDomain checks PUT /settings/rbac/users/local/, which
// menelaus_web splits into the tokens of a route without a domain: an
// external user named "local", which takes no password.
func TestUserPathWithoutDomain(t *testing.T) {
	cluster, _ := newTestMgmtCluster(t)
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", cluster.Nodes()[0].MgmtService().ListenPort())
	resp, body := testHTTPRequest(t, http.MethodPut, baseURL+"/settings/rbac/users/local/", "Administrator",
		url.Values{"password": {"password"}, "roles": {"ro_admin"}})
	if resp.StatusCode != 400 || string(body) != `{"errors":{"password":"Unsupported key"}}` {
		t.Fatalf("expected 400 with an unsupported password, got %d with %s", resp.StatusCode, body)
	}
}

// TestGroupLDAPGroupRef checks the ldap_group_ref of PUT .../groups against
// Couchbase Server 7.0.5, which parses it with eldap:parse_dn/1.
func TestGroupLDAPGroupRef(t *testing.T) {
	cluster, _ := newTestMgmtCluster(t)
	baseURL := fmt.Sprintf("http://127.0.0.1:%d/settings/rbac/groups/g", cluster.Nodes()[0].MgmtService().ListenPort())
	refused := func(reason string) string {
		return `{"errors":{"ldap_group_ref":"Should be valid LDAP distinguished name: ` + reason + `"}}`
	}
	tests := []struct {
		ref    string
		status int
		body   string
	}{
		{",cn=a", 500, `["Unexpected server error, request logged."]`},
		{"=a", 400, refused("invalid_attribute_type")},
		{"cn", 400, refused("expecting_equal_sign")},
		{`cn="abc`, 400, refused("expecting_double_quote_mark")},
		{"cn=a;dc=b", 400, refused("invalid_attribute_type")},
		{"1.2.3=x", 200, `""`},
		{"cn=#zz", 400, refused("invalid_attribute_type")},
		{"cn=a,,dc=b", 200, `""`},
		{"cn=a+ou=b", 200, `""`},
		{`cn=a\`, 400, refused("invalid_attribute_type")},
		{`cn="a\q"`, 400, refused("expecting_double_quote_mark")},
		{"", 200, `""`},
		{"cn=a=b", 400, refused("invalid_attribute_type")},
	}
	for _, tt := range tests {
		resp, body := testHTTPRequest(t, http.MethodPut, baseURL, "Administrator",
			url.Values{"roles": {"ro_admin"}, "ldap_group_ref": {tt.ref}})
		if resp.StatusCode != tt.status || string(body) != tt.body {
			t.Fatalf("%q: expected %d with %s, got %d with %s", tt.ref, tt.status, tt.body, resp.StatusCode, body)
		}
	}
}

// TestUpsertUserRoleCase checks that PUT .../users takes role names in any
// case, as Couchbase Server 7.0.5 does, and keeps them in lower case.
func TestUpsertUserRoleCase(t *testing.T) {
	cluster, _ := newTestMgmtCluster(t)
	baseURL := fmt.Sprintf("http://127.0.0.1:%d/settings/rbac/users/local/t1", cluster.Nodes()[0].MgmtService().ListenPort())
	testHTTPRequest(t, http.MethodPut, baseURL, "Administrator",
		url.Values{"password": {"password"}, "roles": {"Admin,DATA_READER[default],Data_Reader[default:_default]"}})
	_, body := testHTTPRequest(t, http.MethodGet, baseURL, "Administrator", nil)
	got := passwordChangeDate.ReplaceAllString(string(body), "DATE")
	expected := `{"id":"t1","domain":"local","roles":[{"role":"data_reader","bucket_name":"default","scope_name":"_default","collection_name":"*","origins":[{"type":"user"}]},` +
		`{"role":"data_reader","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"user"}]},{"role":"admin","origins":[{"type":"user"}]}],` +
		`"groups":[],"external_groups":[],DATE}`
	if got != expected {
		t.Fatalf("expected %s, got %s", expected, got)
	}
}

// TestCollectionNamedAny checks that a collection named "any" is not taken
// for any collection: GET .../scopes leaves it out for a user with a role on
// another collection of its scope, as Couchbase Server 7.0.5 does.
func TestCollectionNamedAny(t *testing.T) {
	cluster, baseURL := newTestMgmtCluster(t)
	testMgmtRequest(t, baseURL, http.MethodPost, "/buckets/default/scopes", url.Values{"name": {"s"}})
	testMgmtRequest(t, baseURL, http.MethodPost, "/buckets/default/scopes/s/collections", url.Values{"name": {"c0"}})
	testMgmtRequest(t, baseURL, http.MethodPost, "/buckets/default/scopes/s/collections", url.Values{"name": {"any"}})
	addTestUser(t, cluster, "monitor", "data_monitoring[default:s:c0]")
	resp, body := testHTTPRequest(t, http.MethodGet, baseURL+"/buckets/default/scopes", "monitor", nil)
	if expected := `{"uid":"3","scopes":[{"name":"s","uid":"8","collections":[{"name":"c0","uid":"8","maxTTL":0}]}]}`; resp.StatusCode != 200 || string(body) != expected {
		t.Fatalf("expected 200 with %s, got %d with %s", expected, resp.StatusCode, body)
	}
}

// TestRolesDroppedWithBucket checks that dropping a bucket drops the roles
// of users and groups on it, as ns_orchestrator does with
// menelaus_users:cleanup_bucket_roles/1 before it deletes a bucket, so a
// bucket created with the same name gives them nothing, as Couchbase Server
// 7.0.5 does.
func TestRolesDroppedWithBucket(t *testing.T) {
	cluster, baseURL := newTestMgmtCluster(t)
	rbacURL := fmt.Sprintf("http://127.0.0.1:%d/settings/rbac", cluster.Nodes()[0].MgmtService().ListenPort())
	createBucket := func() {
		t.Helper()
		resp, body := testHTTPRequest(t, http.MethodPost, baseURL+"/buckets", "Administrator",
			url.Values{"name": {"tmpb"}, "ramQuotaMB": {"100"}, "bucketType": {"memcached"}})
		if resp.StatusCode != 202 {
			t.Fatalf("failed to create bucket: %d %s", resp.StatusCode, body)
		}
	}
	createBucket()
	testHTTPRequest(t, http.MethodPut, rbacURL+"/groups/gb", "Administrator", url.Values{"roles": {"bucket_admin[tmpb],ro_admin"}})
	testHTTPRequest(t, http.MethodPut, rbacURL+"/users/local/ub", "Administrator", url.Values{"password": {"password"},
		"roles": {"data_reader[tmpb],bucket_full_access[tmpb],data_reader[default]"}, "groups": {"gb"}})
	testMgmtRequest(t, baseURL, http.MethodDelete, "/buckets/tmpb", nil)
	createBucket()

	_, body := testHTTPRequest(t, http.MethodGet, rbacURL+"/users/local/ub", "Administrator", nil)
	got := passwordChangeDate.ReplaceAllString(string(body), "DATE")
	expected := `{"id":"ub","domain":"local","roles":[{"role":"data_reader","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"user"}]},` +
		`{"role":"ro_admin","origins":[{"type":"group","name":"gb"}]}],"groups":["gb"],"external_groups":[],DATE}`
	if got != expected {
		t.Fatalf("expected %s, got %s", expected, got)
	}
	if _, body := testHTTPRequest(t, http.MethodGet, rbacURL+"/groups/gb", "Administrator", nil); string(body) != `{"id":"gb","roles":[{"role":"ro_admin"}]}` {
		t.Fatalf("expected the group without its role on the bucket, got %s", body)
	}
}

// TestMgmtPathTokens checks requests whose paths have empty tokens, which
// menelaus_web leaves out, and requests no route matches, against Couchbase
// Server 7.0.5: their permission is that of the route the remaining tokens
// match, a PUT or DELETE no route matches is not allowed, and a route
// matches the whole path.
func TestMgmtPathTokens(t *testing.T) {
	cluster, _ := newTestMgmtCluster(t)
	addTestUser(t, cluster, "dr", "data_reader[default]")
	addTestUser(t, cluster, "esr", "external_stats_reader")
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", cluster.Nodes()[0].MgmtService().ListenPort())
	testHTTPRequest(t, http.MethodPut, baseURL+"/settings/rbac/groups/g1", "Administrator", url.Values{"roles": {"ro_admin"}})

	const notAllowed = "405 text/plain Method Not Allowed"
	tests := []struct {
		method, path, user string
		expected           string
	}{
		{http.MethodPut, "/settings/rbac/users/", "Administrator", notAllowed},
		{http.MethodDelete, "/settings/rbac/users/", "Administrator", notAllowed},
		{http.MethodPut, "/settings/rbac/users//", "Administrator", notAllowed},
		{http.MethodPut, "/settings/rbac/groups/", "Administrator", notAllowed},
		{http.MethodPut, "/settings/rbac/groups/", "", notAllowed},
		{http.MethodDelete, "/settings/rbac/groups/", "Administrator", notAllowed},
		{http.MethodGet, "/settings/rbac/groups/", "Administrator", `200 application/json [{"id":"g1","roles":[{"role":"ro_admin"}]}]`},
		{http.MethodGet, "/settings/rbac/users/local/", "dr", `403 application/json {"message":"Forbidden. User needs the following permissions","permissions":["cluster.admin.security!read"]}`},
		{http.MethodGet, "/pools/default/buckets/", "esr", `403 application/json {"message":"Forbidden. User needs the following permissions","permissions":["cluster.bucket[.].settings!read"]}`},
	}
	for _, tt := range tests {
		resp, body := testHTTPRequest(t, tt.method, baseURL+tt.path, tt.user, nil)
		got := fmt.Sprintf("%d %s %s", resp.StatusCode, resp.Header.Get("Content-Type"), passwordChangeDate.ReplaceAllString(string(body), "DATE"))
		if got != tt.expected {
			t.Fatalf("%s %s: expected %s, got %s", tt.method, tt.path, tt.expected, got)
		}
	}

	for path, same := range map[string]string{
		"/settings/rbac/users//":      "/settings/rbac/users",
		"/settings/rbac/users/local/": "/settings/rbac/users/local",
		"/pools/default/buckets/":     "/pools/default/buckets",
	} {
		_, body := testHTTPRequest(t, http.MethodGet, baseURL+path, "Administrator", nil)
		_, expected := testHTTPRequest(t, http.MethodGet, baseURL+same, "Administrator", nil)
		if string(body) != string(expected) {
			t.Fatalf("GET %s: expected %s, got %s", path, expected, body)
		}
	}
	if resp, _ := testHTTPRequest(t, http.MethodDelete, baseURL+"/x/pools/default/buckets/default", "dr", nil); resp.StatusCode == 200 || cluster.GetBucket("default") == nil {
		t.Fatalf("expected DELETE /x/pools/default/buckets/default to match no route, got %d", resp.StatusCode)
	}
}

// TestGetRoles checks GET /settings/rbac/roles against Couchbase Server
// 7.0.5: the roles of menelaus_roles:roles/0 in order, with their parameters
// as any, names and descriptions, without the security roles for a user who
// may not read them.
func TestGetRoles(t *testing.T) {
	cluster, _ := newTestMgmtCluster(t)
	addTestUser(t, cluster, "secl", "security_admin_local")
	rolesURL := fmt.Sprintf("http://127.0.0.1:%d/settings/rbac/roles", cluster.Nodes()[0].MgmtService().ListenPort())
	roles := []string{
		"admin", "ro_admin", "security_admin_local", "security_admin_external", "cluster_admin", "eventing_admin",
		"backup_admin", "bucket_admin", "scope_admin", "bucket_full_access", "views_admin", "views_reader",
		"replication_admin", "data_reader", "data_writer", "data_dcp_reader", "data_backup", "data_monitoring",
		"fts_admin", "fts_searcher", "query_select", "query_update", "query_insert", "query_delete",
		"query_manage_index", "query_system_catalog", "query_external_access", "query_manage_global_functions",
		"query_execute_global_functions", "query_manage_functions", "query_execute_functions",
		"query_manage_global_external_functions", "query_execute_global_external_functions",
		"query_manage_external_functions", "query_execute_external_functions", "replication_target",
		"analytics_manager", "analytics_reader", "analytics_select", "analytics_admin", "mobile_sync_gateway",
		"external_stats_reader",
	}
	entries := []string{
		`{"role":"scope_admin","bucket_name":"*","scope_name":"*","name":"Manage Scopes","desc":"Can create/delete scopes and collections within a given bucket. This user cannot access the web console."}`,
		`{"role":"data_reader","bucket_name":"*","scope_name":"*","collection_name":"*","name":"Data Reader","desc":"Can read data from a given bucket, scope or collection. This user cannot access the web console and is intended only for application access. This user can read data, but cannot write it."}`,
		`{"role":"data_backup","bucket_name":"*","name":"Data Backup & Restore","desc":"Can backup and restore a given bucket\u0019s data. This user cannot access the web console and is intended only for application access. This user can read data."}`,
	}
	for _, tt := range []struct {
		user  string
		roles []string
	}{
		{"Administrator", roles},
		{"secl", roles[4:]},
	} {
		resp, body := testHTTPRequest(t, http.MethodGet, rolesURL, tt.user, nil)
		var got []map[string]string
		if err := json.Unmarshal(body, &got); err != nil || resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("%s: expected 200 with JSON, got %d with %s", tt.user, resp.StatusCode, body)
		}
		var names []string
		for _, role := range got {
			names = append(names, role["role"])
		}
		if strings.Join(names, ",") != strings.Join(tt.roles, ",") {
			t.Fatalf("%s: expected roles %v, got %v", tt.user, tt.roles, names)
		}
		for _, entry := range entries {
			if !strings.Contains(string(body), entry) {
				t.Fatalf("%s: expected %s in %s", tt.user, entry, body)
			}
		}
	}
}

// TestRBACPathEncodedSlash checks user and group paths with an encoded "/"
// against Couchbase Server 7.0.5: menelaus_web splits the path before it
// decodes each token, so the "/" stays in the name, which is refused, and
// no user named by part of it is made.
func TestRBACPathEncodedSlash(t *testing.T) {
	cluster, _ := newTestMgmtCluster(t)
	baseURL := fmt.Sprintf("http://127.0.0.1:%d/settings/rbac", cluster.Nodes()[0].MgmtService().ListenPort())
	const invalidName = ` must not contain spaces, control or any of ()<>,;:\\\"/[]?={} characters and must be valid utf8"}}`
	tests := []struct {
		method, path string
		expected     string
	}{
		{http.MethodPut, "/users/local/a%2Fb", `400 {"errors":{"_":"Username` + invalidName},
		{http.MethodGet, "/users/local/a%2Fb", `404 "Unknown user."`},
		{http.MethodDelete, "/users/local/a%2Fb", `404 "User was not found."`},
		{http.MethodPut, "/users/a%2Fb", `400 {"errors":{"_":"Username` + invalidName},
		{http.MethodGet, "/users/a%2Fb", `404 "Unknown user domain."`},
		{http.MethodGet, "/users/local/a", `404 "Unknown user."`},
		{http.MethodPut, "/groups/g%2Fh", `400 {"errors":{"_":"Group name` + invalidName},
		{http.MethodGet, "/groups/g%2Fh", `404 "Unknown group."`},
		{http.MethodDelete, "/groups/g%2Fh", `404 "Group was not found."`},
	}
	for _, tt := range tests {
		resp, body := testHTTPRequest(t, tt.method, baseURL+tt.path, "Administrator",
			url.Values{"password": {"password"}, "roles": {"admin"}})
		if got := fmt.Sprintf("%d %s", resp.StatusCode, body); got != tt.expected {
			t.Fatalf("%s %s: expected %s, got %s", tt.method, tt.path, tt.expected, got)
		}
	}
}

// TestBasicAuthWhitespace checks Basic credentials against Couchbase Server
// 7.0.5, which decodes them with base64:decode/1, skipping whitespace, and
// requires the padding and the scheme as "Basic".
func TestBasicAuthWhitespace(t *testing.T) {
	_, baseURL := newTestMgmtCluster(t)
	const encoded = "QWRtaW5pc3RyYXRvcjpwYXNzd29yZA=="
	tests := []struct {
		header string
		status int
	}{
		{"Basic " + encoded[:5] + " " + encoded[5:], 200},
		{"Basic " + encoded[:5] + "\t" + encoded[5:], 200},
		{"Basic  " + encoded, 200},
		{"basic " + encoded, 401},
		{"Basic " + encoded[:len(encoded)-2], 401},
	}
	for _, tt := range tests {
		req, err := http.NewRequest(http.MethodGet, baseURL, nil)
		if err != nil {
			t.Fatalf("failed to build request: %v", err)
		}
		req.Header.Set("Authorization", tt.header)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != tt.status {
			t.Fatalf("%q: expected status %d, got %d", tt.header, tt.status, resp.StatusCode)
		}
	}
}

// TestKvValidationBeforePrivileges checks GET with keys as sent, the
// collection id encoded or not, against Couchbase Server 7.0.5: kv_engine's
// validator refuses a request before its privileges are checked. For a user
// with privileges on scopes or collections it looks up the scope of a
// collection other than the default one first, and closes the connection
// where that throws.
func TestKvValidationBeforePrivileges(t *testing.T) {
	cluster, _ := newRBACTestCluster(t)
	addTestUser(t, cluster, "reader", "data_reader[default:s:c0]")
	long := strings.Repeat("x", 247)
	const unknown = `{"error":{"context":"Unknown scope or collection in operation"},"manifest_uid":"%s"}`
	tests := []struct {
		name, key string
		admin     string
		reader    string
	}{
		{"cid 1", "\x01a", `04 {"error":{"context":"Invalid collection-id:1"}}`, "88 " + fmt.Sprintf(unknown, "0")},
		{"cid 7", "\x07a", `04 {"error":{"context":"Invalid collection-id:7"}}`, "disconnect"},
		{"cid 200 without a key", "\xc8\x01", `04 {"error":{"context":"No logical key found"}}`, "88 " + fmt.Sprintf(unknown, "0")},
		{"cid 8 without a key", "\x08", `04 {"error":{"context":"Key length must be >= 2"}}`, `04 {"error":{"context":"Key length must be >= 2"}}`},
		{"cid 0 without a key", "\x00", `04 {"error":{"context":"Key length must be >= 2"}}`, `04 {"error":{"context":"Key length must be >= 2"}}`},
		{"cid 99 without a key", "\x63", `04 {"error":{"context":"Key length must be >= 2"}}`, "88 " + fmt.Sprintf(unknown, "0")},
		{"cid 8 with 247", "\x08" + long, `04 {"error":{"context":"Logical key exceeds 246"}}`, `04 {"error":{"context":"Logical key exceeds 246"}}`},
		{"cid 99 with 247", "\x63" + long, `04 {"error":{"context":"Logical key exceeds 246"}}`, "88 " + fmt.Sprintf(unknown, "0")},
		{"cid 0 with 251", "\x00" + long + "xxxx", `04 {"error":{"context":"Key length exceeds 251"}}`, `04 {"error":{"context":"Key length exceeds 251"}}`},
		{"non-canonical cid 8", "\x88\x00a", `04 {"error":{"context":"Key contains non-canonical leb128"}}`, `04 {"error":{"context":"Key contains non-canonical leb128"}}`},
		{"no stop byte", "\x88\x88", `04 {"error":{"context":"No stop-byte found"}}`, "disconnect"},
		{"cid 200", "\xc8\x01x", "88 " + fmt.Sprintf(unknown, "5"), "88 " + fmt.Sprintf(unknown, "0")},
		{"cid 1 with 247", "\x01" + long, `04 {"error":{"context":"Invalid collection-id:1"}}`, "88 " + fmt.Sprintf(unknown, "0")},
		{"six bytes of leb128", "\x80\x80\x80\x80\x80\x01a", `04 {"error":{"context":"No stop-byte found"}}`, "disconnect"},
		{"cid past 32 bits", "\xff\xff\xff\xff\x0fa", "88 " + fmt.Sprintf(unknown, "5"), "88 " + fmt.Sprintf(unknown, "0")},
		{"no key", "", `04 {"error":{"context":"Request must include key"}}`, `04 {"error":{"context":"Request must include key"}}`},
	}
	for _, tt := range tests {
		for _, user := range []string{"Administrator", "reader"} {
			c := connectTestKvAs(t, cluster, user)
			c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})
			// The key is sent as it is, by a connection that does not encode
			// the collection id.
			raw := memd.NewConn(c.netConn)
			if err := raw.WritePacket(&memd.Packet{Magic: memd.CmdMagicReq, Command: memd.CmdGet, Opaque: 99, Key: []byte(tt.key)}); err != nil {
				t.Fatalf("failed to write: %v", err)
			}
			got := "disconnect"
			if resp, _, err := raw.ReadPacket(); err == nil {
				got = fmt.Sprintf("%02x %s", uint16(resp.Status), resp.Value)
			}
			expected := tt.admin
			if user == "reader" {
				expected = tt.reader
			}
			if got != expected {
				t.Fatalf("%s as %s: expected %s, got %s", tt.name, user, expected, got)
			}
		}
	}

	// Every command that takes a document key validates it.
	extras := map[memd.CmdCode][]byte{
		memd.CmdSet: make([]byte, 8), memd.CmdAdd: make([]byte, 8), memd.CmdReplace: make([]byte, 8),
		memd.CmdIncrement: make([]byte, 20), memd.CmdDecrement: make([]byte, 20),
		memd.CmdTouch: make([]byte, 4), memd.CmdGAT: make([]byte, 4), memd.CmdGetLocked: make([]byte, 4),
	}
	values := map[memd.CmdCode][]byte{
		memd.CmdSet: []byte("v"), memd.CmdAdd: []byte("v"), memd.CmdReplace: []byte("v"),
		memd.CmdAppend: []byte("v"), memd.CmdPrepend: []byte("v"),
		memd.CmdSubDocMultiLookup:   {0, 0, 0, 1, 0, 0, 0, 1, 'a'},
		memd.CmdSubDocMultiMutation: {0xc8, 0, 0, 1, 0, 0, 0, 1, 'a', '1'},
	}
	for _, command := range []memd.CmdCode{memd.CmdSet, memd.CmdAdd, memd.CmdReplace, memd.CmdDelete,
		memd.CmdIncrement, memd.CmdDecrement, memd.CmdAppend, memd.CmdPrepend, memd.CmdTouch, memd.CmdGAT,
		memd.CmdGetReplica, memd.CmdGetLocked, memd.CmdUnlockKey, memd.CmdGetMeta,
		memd.CmdSubDocMultiLookup, memd.CmdSubDocMultiMutation} {
		for key, context := range map[string]string{"\x08": "Key length must be >= 2", "\x88\x00a": "Key contains non-canonical leb128"} {
			c := connectTestKvAs(t, cluster, "Administrator")
			c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})
			raw := memd.NewConn(c.netConn)
			req := &memd.Packet{Magic: memd.CmdMagicReq, Command: command, Opaque: 99, Key: []byte(key),
				Extras: extras[command], Value: values[command]}
			if command == memd.CmdUnlockKey {
				req.Cas = 1
			}
			if err := raw.WritePacket(req); err != nil {
				t.Fatalf("failed to write: %v", err)
			}
			resp, _, err := raw.ReadPacket()
			if err != nil {
				t.Fatalf("%s %q: failed to read: %v", command.Name(), key, err)
			}
			if resp.Status != memd.StatusInvalidArgs {
				t.Fatalf("%s %q: expected status 0x04, got 0x%02x", command.Name(), key, uint16(resp.Status))
			}
			expectErrorContext(t, resp, context)
		}
	}
}

// TestGetRolesByPermission checks GET /settings/rbac/roles?permission=
// against Couchbase Server 7.0.5: the roles that allow the permission, a role
// with parameters with each as any and with each leading part of the
// parameters of the bucket, scope or collection the permission names, 400
// for a malformed permission, and 500 where ns_server fails on a name after
// any in its parameters. A role is listed as its name and the
// values of its parameters.
func TestGetRolesByPermission(t *testing.T) {
	cluster, _ := newRBACTestCluster(t)
	rolesURL := fmt.Sprintf("http://127.0.0.1:%d/settings/rbac/roles", cluster.Nodes()[0].MgmtService().ListenPort())
	tests := []struct {
		permission string
		status     int
		expected   string
	}{
		{"cluster.bucket[default].settings!read", 200,
			`admin ro_admin security_admin_local security_admin_external cluster_admin eventing_admin backup_admin bucket_admin:* bucket_admin:default bucket_full_access:* bucket_full_access:default views_admin:* views_admin:default replication_admin data_reader:*:*:* data_reader:default:*:* data_writer:*:*:* data_writer:default:*:* data_dcp_reader:*:*:* data_dcp_reader:default:*:* data_backup:* data_backup:default data_monitoring:*:*:* data_monitoring:default:*:* fts_admin:* fts_admin:default fts_searcher:*:*:* fts_searcher:default:*:* query_select:*:*:* query_select:default:*:* query_update:*:*:* query_update:default:*:* query_insert:*:*:* query_insert:default:*:* query_delete:*:*:* query_delete:default:*:* query_manage_index:*:*:* query_manage_index:default:*:* query_system_catalog query_external_access replication_target:* replication_target:default mobile_sync_gateway:* mobile_sync_gateway:default`},
		{"cluster.collection[default:s:c0].data.docs!read", 200,
			`admin eventing_admin backup_admin bucket_full_access:* bucket_full_access:default views_admin:* views_admin:default views_reader:* views_reader:default replication_admin data_reader:*:*:* data_reader:default:*:* data_reader:default:s:* data_reader:default:s:c0 data_dcp_reader:*:*:* data_dcp_reader:default:*:* data_dcp_reader:default:s:* data_dcp_reader:default:s:c0 data_backup:* data_backup:default fts_admin:* fts_admin:default query_select:*:*:* query_select:default:*:* query_select:default:s:* query_select:default:s:c0 mobile_sync_gateway:* mobile_sync_gateway:default`},
		{"cluster.admin.security!read", 200,
			`admin ro_admin security_admin_local security_admin_external`},
		{"bogus", 400,
			`{"errors":{"permission":"Malformed permission"}}`},
		{"cluster.bucket[.].settings!read", 200,
			`admin ro_admin security_admin_local security_admin_external cluster_admin eventing_admin backup_admin bucket_admin:* bucket_full_access:* views_admin:* replication_admin data_reader:*:*:* data_writer:*:*:* data_dcp_reader:*:*:* data_backup:* data_monitoring:*:*:* fts_admin:* fts_searcher:*:*:* query_select:*:*:* query_update:*:*:* query_insert:*:*:* query_delete:*:*:* query_manage_index:*:*:* query_system_catalog query_external_access replication_target:* mobile_sync_gateway:*`},
		{"cluster.scope[default:s].collections!read", 200,
			`admin ro_admin security_admin_local security_admin_external cluster_admin eventing_admin backup_admin bucket_admin:* bucket_admin:default scope_admin:*:* scope_admin:default:* scope_admin:default:s bucket_full_access:* bucket_full_access:default replication_admin data_dcp_reader:*:*:* data_dcp_reader:default:*:* data_dcp_reader:default:s:* data_backup:* data_backup:default data_monitoring:*:*:* data_monitoring:default:*:* data_monitoring:default:s:* fts_admin:* fts_admin:default query_manage_index:*:*:* query_manage_index:default:*:* query_manage_index:default:s:* query_manage_functions:*:* query_manage_functions:default:* query_manage_functions:default:s query_manage_external_functions:*:* query_manage_external_functions:default:* query_manage_external_functions:default:s replication_target:* replication_target:default mobile_sync_gateway:* mobile_sync_gateway:default`},
		{"cluster.nosuch!read", 200,
			`admin ro_admin security_admin_local security_admin_external cluster_admin eventing_admin backup_admin bucket_admin:* views_admin:* replication_admin`},
		{"cluster.bucket[nope].settings!read", 200,
			`admin ro_admin security_admin_local security_admin_external cluster_admin eventing_admin backup_admin bucket_admin:* bucket_full_access:* views_admin:* replication_admin data_reader:*:*:* data_writer:*:*:* data_dcp_reader:*:*:* data_backup:* data_monitoring:*:*:* fts_admin:* fts_searcher:*:*:* query_select:*:*:* query_update:*:*:* query_insert:*:*:* query_delete:*:*:* query_manage_index:*:*:* query_system_catalog query_external_access replication_target:* mobile_sync_gateway:*`},
		{"cluster!read", 200,
			`admin ro_admin security_admin_local security_admin_external cluster_admin eventing_admin backup_admin bucket_admin:* views_admin:* replication_admin`},
		{"cluster.collection[default:s].x!read", 400,
			`{"errors":{"permission":"Malformed permission"}}`},
		{"cluster.collection[.:c1:x]!write", 500, `["Unexpected server error, request logged."]`},
		{"cluster.scope[.:s].!read", 500, `["Unexpected server error, request logged."]`},
		{"cluster.collection[default:.:default].collections!read", 500, `["Unexpected server error, request logged."]`},
		{"cluster.collection[.:.:x]!read", 500, `["Unexpected server error, request logged."]`},
		{"cluster.collection[nope:.:x]!read", 200,
			`admin ro_admin security_admin_local security_admin_external cluster_admin eventing_admin backup_admin bucket_admin:* bucket_full_access:* mobile_sync_gateway:*`},
		{"cluster.collection[default:s:.].data.docs!read", 200,
			`admin eventing_admin backup_admin bucket_full_access:* bucket_full_access:default views_admin:* views_admin:default views_reader:* views_reader:default replication_admin data_reader:*:*:* data_reader:default:*:* data_reader:default:s:* data_dcp_reader:*:*:* data_dcp_reader:default:*:* data_dcp_reader:default:s:* data_backup:* data_backup:default fts_admin:* fts_admin:default query_select:*:*:* query_select:default:*:* query_select:default:s:* mobile_sync_gateway:* mobile_sync_gateway:default`},
	}
	for _, tt := range tests {
		resp, body := testHTTPRequest(t, http.MethodGet, rolesURL+"?permission="+url.QueryEscape(tt.permission), "Administrator", nil)
		got := string(body)
		if resp.StatusCode == 200 {
			var roles []map[string]string
			if err := json.Unmarshal(body, &roles); err != nil {
				t.Fatalf("%s: failed to decode %s: %v", tt.permission, body, err)
			}
			var listed []string
			for _, role := range roles {
				fields := []string{role["role"]}
				for _, param := range []string{"bucket_name", "scope_name", "collection_name"} {
					if value, ok := role[param]; ok {
						fields = append(fields, value)
					}
				}
				listed = append(listed, strings.Join(fields, ":"))
			}
			got = strings.Join(listed, " ")
		}
		if resp.StatusCode != tt.status || got != tt.expected {
			t.Fatalf("%s: expected %d with %s, got %d with %s", tt.permission, tt.status, tt.expected, resp.StatusCode, got)
		}
	}
}

// TestAdminNotAUser checks the administrator against Couchbase Server 7.0.5:
// it belongs to ns_server's admin domain, so it is not listed among the
// users, nor found or deleted as a local user, and a local user of the same
// name is a user of its own, whose credentials REST takes once they are not
// the administrator's.
func TestAdminNotAUser(t *testing.T) {
	cluster, _ := newTestMgmtCluster(t)
	baseURL := fmt.Sprintf("http://127.0.0.1:%d/settings/rbac/users", cluster.Nodes()[0].MgmtService().ListenPort())
	tests := []struct {
		method, path string
		form         url.Values
		expected     string
	}{
		{http.MethodGet, "", nil, `200 []`},
		{http.MethodGet, "/local/Administrator", nil, `404 "Unknown user."`},
		{http.MethodDelete, "/local/Administrator", nil, `404 "User was not found."`},
		{http.MethodPut, "/local/Administrator", url.Values{"password": {"password"}, "roles": {"ro_admin"}}, `200 ""`},
		{http.MethodGet, "", nil, `200 [{"id":"Administrator","domain":"local","roles":[{"role":"ro_admin","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],DATE}]`},
		{http.MethodDelete, "/local/Administrator", nil, `200 ""`},
	}
	for _, tt := range tests {
		resp, body := testHTTPRequest(t, tt.method, baseURL+tt.path, "Administrator", tt.form)
		if got := fmt.Sprintf("%d %s", resp.StatusCode, passwordChangeDate.ReplaceAllString(string(body), "DATE")); got != tt.expected {
			t.Fatalf("%s %s: expected %s, got %s", tt.method, tt.path, tt.expected, got)
		}
	}

	// menelaus_auth tries the administrator's credentials first, then those
	// of the local user of the same name.
	testHTTPRequest(t, http.MethodPut, baseURL+"/local/Administrator", "Administrator",
		url.Values{"password": {"other12"}, "roles": {"data_reader[default]"}})
	mgmtURL := fmt.Sprintf("http://127.0.0.1:%d", cluster.Nodes()[0].MgmtService().ListenPort())
	for _, tt := range []struct {
		path, password string
		status         int
	}{
		{"/pools/default/buckets/default", "password", 200},
		{"/settings/rbac/users", "password", 200},
		{"/pools/default/buckets/default", "other12", 200},
		{"/settings/rbac/users", "other12", 403},
	} {
		req, err := http.NewRequest(http.MethodGet, mgmtURL+tt.path, nil)
		if err != nil {
			t.Fatalf("failed to build request: %v", err)
		}
		req.SetBasicAuth("Administrator", tt.password)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != tt.status {
			t.Fatalf("GET %s with %s: expected status %d, got %d", tt.path, tt.password, tt.status, resp.StatusCode)
		}
	}
}

// TestForbiddenBodyUnescaped checks that the permission in a forbidden
// response keeps "<", ">" and "&", as ns_server's JSON encoder does.
func TestForbiddenBodyUnescaped(t *testing.T) {
	permission := mockauth.RBACPermission{
		Object:    []mockauth.Vertex{mockauth.BucketVertex(mockauth.NameParam("a<b>&c")), {Name: "views"}},
		Operation: "read",
	}
	expected := `{"message":"Forbidden. User needs the following permissions","permissions":["cluster.bucket[a<b>&c].views!read"]}`
	if body := string(forbiddenBody(permission)); body != expected {
		t.Fatalf("expected %s, got %s", expected, body)
	}
}

// TestGetUsersQuery checks GET /settings/rbac/users with a permission and
// with pages against Couchbase Server 7.0.5: handle_get_users_page/4 orders,
// filters and links the users as it does, and handle_get_all_users/3 keeps
// the users with a role allowing the permission of their own.
func TestGetUsersQuery(t *testing.T) {
	cluster, baseURL := newTestMgmtCluster(t)
	mgmtURL := fmt.Sprintf("http://127.0.0.1:%d", cluster.Nodes()[0].MgmtService().ListenPort())
	testMgmtRequest(t, baseURL, http.MethodPost, "/buckets/default/scopes", url.Values{"name": {"s1"}})
	for _, tt := range []struct {
		path string
		form url.Values
	}{
		{"/groups/g1", url.Values{"roles": {"bucket_admin[default]"}}},
		{"/users/local/alice", url.Values{"password": {"password"}, "name": {"Zed"}, "roles": {"data_reader[default]"}}},
		{"/users/local/bob", url.Values{"password": {"password"}, "roles": {"ro_admin"}}},
		{"/users/external/carol", url.Values{"name": {"Carol"}, "roles": {"data_reader[default:s1]"}}},
		{"/users/local/dave", url.Values{"password": {"password"}, "groups": {"g1"}, "roles": {""}}},
		{"/users/local/eve", url.Values{"password": {"password"}, "name": {"alice smith"}, "roles": {"data_writer[default],data_reader[default:s1]"}}},
		{"/users/external/Bob", url.Values{"roles": {"admin"}}},
	} {
		// The passwords change in the order of the names, in the same second
		// or not.
		if resp, body := testHTTPRequest(t, http.MethodPut, mgmtURL+"/settings/rbac"+tt.path, "Administrator", tt.form); resp.StatusCode != 200 {
			t.Fatalf("PUT %s: %d %s", tt.path, resp.StatusCode, body)
		}
	}

	for _, tt := range []struct {
		query, expected string
	}{
		{"/settings/rbac/users?permission=bogus",
			`400 {"errors":{"permission":"Malformed permission"}}`},
		{"/settings/rbac/users?pageSize=2",
			`200 {"total":6,"links":{"next":"/settings/rbac/users?pageSize=2&sortBy=id&order=asc&startFromDomain=local&startFrom=bob","last":"/settings/rbac/users?pageSize=2&sortBy=id&order=asc&startFromDomain=local&startFrom=dave"},"skipped":0,"users":[{"id":"Bob","domain":"external","roles":[{"role":"admin","origins":[{"type":"user"}]}],"groups":[],"external_groups":[]},{"id":"alice","domain":"local","roles":[{"role":"data_reader","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"name":"Zed","password_change_date":"T"}]}`},
		{"/settings/rbac/users?pageSize=2&startFrom=bob&startFromDomain=local",
			`200 {"total":6,"links":{"first":"/settings/rbac/users?pageSize=2&sortBy=id&order=asc","prev":"/settings/rbac/users?pageSize=2&sortBy=id&order=asc&startFromDomain=external&startFrom=Bob","next":"/settings/rbac/users?pageSize=2&sortBy=id&order=asc&startFromDomain=local&startFrom=dave","last":"/settings/rbac/users?pageSize=2&sortBy=id&order=asc&startFromDomain=local&startFrom=dave"},"skipped":2,"users":[{"id":"bob","domain":"local","roles":[{"role":"ro_admin","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"password_change_date":"T"},{"id":"carol","domain":"external","roles":[{"role":"data_reader","bucket_name":"default","scope_name":"s1","collection_name":"*","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"name":"Carol"}]}`},
		{"/settings/rbac/users?pageSize=2&startFrom=carol&startFromDomain=external",
			`200 {"total":6,"links":{"first":"/settings/rbac/users?pageSize=2&sortBy=id&order=asc","prev":"/settings/rbac/users?pageSize=2&sortBy=id&order=asc&startFromDomain=local&startFrom=alice","next":"/settings/rbac/users?pageSize=2&sortBy=id&order=asc&startFromDomain=local&startFrom=dave","last":"/settings/rbac/users?pageSize=2&sortBy=id&order=asc&startFromDomain=local&startFrom=dave"},"skipped":3,"users":[{"id":"carol","domain":"external","roles":[{"role":"data_reader","bucket_name":"default","scope_name":"s1","collection_name":"*","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"name":"Carol"},{"id":"dave","domain":"local","roles":[{"role":"bucket_admin","bucket_name":"default","origins":[{"type":"group","name":"g1"}]}],"groups":["g1"],"external_groups":[],"password_change_date":"T"}]}`},
		{"/settings/rbac/users?pageSize=2&sortBy=name&order=desc",
			`200 {"total":6,"links":{"next":"/settings/rbac/users?pageSize=2&sortBy=name&order=desc&startFromDomain=external&startFrom=carol","last":"/settings/rbac/users?pageSize=2&sortBy=name&order=desc&startFromDomain=local&startFrom=bob"},"skipped":0,"users":[{"id":"eve","domain":"local","roles":[{"role":"data_writer","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"user"}]},{"role":"data_reader","bucket_name":"default","scope_name":"s1","collection_name":"*","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"name":"alice smith","password_change_date":"T"},{"id":"alice","domain":"local","roles":[{"role":"data_reader","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"name":"Zed","password_change_date":"T"}]}`},
		{"/settings/rbac/users?pageSize=2&sortBy=name",
			`200 {"total":6,"links":{"next":"/settings/rbac/users?pageSize=2&sortBy=name&order=asc&startFromDomain=local&startFrom=dave","last":"/settings/rbac/users?pageSize=2&sortBy=name&order=asc&startFromDomain=local&startFrom=alice"},"skipped":0,"users":[{"id":"Bob","domain":"external","roles":[{"role":"admin","origins":[{"type":"user"}]}],"groups":[],"external_groups":[]},{"id":"bob","domain":"local","roles":[{"role":"ro_admin","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"password_change_date":"T"}]}`},
		{"/settings/rbac/users?pageSize=2&substr=ali",
			`200 {"total":2,"links":{},"skipped":0,"users":[{"id":"alice","domain":"local","roles":[{"role":"data_reader","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"name":"Zed","password_change_date":"T"},{"id":"eve","domain":"local","roles":[{"role":"data_writer","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"user"}]},{"role":"data_reader","bucket_name":"default","scope_name":"s1","collection_name":"*","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"name":"alice smith","password_change_date":"T"}]}`},
		{"/settings/rbac/users?pageSize=1",
			`400 {"errors":{"pageSize":"The value must be in range from 2 to 100"}}`},
		{"/settings/rbac/users?pageSize=x",
			`400 {"errors":{"pageSize":"The value must be an integer"}}`},
		{"/settings/rbac/users?pageSize=2&sortBy=foo&order=up",
			`400 {"errors":{"order":"The value must be one of the following: [asc,desc]","sortBy":"The value must be one of the following: [id,name,domain,password_change_timestamp]"}}`},
		{"/settings/rbac/users?pageSize=2&startFrom=bob",
			`400 {"errors":{"startFromDomain":"The value must be supplied"}}`},
		{"/settings/rbac/users/local?pageSize=2&startFrom=bob&startFromDomain=local",
			`400 {"errors":{"startFromDomain":"The value must not be supplied"}}`},
		{"/settings/rbac/users/local?pageSize=2&startFrom=bob",
			`200 {"total":4,"links":{"first":"/settings/rbac/users/local?pageSize=2&sortBy=id&order=asc","prev":"/settings/rbac/users/local?pageSize=2&sortBy=id&order=asc&startFrom=alice","next":"/settings/rbac/users/local?pageSize=2&sortBy=id&order=asc&startFrom=dave","last":"/settings/rbac/users/local?pageSize=2&sortBy=id&order=asc&startFrom=dave"},"skipped":1,"users":[{"id":"bob","domain":"local","roles":[{"role":"ro_admin","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"password_change_date":"T"},{"id":"dave","domain":"local","roles":[{"role":"bucket_admin","bucket_name":"default","origins":[{"type":"group","name":"g1"}]}],"groups":["g1"],"external_groups":[],"password_change_date":"T"}]}`},
		{"/settings/rbac/users?pageSize=3&sortBy=password_change_timestamp",
			`200 {"total":6,"links":{"next":"/settings/rbac/users?pageSize=3&sortBy=password_change_timestamp&order=asc&startFromDomain=local&startFrom=eve","last":"/settings/rbac/users?pageSize=3&sortBy=password_change_timestamp&order=asc&startFromDomain=local&startFrom=eve"},"skipped":0,"users":[{"id":"alice","domain":"local","roles":[{"role":"data_reader","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"name":"Zed","password_change_date":"T"},{"id":"bob","domain":"local","roles":[{"role":"ro_admin","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"password_change_date":"T"},{"id":"dave","domain":"local","roles":[{"role":"bucket_admin","bucket_name":"default","origins":[{"type":"group","name":"g1"}]}],"groups":["g1"],"external_groups":[],"password_change_date":"T"}]}`},
		{"/settings/rbac/users?pageSize=2&sortBy=domain&order=desc",
			`200 {"total":6,"links":{"next":"/settings/rbac/users?pageSize=2&sortBy=domain&order=desc&startFromDomain=local&startFrom=bob","last":"/settings/rbac/users?pageSize=2&sortBy=domain&order=desc&startFromDomain=external&startFrom=carol"},"skipped":0,"users":[{"id":"eve","domain":"local","roles":[{"role":"data_writer","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"user"}]},{"role":"data_reader","bucket_name":"default","scope_name":"s1","collection_name":"*","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"name":"alice smith","password_change_date":"T"},{"id":"dave","domain":"local","roles":[{"role":"bucket_admin","bucket_name":"default","origins":[{"type":"group","name":"g1"}]}],"groups":["g1"],"external_groups":[],"password_change_date":"T"}]}`},
		{"/settings/rbac/users?pageSize=3&permission=cluster.bucket[default].data.docs!read&substr=e",
			`200 {"total":1,"links":{},"skipped":0,"users":[{"id":"alice","domain":"local","roles":[{"role":"data_reader","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"name":"Zed","password_change_date":"T"}]}`},
		{"/settings/rbac/users?pageSize=100&order=desc",
			`200 {"total":6,"links":{},"skipped":0,"users":[{"id":"eve","domain":"local","roles":[{"role":"data_writer","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"user"}]},{"role":"data_reader","bucket_name":"default","scope_name":"s1","collection_name":"*","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"name":"alice smith","password_change_date":"T"},{"id":"dave","domain":"local","roles":[{"role":"bucket_admin","bucket_name":"default","origins":[{"type":"group","name":"g1"}]}],"groups":["g1"],"external_groups":[],"password_change_date":"T"},{"id":"carol","domain":"external","roles":[{"role":"data_reader","bucket_name":"default","scope_name":"s1","collection_name":"*","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"name":"Carol"},{"id":"bob","domain":"local","roles":[{"role":"ro_admin","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"password_change_date":"T"},{"id":"alice","domain":"local","roles":[{"role":"data_reader","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"name":"Zed","password_change_date":"T"},{"id":"Bob","domain":"external","roles":[{"role":"admin","origins":[{"type":"user"}]}],"groups":[],"external_groups":[]}]}`},
		{"/settings/rbac/users?pageSize=2&startFrom=zzz&startFromDomain=local",
			`200 {"total":6,"links":{"first":"/settings/rbac/users?pageSize=2&sortBy=id&order=asc","prev":"/settings/rbac/users?pageSize=2&sortBy=id&order=asc&startFromDomain=local&startFrom=dave"},"skipped":6,"users":[]}`},
		{"/settings/rbac/users?pageSize=2&startFrom=Bob&startFromDomain=external&foo=1",
			`200 {"total":6,"links":{"next":"/settings/rbac/users?pageSize=2&sortBy=id&order=asc&startFromDomain=local&startFrom=bob","last":"/settings/rbac/users?pageSize=2&sortBy=id&order=asc&startFromDomain=local&startFrom=dave"},"skipped":0,"users":[{"id":"Bob","domain":"external","roles":[{"role":"admin","origins":[{"type":"user"}]}],"groups":[],"external_groups":[]},{"id":"alice","domain":"local","roles":[{"role":"data_reader","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"name":"Zed","password_change_date":"T"}]}`},
		{"/settings/rbac/users?foo=1&permission=cluster.admin!read&just_validate=1",
			`200 {"errors":{}}`},
		{"/settings/rbac/users?pageSize=2&just_validate=1",
			`200 {"errors":{}}`},
		{"/settings/rbac/users?pageSize=2&substr=AL&sortBy=id&order=asc",
			`200 {"total":2,"links":{},"skipped":0,"users":[{"id":"alice","domain":"local","roles":[{"role":"data_reader","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"name":"Zed","password_change_date":"T"},{"id":"eve","domain":"local","roles":[{"role":"data_writer","bucket_name":"default","scope_name":"*","collection_name":"*","origins":[{"type":"user"}]},{"role":"data_reader","bucket_name":"default","scope_name":"s1","collection_name":"*","origins":[{"type":"user"}]}],"groups":[],"external_groups":[],"name":"alice smith","password_change_date":"T"}]}`},
		{"/settings/rbac/users?pageSize=2&permission=bogus&sortBy=x",
			`400 {"errors":{"permission":"Malformed permission","sortBy":"The value must be one of the following: [id,name,domain,password_change_timestamp]"}}`},
	} {
		resp, body := testHTTPRequest(t, http.MethodGet, mgmtURL+tt.query, "Administrator", nil)
		if got := fmt.Sprintf("%d %s", resp.StatusCode, passwordChangeDate.ReplaceAllString(string(body), `"password_change_date":"T"`)); got != tt.expected {
			t.Fatalf("GET %s: expected %s, got %s", tt.query, tt.expected, got)
		}
	}
	for _, tt := range []struct {
		query    string
		expected []string
	}{
		{"/settings/rbac/users?permission=cluster.bucket[default].data.docs!read", []string{"Bob/external", "alice/local"}},
		{"/settings/rbac/users?permission=cluster.admin!all", []string{"Bob/external"}},
		{"/settings/rbac/users?permission=cluster.bucket[default]!all", []string{"Bob/external"}},
		{"/settings/rbac/users/local?permission=cluster.bucket[default].settings!read", []string{"alice/local", "bob/local", "eve/local"}},
	} {
		resp, body := testHTTPRequest(t, http.MethodGet, mgmtURL+tt.query, "Administrator", nil)
		var users []struct{ ID, Domain string }
		if err := json.Unmarshal(body, &users); err != nil || resp.StatusCode != 200 {
			t.Fatalf("GET %s: expected users, got %d %s", tt.query, resp.StatusCode, body)
		}
		var ids []string
		for _, user := range users {
			ids = append(ids, user.ID+"/"+user.Domain)
		}
		sort.Strings(ids)
		if strings.Join(ids, ",") != strings.Join(tt.expected, ",") {
			t.Fatalf("GET %s: expected %v, got %v", tt.query, tt.expected, ids)
		}
	}

	// GET /whoami answers the identity of the request, as
	// menelaus_web_rbac:handle_whoami/1 does.
	for _, tt := range []struct {
		user, password, expected string
	}{
		{"Administrator", "password", `200 {"roles":[{"role":"admin"}],"id":"Administrator","domain":"admin"}`},
		{"dave", "password", `200 {"roles":[{"role":"bucket_admin","bucket_name":"default"}],"id":"dave","domain":"local","password_change_date":"T"}`},
		{"alice", "password", `200 {"roles":[{"role":"data_reader","bucket_name":"default","scope_name":"*","collection_name":"*"}],"id":"alice","domain":"local","name":"Zed","password_change_date":"T"}`},
		{"eve", "password", `200 {"roles":[{"role":"data_reader","bucket_name":"default","scope_name":"s1","collection_name":"*"},{"role":"data_writer","bucket_name":"default","scope_name":"*","collection_name":"*"}],"id":"eve","domain":"local","name":"alice smith","password_change_date":"T"}`},
		{"Bob", "x", `401`},
		{"nobody", "x", `401`},
		{"", "", `200 {"roles":[],"id":"","domain":"anonymous"}`},
	} {
		req, err := http.NewRequest(http.MethodGet, mgmtURL+"/whoami", nil)
		if err != nil {
			t.Fatalf("failed to build request: %v", err)
		}
		if tt.user != "" {
			req.SetBasicAuth(tt.user, tt.password)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if got := strings.TrimSpace(fmt.Sprintf("%d %s", resp.StatusCode, passwordChangeDate.ReplaceAllString(string(body), `"password_change_date":"T"`))); got != tt.expected {
			t.Fatalf("whoami as %q: expected %s, got %s", tt.user, tt.expected, got)
		}
	}
}

// TestPasswordChangeMilliseconds checks that the time a password changes
// is kept in milliseconds, as replicated_dets keeps it, so a page sorted by
// it orders two changes in the same second, while password_change_date
// gives whole seconds, as format_password_change_time/1 does.
func TestPasswordChangeMilliseconds(t *testing.T) {
	cluster, _ := newTestMgmtCluster(t)
	mgmtURL := fmt.Sprintf("http://127.0.0.1:%d", cluster.Nodes()[0].MgmtService().ListenPort())
	for _, name := range []string{"zz", "aa"} {
		testHTTPRequest(t, http.MethodPut, mgmtURL+"/settings/rbac/users/local/"+name, "Administrator",
			url.Values{"password": {"password"}, "roles": {"ro_admin"}})
		time.Sleep(5 * time.Millisecond)
	}
	_, body := testHTTPRequest(t, http.MethodGet, mgmtURL+"/settings/rbac/users?pageSize=2&sortBy=password_change_timestamp", "Administrator", nil)
	var page struct {
		Users []struct {
			ID   string `json:"id"`
			Date string `json:"password_change_date"`
		} `json:"users"`
	}
	if err := json.Unmarshal(body, &page); err != nil || len(page.Users) != 2 {
		t.Fatalf("expected a page of two users, got %s", body)
	}
	if page.Users[0].ID != "zz" || page.Users[1].ID != "aa" {
		t.Fatalf("expected zz before aa, got %s", body)
	}
	for _, user := range page.Users {
		if !strings.HasSuffix(user.Date, ".000Z") || !passwordChangeDate.MatchString(`"password_change_date":"`+user.Date+`"`) {
			t.Fatalf("expected a date in whole seconds, got %q", user.Date)
		}
	}
}
