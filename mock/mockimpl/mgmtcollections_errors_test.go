package mockimpl

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/couchbaselabs/gocaves/mock"
)

// collectionsRequest sends a request to the management port of the first
// node of cluster, with path relative to /pools/default/buckets, and returns
// the status and body. It fails the test unless a refusal carries the
// Content-Type ns_server sends: application/json for a JSON body,
// text/plain otherwise.
func collectionsRequest(t *testing.T, cluster mock.Cluster, method, path string, form url.Values) (int, []byte) {
	t.Helper()
	reqURL := fmt.Sprintf("http://127.0.0.1:%d/pools/default/buckets%s",
		cluster.Nodes()[0].MgmtService().ListenPort(), path)
	req, err := http.NewRequest(method, reqURL, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("failed to build request: %v", err)
	}
	req.SetBasicAuth("Administrator", "password")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	wantType := "text/plain"
	if json.Valid(body) {
		wantType = "application/json"
	}
	if got := resp.Header.Get("Content-Type"); resp.StatusCode != http.StatusOK && got != wantType {
		t.Fatalf("%s %s: expected Content-Type %s, got %q for body %s", method, path, wantType, got, body)
	}
	return resp.StatusCode, body
}

func newCollectionsTestCluster(t *testing.T) mock.Cluster {
	cluster, err := NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })
	return cluster
}

// expectGlobalError checks a reply_global_error answer of ns_server's
// menelaus_web_collections. message is the JSON string as ns_server sends
// it, escapes included.
func expectGlobalError(t *testing.T, status int, body []byte, wantStatus int, message string) {
	t.Helper()
	want := `{"errors":{"_":"` + message + `"}}`
	if status != wantStatus || string(body) != want {
		t.Fatalf("expected %d %q, got %d %q", wantStatus, want, status, body)
	}
}

// TestCollectionsGlobalErrors checks the status and body of each refusal of
// a scope or collection update that ns_server 7.0's collections:verify_oper
// makes, answered by reply_global_error in menelaus_web_collections. SDKs
// match "Scope with name " and " is not found" in a 404 body to tell a
// missing scope from a missing bucket.
func TestCollectionsGlobalErrors(t *testing.T) {
	cluster := newCollectionsTestCluster(t)

	manifest := cluster.GetBucket("default").CollectionManifest()
	if _, err := manifest.AddScope("app"); err != nil {
		t.Fatalf("failed to add scope: %v", err)
	}
	if _, err := manifest.AddCollection("app", "users%1", 0); err != nil {
		t.Fatalf("failed to add collection: %v", err)
	}

	tests := []struct {
		name    string
		method  string
		path    string
		form    url.Values
		status  int
		message string
	}{
		{
			name:    "create collection in missing scope",
			method:  http.MethodPost,
			path:    "/default/scopes/does-not-exist/collections",
			form:    url.Values{"name": {"coll-1"}},
			status:  http.StatusNotFound,
			message: `Scope with name \"does-not-exist\" is not found`,
		},
		{
			name:    "drop collection in missing scope",
			method:  http.MethodDelete,
			path:    "/default/scopes/does-not-exist/collections/coll-1",
			status:  http.StatusNotFound,
			message: `Scope with name \"does-not-exist\" is not found`,
		},
		{
			name:    "drop missing collection",
			method:  http.MethodDelete,
			path:    "/default/scopes/app/collections/coll%251",
			status:  http.StatusNotFound,
			message: `Collection with name \"coll%1\" in scope \"app\" is not found`,
		},
		{
			name:    "drop missing scope",
			method:  http.MethodDelete,
			path:    "/default/scopes/does%25not%25exist",
			status:  http.StatusNotFound,
			message: `Scope with name \"does%not%exist\" is not found`,
		},
		{
			name:    "drop scope named with a plus",
			method:  http.MethodDelete,
			path:    "/default/scopes/a+b",
			status:  http.StatusNotFound,
			message: `Scope with name \"a b\" is not found`,
		},
		{
			name:    "drop scope named with an encoded slash",
			method:  http.MethodDelete,
			path:    "/default/scopes/a%2Fb",
			status:  http.StatusNotFound,
			message: `Scope with name \"a/b\" is not found`,
		},
		{
			name:    "drop scope named with latin1 bytes",
			method:  http.MethodDelete,
			path:    "/default/scopes/%C3%A9%FF",
			status:  http.StatusNotFound,
			message: "Scope with name \\\"\xc3\xa9\xff\\\" is not found",
		},
		{
			name:    "create collection in scope named with a control byte",
			method:  http.MethodPost,
			path:    "/default/scopes/a%01b/collections",
			form:    url.Values{"name": {"x"}},
			status:  http.StatusNotFound,
			message: `Scope with name [97,1,98] is not found`,
		},
		{
			name:   "drop collection named with control bytes",
			method: http.MethodDelete,
			path:   "/default/scopes/app/collections/" + strings.Repeat("x%01", 30),
			status: http.StatusNotFound,
			message: `Collection with name [120,1,120,1,120,1,120,1,120,1,120,1,120,1,120,1,120,1,\n` +
				`                      120,1,120,1,120,1,120,1,120,1,120,1,120,1,120,1,120,1,\n` +
				`                      120,1,120,1,120,1,120,1,120,1,120,1,120,1,120,1,120,1,\n` +
				`                      120,1,120,1,120,1] in scope \"app\" is not found`,
		},
		{
			name:    "drop default scope",
			method:  http.MethodDelete,
			path:    "/default/scopes/_default",
			status:  http.StatusBadRequest,
			message: "Deleting _default scope is not allowed",
		},
		{
			name:    "create existing scope",
			method:  http.MethodPost,
			path:    "/default/scopes",
			form:    url.Values{"name": {"app"}},
			status:  http.StatusBadRequest,
			message: `Scope with name \"app\" already exists`,
		},
		{
			name:    "create existing collection",
			method:  http.MethodPost,
			path:    "/default/scopes/app/collections",
			form:    url.Values{"name": {"users%1"}},
			status:  http.StatusBadRequest,
			message: `Collection with name \"users%1\" in scope \"app\" already exists`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, body := collectionsRequest(t, cluster, tt.method, tt.path, tt.form)
			expectGlobalError(t, status, body, tt.status, tt.message)
		})
	}

	if _, _, err := manifest.GetByName("_default", "_default"); err != nil {
		t.Fatalf("the _default scope was dropped: %v", err)
	}
}

// TestCollectionsClusterLimits checks collections:check_cluster_limits of
// ns_server 7.0: at most 1200 scopes and 1200 collections in all buckets
// together, not counting the _default scope and collection. An update that
// verify_oper refuses is answered with its refusal even at the limit.
func TestCollectionsClusterLimits(t *testing.T) {
	cluster := newCollectionsTestCluster(t)

	other, err := cluster.AddBucket(mock.NewBucketOptions{Name: "other", Type: mock.BucketTypeCouchbase})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	manifest := cluster.GetBucket("default").CollectionManifest()
	if _, err := manifest.AddScope("app"); err != nil {
		t.Fatalf("failed to add scope: %v", err)
	}
	otherManifest := other.CollectionManifest()
	for i := 1; i < 1200; i++ {
		if _, err := otherManifest.AddScope(fmt.Sprintf("s%d", i)); err != nil {
			t.Fatalf("failed to add scope: %v", err)
		}
	}
	for i := 0; i < 1199; i++ {
		if _, err := otherManifest.AddCollection("s1", fmt.Sprintf("c%d", i), 0); err != nil {
			t.Fatalf("failed to add collection: %v", err)
		}
	}

	status, body := collectionsRequest(t, cluster, http.MethodPost, "/default/scopes/app/collections", url.Values{"name": {"last"}})
	if status != http.StatusOK {
		t.Fatalf("1200th collection: expected status 200, got %d with body %s", status, body)
	}
	status, body = collectionsRequest(t, cluster, http.MethodPost, "/default/scopes/app/collections", url.Values{"name": {"over"}})
	expectGlobalError(t, status, body, http.StatusBadRequest, "Maximum number of collections has been reached")
	status, body = collectionsRequest(t, cluster, http.MethodPost, "/default/scopes/app/collections", url.Values{"name": {"last"}})
	expectGlobalError(t, status, body, http.StatusBadRequest, `Collection with name \"last\" in scope \"app\" already exists`)
	status, body = collectionsRequest(t, cluster, http.MethodPost, "/default/scopes/nope/collections", url.Values{"name": {"over"}})
	expectGlobalError(t, status, body, http.StatusNotFound, `Scope with name \"nope\" is not found`)

	// The _default collection is not counted, so dropping it frees nothing.
	status, body = collectionsRequest(t, cluster, http.MethodDelete, "/default/scopes/_default/collections/_default", nil)
	if status != http.StatusOK {
		t.Fatalf("drop _default collection: expected status 200, got %d with body %s", status, body)
	}
	status, body = collectionsRequest(t, cluster, http.MethodPost, "/default/scopes/app/collections", url.Values{"name": {"over"}})
	expectGlobalError(t, status, body, http.StatusBadRequest, "Maximum number of collections has been reached")
	status, body = collectionsRequest(t, cluster, http.MethodDelete, "/default/scopes/app/collections/last", nil)
	if status != http.StatusOK {
		t.Fatalf("drop collection: expected status 200, got %d with body %s", status, body)
	}
	status, body = collectionsRequest(t, cluster, http.MethodPost, "/default/scopes/app/collections", url.Values{"name": {"over"}})
	if status != http.StatusOK {
		t.Fatalf("collection after drop: expected status 200, got %d with body %s", status, body)
	}

	status, body = collectionsRequest(t, cluster, http.MethodPost, "/default/scopes", url.Values{"name": {"over"}})
	expectGlobalError(t, status, body, http.StatusBadRequest, "Maximum number of scopes has been reached")
	status, body = collectionsRequest(t, cluster, http.MethodPost, "/default/scopes", url.Values{"name": {"app"}})
	expectGlobalError(t, status, body, http.StatusBadRequest, `Scope with name \"app\" already exists`)
	if _, err := otherManifest.DropScope("s2"); err != nil {
		t.Fatalf("failed to drop scope: %v", err)
	}
	status, body = collectionsRequest(t, cluster, http.MethodPost, "/default/scopes", url.Values{"name": {"over"}})
	if status != http.StatusOK {
		t.Fatalf("scope after drop: expected status 200, got %d with body %s", status, body)
	}
}

// TestCollectionsOnMemcachedBucket checks menelaus_web_collections'
// assert_api_available, which refuses scope and collection updates on a
// bucket without collections, after checking that the bucket exists.
func TestCollectionsOnMemcachedBucket(t *testing.T) {
	cluster := newCollectionsTestCluster(t)
	if _, err := cluster.AddBucket(mock.NewBucketOptions{Name: "mc", Type: mock.BucketTypeMemcached}); err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}

	updates := []struct {
		method string
		path   string
		form   url.Values
	}{
		{http.MethodPost, "/mc/scopes", url.Values{"name": {"s1"}}},
		{http.MethodPost, "/mc/scopes/_default/collections", url.Values{"name": {"c1"}}},
		{http.MethodDelete, "/mc/scopes/_default", nil},
		{http.MethodDelete, "/mc/scopes/_default/collections/_default", nil},
	}
	for _, u := range updates {
		status, body := collectionsRequest(t, cluster, u.method, u.path, u.form)
		if status != http.StatusBadRequest || string(body) != "Not allowed on this type of bucket" {
			t.Fatalf("%s %s: expected 400 Not allowed on this type of bucket, got %d %s", u.method, u.path, status, body)
		}
	}

	missing := append(updates, struct {
		method string
		path   string
		form   url.Values
	}{http.MethodGet, "/mc/scopes", nil})
	for _, u := range missing {
		status, body := collectionsRequest(t, cluster, u.method, strings.Replace(u.path, "/mc/", "/missing/", 1), u.form)
		if status != http.StatusNotFound || string(body) != "Requested resource not found.\r\n" {
			t.Fatalf("%s %s: expected 404 %q, got %d %q", u.method, u.path, "Requested resource not found.\r\n", status, body)
		}
	}
}
