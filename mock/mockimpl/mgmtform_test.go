package mockimpl

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/couchbaselabs/gocaves/mock"
)

// TestFormParsedAsMochiweb checks that a handler reading the form gets
// the parameters mochiweb_request:parse_post gives it: every pair of the
// body, a "%" that starts no escape kept where net/http's form parser
// drops the pair, and none of the query.
func TestFormParsedAsMochiweb(t *testing.T) {
	cluster, err := NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })

	reqURL := fmt.Sprintf("http://127.0.0.1:%d/settings/rbac/users/local/u1?groups=g1",
		cluster.Nodes()[0].MgmtService().ListenPort())
	req, err := http.NewRequest(http.MethodPut, reqURL, strings.NewReader("password=pass%zzword;name=a+b&roles=admin"))
	if err != nil {
		t.Fatalf("failed to build request: %v", err)
	}
	req.SetBasicAuth("Administrator", "password")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	user := cluster.Users().GetUser("u1")
	if user == nil || user.Password != "pass%zzword" || user.DisplayName != "a b" || len(user.Roles) != 1 || len(user.Groups) != 0 {
		t.Fatalf("expected user u1 with password pass%%zzword, name \"a b\", one role and no group, got %+v", user)
	}
}

// TestDefaultClusterBuckets checks the settings the buckets of the default
// cluster get, which ns_server gives buckets of their types, and that the
// memcached one keeps its quota as ns_server keeps a memcached bucket's.
func TestDefaultClusterBuckets(t *testing.T) {
	cluster, err := NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })

	for _, want := range []struct {
		name        string
		ramQuota    uint64
		compression mock.CompressionMode
		eviction    string
		backend     string
	}{
		{"default", 100 << 20, mock.CompressionModePassive, "valueOnly", "couchstore"},
		{"memd", 100 << 20, mock.CompressionModeOff, "valueOnly", "undefined"},
	} {
		b := cluster.GetBucket(want.name)
		s := b.Settings()
		if b.RamQuota() != want.ramQuota || b.CompressionMode() != want.compression ||
			s.EvictionPolicy != want.eviction || s.StorageBackend != want.backend ||
			s.DurabilityMinLevel != "none" || s.ConflictResolutionType != "seqno" || s.NumThreads != "3" {
			t.Errorf("%s: unexpected settings: quota %d, compression %s, %+v", want.name, b.RamQuota(), b.CompressionMode(), s)
		}
	}

	update := func(body string) (int, string) {
		reqURL := fmt.Sprintf("http://127.0.0.1:%d/pools/default/buckets/memd", cluster.Nodes()[0].MgmtService().ListenPort())
		req, _ := http.NewRequest(http.MethodPost, reqURL, strings.NewReader(body))
		req.SetBasicAuth("Administrator", "password")
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		return resp.StatusCode, readBody(t, resp)
	}
	if status, body := update("flushEnabled=1&ramQuotaMB=100"); status != http.StatusOK {
		t.Errorf("update keeping the quota: expected 200, got %d %s", status, body)
	}
	if status, body := update("ramQuotaMB=200"); status != http.StatusBadRequest ||
		!strings.HasPrefix(body, `{"errors":{"ramQuota":"cannot change quota of memcached buckets"},"summaries":`) {
		t.Errorf("update changing the quota: expected 400 cannot change quota, got %d %s", status, body)
	}
	if _, err := cluster.AddBucket(mock.NewBucketOptions{Name: "DEFAULT", Type: mock.BucketTypeCouchbase}); err != mock.ErrBucketExists {
		t.Errorf("adding a bucket named as another but for case: expected ErrBucketExists, got %v", err)
	}
}

// TestBodyLimit checks the largest body each port reads: a handler of the
// management port reads up to mochiweb_request's MAX_RECV_BODY of 20 MiB
// and fails on a larger one as Couchbase Server 7.0.5 does, with 500 and
// the connection closed, while one that does not read it answers as usual
// and the connection closes; the other services read a form of up to
// 10 MiB, as net/http's ParseForm does.
func TestBodyLimit(t *testing.T) {
	cluster, err := NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })
	node := cluster.Nodes()[0]
	send := func(method string, port int, path string, size int) (*http.Response, string) {
		t.Helper()
		body := "name=s&x=" + strings.Repeat("a", size-len("name=s&x="))
		req, err := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1:%d%s", port, path), strings.NewReader(body))
		if err != nil {
			t.Fatalf("failed to build request: %v", err)
		}
		req.SetBasicAuth("Administrator", "password")
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		return resp, readBody(t, resp)
	}
	post := func(port int, path string, size int) (*http.Response, string) {
		t.Helper()
		return send(http.MethodPost, port, path, size)
	}

	mgmt := node.MgmtService().ListenPort()
	if resp, body := post(mgmt, "/pools/default/buckets/default/scopes", 20<<20); resp.StatusCode != http.StatusBadRequest ||
		body != `{"errors":{"x":"Unsupported key"}}` {
		t.Errorf("20 MiB: expected the form to be validated, got %d %s", resp.StatusCode, body)
	}
	if resp, body := post(mgmt, "/pools/default/buckets/default/scopes", 20<<20+1); resp.StatusCode != http.StatusInternalServerError ||
		body != `["Unexpected server error, request logged."]` || !resp.Close {
		t.Errorf("20 MiB and a byte: expected 500 and the connection closed, got %d %s close %t", resp.StatusCode, body, resp.Close)
	}

	// A handler that does not read the body answers, and the connection
	// closes; one that reads it changes nothing.
	rev := cluster.ConfigRev()
	if resp, body := post(mgmt, "/pools/default/buckets/default", 20<<20+1); resp.StatusCode != http.StatusInternalServerError ||
		body != `["Unexpected server error, request logged."]` || cluster.ConfigRev() != rev {
		t.Errorf("bucket update with 20 MiB and a byte: expected 500 and no change, got %d %s", resp.StatusCode, body)
	}
	if resp, body := send(http.MethodPut, mgmt, "/settings/rbac/users/local/big", 20<<20+1); resp.StatusCode != http.StatusInternalServerError ||
		cluster.Users().GetUser("big") != nil {
		t.Errorf("user upsert with 20 MiB and a byte: expected 500 and no user, got %d %s", resp.StatusCode, body)
	}
	if resp, body := send(http.MethodDelete, mgmt, "/pools/default/buckets/default/scopes/nosuch", 20<<20+1); resp.StatusCode != http.StatusNotFound ||
		body != `{"errors":{"_":"Scope with name \"nosuch\" is not found"}}` || !resp.Close {
		t.Errorf("scope drop with 20 MiB and a byte: expected 404 and the connection closed, got %d %s close %t", resp.StatusCode, body, resp.Close)
	}
	if resp, body := send(http.MethodGet, mgmt, "/pools/default/buckets/default/scopes", 20<<20+1); resp.StatusCode != http.StatusOK || !resp.Close {
		t.Errorf("scopes with 20 MiB and a byte: expected 200 and the connection closed, got %d %s close %t", resp.StatusCode, body, resp.Close)
	}

	// A small body is read only by the handler that needs it; the
	// connection closes after one left unread.
	if resp, body := send(http.MethodGet, mgmt, "/pools/default/buckets/default/scopes", 100); resp.StatusCode != http.StatusOK || !resp.Close {
		t.Errorf("scopes with a small body: expected 200 and the connection closed, got %d %s close %t", resp.StatusCode, body, resp.Close)
	}
	if resp, body := post(mgmt, "/pools/default/buckets/default/scopes", 100); resp.StatusCode != http.StatusBadRequest || resp.Close {
		t.Errorf("scope create with a small body: expected 400 and the connection kept, got %d %s close %t", resp.StatusCode, body, resp.Close)
	}

	query := node.QueryService().ListenPort()
	if resp, body := post(query, "/query/service", 10<<20+1); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("query service, 10 MiB and a byte: expected 400, got %d %s", resp.StatusCode, body)
	}
}
