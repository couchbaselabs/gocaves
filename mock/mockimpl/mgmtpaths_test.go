package mockimpl

import (
	"bufio"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
)

// rawMgmtRequest sends a request line as given, which a Go client may
// refuse to send, to addr, and returns the status, Content-Type and body.
func rawMgmtRequest(t *testing.T, addr, method, target, body string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	auth := base64.StdEncoding.EncodeToString([]byte("Administrator:password"))
	_, err = fmt.Fprintf(conn, "%s %s HTTP/1.1\r\nHost: x\r\nAuthorization: Basic %s\r\n"+
		"Content-Type: application/x-www-form-urlencoded\r\nContent-Length: %d\r\n\r\n%s",
		method, target, auth, len(body), body)
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("%s %s: failed to read the response: %v", method, target, err)
	}
	respBody, _ := io.ReadAll(resp.Body)
	return fmt.Sprintf("%d %s %q", resp.StatusCode, resp.Header.Get("Content-Type"), respBody)
}

// TestMgmtPathDecoding checks paths against the answers of Couchbase
// Server 7.0.5, whose menelaus_web cuts the target at the query or
// fragment, splits it at "/" dropping empty tokens, and only then unquotes
// each token with mochiweb: an encoded "/" stays within its token, an
// encoded letter matches a literal one, and a "%" that starts no escape
// is kept.
func TestMgmtPathDecoding(t *testing.T) {
	cluster := newCollectionsTestCluster(t)
	addr := fmt.Sprintf("127.0.0.1:%d", cluster.Nodes()[0].MgmtService().ListenPort())

	tests := []struct {
		method, target, body string
		want                 string
	}{
		{`DELETE`, `/pools/default/buckets/default/scopes/zz/`, ``,
			`404 application/json "{\"errors\":{\"_\":\"Scope with name \\\"zz\\\" is not found\"}}"`},
		{`DELETE`, `/pools/default/buckets/default/scopes//zz`, ``,
			`404 application/json "{\"errors\":{\"_\":\"Scope with name \\\"zz\\\" is not found\"}}"`},
		{`DELETE`, `//pools//default/buckets/default/scopes/zz`, ``,
			`404 application/json "{\"errors\":{\"_\":\"Scope with name \\\"zz\\\" is not found\"}}"`},
		{`DELETE`, `/pools/default/buckets/default/scopes/zz#frag`, ``,
			`404 application/json "{\"errors\":{\"_\":\"Scope with name \\\"zz\\\" is not found\"}}"`},
		{`DELETE`, `/pools/default/buckets/default/scopes/zz?x=1#f`, ``,
			`404 application/json "{\"errors\":{\"_\":\"Scope with name \\\"zz\\\" is not found\"}}"`},
		{`DELETE`, `/pools/default/buckets/default/scopes/a%2Fb{`, ``,
			`404 application/json "{\"errors\":{\"_\":\"Scope with name \\\"a/b{\\\" is not found\"}}"`},
		{`DELETE`, `/pools/default/buckets/default/scope%73/zz`, ``,
			`404 application/json "{\"errors\":{\"_\":\"Scope with name \\\"zz\\\" is not found\"}}"`},
		{`DELETE`, `/pools/default/buckets/default/scopes/a%`, ``,
			`404 application/json "{\"errors\":{\"_\":\"Scope with name \\\"a%\\\" is not found\"}}"`},
		{`DELETE`, `/pools/default/buckets/default/scopes/a%4`, ``,
			`404 application/json "{\"errors\":{\"_\":\"Scope with name \\\"a%4\\\" is not found\"}}"`},
		{`DELETE`, `/pools/default/buckets/default/scopes/a+b%zz`, ``,
			`404 application/json "{\"errors\":{\"_\":\"Scope with name \\\"a b%zz\\\" is not found\"}}"`},
		{`DELETE`, `/pools/default/buckets/default/scopes/%`, ``,
			`404 application/json "{\"errors\":{\"_\":\"Scope with name \\\"%\\\" is not found\"}}"`},
		{`DELETE`, `http://x/pools/default/buckets/default/scopes/abs`, ``,
			`404 application/json "{\"errors\":{\"_\":\"Scope with name \\\"abs\\\" is not found\"}}"`},
		{`DELETE`, `https://u@x:8091//pools/default/buckets/default/scopes/a%`, ``,
			`404 application/json "{\"errors\":{\"_\":\"Scope with name \\\"a%\\\" is not found\"}}"`},
		{`DELETE`, `HTTP://x?a=/pools/default/buckets/default/scopes/zz`, ``,
			`404 application/json "{\"errors\":{\"_\":\"Scope with name \\\"zz\\\" is not found\"}}"`},
		{`DELETE`, `ftp://x/pools/default/buckets/default/scopes/zz`, ``,
			`500 application/json "[\"Unexpected server error, request logged.\"]"`},
		{`DELETE`, `pools/default/buckets/default/scopes/zz`, ``,
			`500 application/json "[\"Unexpected server error, request logged.\"]"`},
		{`DELETE`, `%2Fpools/default/buckets/default/scopes/zz`, ``,
			`500 application/json "[\"Unexpected server error, request logged.\"]"`},
		{`DELETE`, `?a=b`, ``,
			`500 application/json "[\"Unexpected server error, request logged.\"]"`},
		{`DELETE`, `*`, ``,
			`500 application/json "[\"Unexpected server error, request logged.\"]"`},
		{`DELETE`, `http://x%41/pools/default/buckets/default/scopes/zz`, ``,
			`404 application/json "{\"errors\":{\"_\":\"Scope with name \\\"zz\\\" is not found\"}}"`},
		{`DELETE`, `http://x%2F/pools/default/buckets/default/scopes/zz`, ``,
			`404 application/json "{\"errors\":{\"_\":\"Scope with name \\\"zz\\\" is not found\"}}"`},
		{`DELETE`, `/pools/default/buckets/no%2Fb/scopes/zz`, ``,
			`404 text/plain "Requested resource not found.\r\n"`},
	}
	for _, tt := range tests {
		if got := rawMgmtRequest(t, addr, tt.method, tt.target, tt.body); got != tt.want {
			t.Errorf("%s %s:\n got %s\nwant %s", tt.method, tt.target, got, tt.want)
		}
	}

	// The user routes, by status and message.
	for _, tt := range []struct {
		method, target string
		status         int
		message        string
	}{
		{`GET`, `/settings/rbac/users/local/`, 200, ``},
		{`GET`, `/settings/rbac/users//local/zz`, 404, `Unknown user.`},
		{`GET`, `/settings/rbac/users/local/a%2Fb`, 404, `Unknown user.`},
		{`GET`, `/settings/rbac/users/local/zz/`, 404, `Unknown user.`},
		{`GET`, `/settings/rbac/users/loca%6C/zz`, 404, `Unknown user.`},
		{`DELETE`, `/settings/rbac/users/local/zz/`, 404, `User was not found.`},
		{`DELETE`, `/settings/rbac/users/local/a+b`, 404, `User was not found.`},
	} {
		got := rawMgmtRequest(t, addr, tt.method, tt.target, ``)
		if !strings.HasPrefix(got, fmt.Sprintf("%d ", tt.status)) || !strings.Contains(got, tt.message) {
			t.Errorf("%s %s: expected %d %s, got %s", tt.method, tt.target, tt.status, tt.message, got)
		}
	}
	if got, want := rawMgmtRequest(t, addr, `DELETE`, `/settings/rbac/users/local/zz`, ``),
		`404 application/json "\"User was not found.\""`; got != want {
		t.Errorf("DELETE a missing user:\n got %s\nwant %s", got, want)
	}
}

// TestMgmtPathWithEncodedSlash checks that an encoded "/" stays within its
// path part on management routes that name a bucket, as ns_server's
// menelaus_web splits the path before it decodes it: Couchbase Server
// 7.0.5 answers 404 for each, there being no such bucket.
func TestMgmtPathWithEncodedSlash(t *testing.T) {
	cluster := newCollectionsTestCluster(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", cluster.Nodes()[0].MgmtService().ListenPort())

	tests := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/pools/default/buckets/a%2Fb"},
		{http.MethodPost, "/pools/default/buckets/a%2Fb"},
		{http.MethodDelete, "/pools/default/buckets/a%2Fb"},
		{http.MethodPost, "/pools/default/buckets/a%2Fb/controller/doFlush"},
		{http.MethodGet, "/pools/default/b/a%2Fb"},
		{http.MethodGet, "/pools/default/buckets/a%2Fb/ddocs"},
	}
	for _, tt := range tests {
		req, err := http.NewRequest(tt.method, base+tt.path, nil)
		if err != nil {
			t.Fatalf("failed to build request: %v", err)
		}
		req.SetBasicAuth("Administrator", "password")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: request failed: %v", tt.method, tt.path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s %s: expected 404, got %d", tt.method, tt.path, resp.StatusCode)
		}
	}
}

// TestMgmtTLSPort checks that the TLS management port keeps a "%" that
// starts no escape as the plain port does, and closes a connection that
// sends plain HTTP without an answer, as Couchbase Server 7.0.5 does.
func TestMgmtTLSPort(t *testing.T) {
	cluster, err := NewCluster(mock.NewClusterOptions{InitialNode: mock.NewNodeOptions{
		Features: []mock.ClusterNodeFeature{mock.ClusterNodeFeatureTLS},
	}})
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })
	if _, err := cluster.AddBucket(mock.NewBucketOptions{Name: "default", Type: mock.BucketTypeCouchbase}); err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	if err := cluster.Users().UpsertUser(mockauth.UpsertUserOptions{Username: "Administrator", Password: "password",
		Roles: []string{"admin"}}); err != nil {
		t.Fatalf("failed to add user: %v", err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", cluster.Nodes()[0].MgmtService().ListenPortTLS())

	conn, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	auth := base64.StdEncoding.EncodeToString([]byte("Administrator:password"))
	if _, err := fmt.Fprintf(conn, "DELETE /pools/default/buckets/default/scopes/a%% HTTP/1.1\r\nHost: x\r\nAuthorization: Basic %s\r\n\r\n", auth); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("failed to read the response: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	expectGlobalError(t, resp.StatusCode, body, http.StatusNotFound, `Scope with name \"a%\" is not found`)

	plain, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	t.Cleanup(func() { _ = plain.Close() })
	if _, err := fmt.Fprintf(plain, "GET / HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if answer, _ := io.ReadAll(plain); len(answer) != 0 {
		t.Fatalf("expected no answer to plain HTTP, got %q", answer)
	}
}

// TestTLSPorts checks that every TLS port of a node completes a handshake
// with the cluster's certificate.
func TestTLSPorts(t *testing.T) {
	cluster := newCollectionsTestCluster(t)
	node := cluster.Nodes()[0]
	for name, port := range map[string]int{
		"kv":        node.KvService().ListenPortTLS(),
		"mgmt":      node.MgmtService().ListenPortTLS(),
		"views":     node.ViewService().ListenPortTLS(),
		"query":     node.QueryService().ListenPortTLS(),
		"search":    node.SearchService().ListenPortTLS(),
		"analytics": node.AnalyticsService().ListenPortTLS(),
	} {
		conn, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port), &tls.Config{InsecureSkipVerify: true})
		if err != nil {
			t.Errorf("%s: TLS handshake failed: %v", name, err)
			continue
		}
		_ = conn.Close()
	}
}
