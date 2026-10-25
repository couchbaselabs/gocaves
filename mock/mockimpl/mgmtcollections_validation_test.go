package mockimpl

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/couchbaselabs/gocaves/mock"
)

// postToBucket sends a POST with the given query, Content-Type and body to
// path under the "default" bucket on the management port of the first node
// of cluster, and returns the status, Content-Type and body.
func postToBucket(t *testing.T, cluster mock.Cluster, path, query, contentType, body string) (int, string, string) {
	t.Helper()
	reqURL := fmt.Sprintf("http://127.0.0.1:%d/pools/default/buckets/default%s",
		cluster.Nodes()[0].MgmtService().ListenPort(), path)
	if query != "" {
		reqURL += "?" + query
	}
	req, err := http.NewRequest(http.MethodPost, reqURL, strings.NewReader(body))
	if err != nil {
		t.Fatalf("failed to build request: %v", err)
	}
	req.SetBasicAuth("Administrator", "password")
	req.Header.Set("Content-Type", contentType)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	return resp.StatusCode, resp.Header.Get("Content-Type"), string(respBody)
}

// TestCollectionFormValidation checks scope and collection creation against
// the answers of Couchbase Server 7.0.5, which runs scope_validators and
// collection_validators of menelaus_web_collections through
// validator:handle on the form mochiweb parses. An empty want is a request
// the server carries out.
func TestCollectionFormValidation(t *testing.T) {
	cluster, err := NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })
	manifest := cluster.GetBucket("default").CollectionManifest()
	if _, err := manifest.AddScope("app"); err != nil {
		t.Fatalf("failed to add scope: %v", err)
	}

	const form = "application/x-www-form-urlencoded"
	tests := []struct {
		name        string
		path        string
		query       string
		contentType string
		body        string
		status      int
		want        string
	}{
		{"scope: name missing", "/scopes", "", form, "", 400, "{\"errors\":{\"name\":\"The value must be supplied\"}}"},
		{"scope: name empty", "/scopes", "", form, "name=", 400, "{\"errors\":{\"name\":\"Name length (0) must be in the range from 1 to 251, inclusive\"}}"},
		{"scope: name with a space", "/scopes", "", form, "name=a%20b", 400, "{\"errors\":{\"name\":\"Invalid value \\\"a b\\\" : \\\"Can only contain characters A-Z, a-z, 0-9 and the following symbols _ - %\\\"\"}}"},
		{"scope: name with quote and backslash", "/scopes", "", form, "name=a%22b%5Cc", 400, "{\"errors\":{\"name\":\"Invalid value \\\"a\\\\\\\"b\\\\\\\\c\\\" : \\\"Can only contain characters A-Z, a-z, 0-9 and the following symbols _ - %\\\"\"}}"},
		{"scope: name with control escapes", "/scopes", "", form, "name=a%09%0D%0B%08%0C%1Bb", 400, "{\"errors\":{\"name\":\"Invalid value \\\"a\\\\t\\\\r\\\\v\\\\b\\\\f\\\\eb\\\" : \\\"Can only contain characters A-Z, a-z, 0-9 and the following symbols _ - %\\\"\"}}"},
		{"scope: name with DEL", "/scopes", "", form, "name=a%7Fb", 400, "{\"errors\":{\"name\":\"Invalid value [97,127,98] : \\\"Can only contain characters A-Z, a-z, 0-9 and the following symbols _ - %\\\"\"}}"},
		{"scope: name with latin1 C1 byte", "/scopes", "", form, "name=%C2%80", 400, "{\"errors\":{\"name\":\"Invalid value [194,128] : \\\"Can only contain characters A-Z, a-z, 0-9 and the following symbols _ - %\\\"\"}}"},
		{"scope: name with UTF-8", "/scopes", "", form, "name=%C3%A9", 400, "{\"errors\":{\"name\":\"Invalid value \\\"\xc3\xa9\\\" : \\\"Can only contain characters A-Z, a-z, 0-9 and the following symbols _ - %\\\"\"}}"},
		{"scope: ill-formed UTF-8", "/scopes", "", form, "name=%C3a", 400, "{\"errors\":{\"name\":\"Ill-formed utf8 name \\\"\xc3a\\\"\"}}"},
		{"scope: ill-formed UTF-8 shown as list", "/scopes", "", form, "name=%E0%80%80", 400, "{\"errors\":{\"name\":\"Ill-formed utf8 name [224,128,128]\"}}"},
		{"scope: incomplete UTF-8", "/scopes", "", form, "name=%E4%BD", 400, "{\"errors\":{\"name\":\"Incomplete utf8 name \\\"\xe4\xbd\\\"\"}}"},
		{"scope: 252 characters", "/scopes", "", form, "name=" + strings.Repeat("a", 252), 400, "{\"errors\":{\"name\":\"Name length (252) must be in the range from 1 to 251, inclusive\"}}"},
		{"scope: 252 graphemes of two code points", "/scopes", "", form, "name=" + strings.Repeat("e%CC%81", 252), 400, "{\"errors\":{\"name\":\"Name length (252) must be in the range from 1 to 251, inclusive\"}}"},
		{"scope: invalid name wrapped as a list", "/scopes", "", form, "name=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa%01", 400, "{\"errors\":{\"name\":\"Invalid value [97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,\\n               97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,\\n               97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,\\n               97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,\\n               97,97,97,97,97,97,1] : \\\"Can only contain characters A-Z, a-z, 0-9 and the following symbols _ - %\\\"\"}}"},
		{"scope: first character underscore", "/scopes", "", form, "name=_x", 400, "{\"errors\":{\"name\":\"First character must not be _ or %\"}}"},
		{"scope: first character percent", "/scopes", "", form, "name=%25x", 400, "{\"errors\":{\"name\":\"First character must not be _ or %\"}}"},
		{"scope: unsupported keys in order", "/scopes", "", form, "b=1&a=2&name=_x&c=3", 400, "{\"errors\":{\"b\":\"Unsupported key\",\"a\":\"Unsupported key\",\"c\":\"Unsupported key\",\"name\":\"First character must not be _ or %\"}}"},
		{"scope: unsupported key decoded", "/scopes", "", form, "a+b%01=1&name=vs1x", 400, "{\"errors\":{\"a b\\u0001\":\"Unsupported key\"}}"},
		{"scope: empty key from separators", "/scopes", "", form, "&name=vs1y", 400, "{\"errors\":{\"\":\"Unsupported key\"}}"},
		{"scope: semicolon separator", "/scopes", "", form, "name=vs1z;bad=2", 400, "{\"errors\":{\"bad\":\"Unsupported key\"}}"},
		{"scope: key without value", "/scopes", "", form, "name=vs1w&foo", 400, "{\"errors\":{\"foo\":\"Unsupported key\"}}"},
		{"scope: first name wins", "/scopes", "", form, "name=_bad&name=vs1v", 400, "{\"errors\":{\"name\":\"First character must not be _ or %\"}}"},
		{"scope: query ignored", "/scopes", "name=vs1u", form, "", 400, "{\"errors\":{\"name\":\"The value must be supplied\"}}"},
		{"scope: just_validate valid", "/scopes", "just_validate=1", form, "name=vs1t", 200, "{\"errors\":{}}"},
		{"scope: just_validate invalid", "/scopes", "just_validate=1", form, "name=_jv&x=1", 200, "{\"errors\":{\"x\":\"Unsupported key\",\"name\":\"First character must not be _ or %\"}}"},
		{"scope: just_validate in body", "/scopes", "", form, "name=vs1s&just_validate=1", 400, "{\"errors\":{\"just_validate\":\"Unsupported key\"}}"},
		{"scope: content type with charset", "/scopes", "", "application/x-www-form-urlencoded; charset=UTF-8", "name=_cs", 400, "{\"errors\":{\"name\":\"First character must not be _ or %\"}}"},
		{"scope: content type in other case", "/scopes", "", "Application/X-WWW-Form-Urlencoded", "name=vs1r", 400, "{\"errors\":{\"name\":\"The value must be supplied\"}}"},
		{"scope: JSON body", "/scopes", "", "application/json", "{\"name\":\"vs1q\"}", 400, "{\"errors\":{\"name\":\"The value must be supplied\"}}"},
		{"scope: undecodable escape kept", "/scopes", "", form, "name=q%zz%", 200, ""},
		{"collection: maxTTL -1", "/scopes/app/collections", "", form, "name=vc1&maxTTL=-1", 400, "{\"errors\":{\"maxTTL\":\"The value must be in range from 0 to 2147483647\"}}"},
		{"collection: maxTTL above range", "/scopes/app/collections", "", form, "name=vc1&maxTTL=2147483648", 400, "{\"errors\":{\"maxTTL\":\"The value must be in range from 0 to 2147483647\"}}"},
		{"collection: maxTTL beyond int64", "/scopes/app/collections", "", form, "name=vc1&maxTTL=99999999999999999999", 400, "{\"errors\":{\"maxTTL\":\"The value must be in range from 0 to 2147483647\"}}"},
		{"collection: maxTTL not an integer", "/scopes/app/collections", "", form, "name=vc1&maxTTL=1.5", 400, "{\"errors\":{\"maxTTL\":\"The value must be an integer\"}}"},
		{"collection: maxTTL with a space", "/scopes/app/collections", "", form, "name=vc1&maxTTL=%205", 400, "{\"errors\":{\"maxTTL\":\"The value must be an integer\"}}"},
		{"collection: maxTTL empty", "/scopes/app/collections", "", form, "name=vc1&maxTTL=", 400, "{\"errors\":{\"maxTTL\":\"The value must be an integer\"}}"},
		{"collection: maxTTL digits then letter", "/scopes/app/collections", "", form, "name=vc1&maxTTL=99999999999999999999x", 400, "{\"errors\":{\"maxTTL\":\"The value must be an integer\"}}"},
		{"collection: errors in validator order", "/scopes/app/collections", "", form, "foo=1&maxTTL=-1&name=a%20b", 400, "{\"errors\":{\"foo\":\"Unsupported key\",\"name\":\"Invalid value \\\"a b\\\" : \\\"Can only contain characters A-Z, a-z, 0-9 and the following symbols _ - %\\\"\",\"maxTTL\":\"The value must be in range from 0 to 2147483647\"}}"},
		{"collection: name missing and maxTTL", "/scopes/app/collections", "", form, "maxTTL=-1", 400, "{\"errors\":{\"name\":\"The value must be supplied\",\"maxTTL\":\"The value must be in range from 0 to 2147483647\"}}"},
		{"collection: name empty and maxTTL not an integer", "/scopes/app/collections", "", form, "maxTTL=abc&name=", 400, "{\"errors\":{\"name\":\"Name length (0) must be in the range from 1 to 251, inclusive\",\"maxTTL\":\"The value must be an integer\"}}"},
		{"collection: validated before the scope", "/scopes/nope/collections", "", form, "name=vc1&maxTTL=-1", 400, "{\"errors\":{\"maxTTL\":\"The value must be in range from 0 to 2147483647\"}}"},
		{"collection: just_validate in missing scope", "/scopes/nope/collections", "just_validate=1", form, "name=vc1", 200, "{\"errors\":{}}"},
	}
	for _, tt := range tests {
		status, contentType, body := postToBucket(t, cluster, tt.path, tt.query, tt.contentType, tt.body)
		if tt.want == "" {
			if status != http.StatusOK {
				t.Errorf("%s: expected 200, got %d %q", tt.name, status, body)
			}
			continue
		}
		if status != tt.status || body != tt.want || contentType != "application/json" {
			t.Errorf("%s:\n got %d %s %q\nwant %d application/json %q", tt.name, status, contentType, body, tt.status, tt.want)
		}
	}

	if _, _, err := manifest.GetByName("q%zz%", "_default"); err != mock.ErrCollectionNotFound {
		t.Errorf("expected scope \"q%%zz%%\" to be created, got %v", err)
	}
}

// TestCreateCollectionMaxTTL checks that a collection stores the maxTTL it
// is created with, and none without one.
func TestCreateCollectionMaxTTL(t *testing.T) {
	cluster, err := NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })
	manifest := cluster.GetBucket("default").CollectionManifest()
	if _, err := manifest.AddScope("app"); err != nil {
		t.Fatalf("failed to add scope: %v", err)
	}

	tests := []struct {
		body   string
		maxTTL uint32
	}{
		{"name=c1", 0},
		{"name=c2&maxTTL=0", 0},
		{"name=c3&maxTTL=%2B5", 5},
		{"name=c4&maxTTL=007", 7},
		{"name=c5&maxTTL=-0", 0},
		{"name=c6&maxTTL=2147483647", 2147483647},
	}
	for _, tt := range tests {
		status, _, body := postToBucket(t, cluster, "/scopes/app/collections", "", "application/x-www-form-urlencoded", tt.body)
		if status != http.StatusOK {
			t.Fatalf("%s: expected 200, got %d %s", tt.body, status, body)
		}
		name := strings.TrimPrefix(strings.SplitN(tt.body, "&", 2)[0], "name=")
		_, scopes := manifest.GetManifest()
		found := false
		for _, scope := range scopes {
			for _, col := range scope.Collections {
				if scope.Name == "app" && col.Name == name {
					found = true
					if col.MaxTTL != tt.maxTTL {
						t.Errorf("%s: expected maxTTL %d, got %d", tt.body, tt.maxTTL, col.MaxTTL)
					}
				}
			}
		}
		if !found {
			t.Errorf("%s: collection was not created", tt.body)
		}
	}
}
