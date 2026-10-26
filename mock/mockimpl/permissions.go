package mockimpl

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
)

type forbiddenResponse struct {
	Message     string   `json:"message"`
	Permissions []string `json:"permissions"`
}

// forbiddenBody is menelaus_web_rbac:forbidden_response/1.
func forbiddenBody(permission mockauth.RBACPermission) []byte {
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(forbiddenResponse{
		Message:     "Forbidden. User needs the following permissions",
		Permissions: []string{permission.String()},
	})
	return bytes.TrimSuffix(body.Bytes(), []byte("\n"))
}

// checkViewPermission answers a request for a bucket database as
// capi_frontend:verify_bucket_auth/3 does when the bucket does not exist, the
// user is not authenticated, or lacks the permission of the request: on
// [{bucket, Bucket}, views] for a design document, else on
// [{bucket, Bucket}, data, couchdb], read for GET and write otherwise.
// couch_httpd splits the path as sent into tokens, without empty ones, and
// decodes each; the database is the first, its bucket the part before a "/"
// or "+". It returns nil for any other request.
func checkViewPermission(cluster mock.Cluster, req *mock.HTTPRequest) *mock.HTTPResponse {
	path := req.Target
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	var tokens []string
	for _, token := range strings.Split(path, "/") {
		if token != "" {
			tokens = append(tokens, mock.MochiwebUnquote(token))
		}
	}
	if len(tokens) == 0 || strings.HasPrefix(tokens[0], "_") {
		return nil
	}
	// couch_httpd ends a JSON body with a newline.
	reply := func(status int, header http.Header, body []byte) *mock.HTTPResponse {
		header.Set("Content-Type", "text/plain;charset=utf-8")
		return &mock.HTTPResponse{StatusCode: status, Header: header, Body: bytes.NewReader(append(body, '\n'))}
	}
	bucket, _, _ := strings.Cut(tokens[0], "/")
	bucket, _, _ = strings.Cut(bucket, "+")
	if cluster.GetBucket(bucket) == nil {
		return reply(404, http.Header{}, []byte(`{"error":"not_found","reason":"missing"}`))
	}
	user := req.AuthenticatedUser(cluster.Users())
	if user == nil {
		return reply(401, http.Header{"WWW-Authenticate": {`Basic realm="capi"`}},
			[]byte(`{"error":"unauthorized","reason":"password required"}`))
	}
	operation := "write"
	if req.Method == "GET" {
		operation = "read"
	}
	object := []mockauth.Vertex{mockauth.BucketVertex(mockauth.NameParam(bucket)), {Name: "data"}, {Name: "couchdb"}}
	if len(tokens) > 1 && tokens[1] == "_design" {
		object = []mockauth.Vertex{mockauth.BucketVertex(mockauth.NameParam(bucket)), {Name: "views"}}
	}
	permission := mockauth.RBACPermission{Object: object, Operation: operation}
	if !user.IsAllowed(mock.ClusterResources{Cluster: cluster}, permission) {
		return reply(403, http.Header{}, forbiddenBody(permission))
	}
	return nil
}
