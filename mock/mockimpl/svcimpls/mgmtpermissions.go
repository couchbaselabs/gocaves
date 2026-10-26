package svcimpls

import (
	"bytes"
	"context"
	"net/http"

	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
)

// mgmtPermission gives the permission menelaus_web:get_action/4 requires
// of a route, from the path segments the "*" of its template match.
type mgmtPermission func(parts []string) mockauth.RBACPermission

func plainPermission(operation string, names ...string) mgmtPermission {
	return func([]string) mockauth.RBACPermission {
		return mockauth.RBACPermission{Object: mockauth.PlainVertices(names...), Operation: operation}
	}
}

func bucketPermission(operation string, names ...string) mgmtPermission {
	return func(parts []string) mockauth.RBACPermission {
		object := append([]mockauth.Vertex{mockauth.BucketVertex(mockauth.NameParam(parts[0]))}, mockauth.PlainVertices(names...)...)
		return mockauth.RBACPermission{Object: object, Operation: operation}
	}
}

func collectionsPermission(operation string, scope, collection func(parts []string) mockauth.Param) mgmtPermission {
	return func(parts []string) mockauth.RBACPermission {
		vertex := mockauth.CollectionVertex(mockauth.NameParam(parts[0]), scope(parts), collection(parts))
		return mockauth.RBACPermission{Object: []mockauth.Vertex{vertex, {Name: "collections"}}, Operation: operation}
	}
}

func anyParam([]string) mockauth.Param { return mockauth.ParamAny }

func allParam([]string) mockauth.Param { return mockauth.ParamAll }

func scopeParam(parts []string) mockauth.Param { return mockauth.NameParam(parts[1]) }

// mgmtPermissions are the permissions of the management routes, keyed by
// method and template. A nil permission requires none.
var mgmtPermissions = map[string]mgmtPermission{
	"GET /":                           nil,
	"GET /ui/index.html":              nil,
	"GET /pools":                      plainPermission("read", "pools"),
	"GET /pools/default":              plainPermission("read", "pools"),
	"GET /pools/default/nodeServices": plainPermission("read", "pools"),
	"GET /pools/default/buckets": func([]string) mockauth.RBACPermission {
		return mockauth.RBACPermission{
			Object:    []mockauth.Vertex{mockauth.BucketVertex(mockauth.ParamAny), {Name: "settings"}},
			Operation: "read",
		}
	},
	"GET /pools/default/buckets/*":                           bucketPermission("read", "settings"),
	"GET /pools/default/b/*":                                 bucketPermission("read", "settings"),
	"GET /pools/default/bs/*":                                bucketPermission("read", "settings"),
	"POST /pools/default/buckets":                            plainPermission("create", "buckets"),
	"POST /pools/default/buckets/*":                          bucketPermission("write", "settings"),
	"DELETE /pools/default/buckets/*":                        bucketPermission("delete"),
	"POST /pools/default/buckets/*/controller/doFlush":       bucketPermission("flush"),
	"GET /pools/default/buckets/*/ddocs":                     bucketPermission("read", "views"),
	"GET /pools/default/buckets/*/scopes":                    collectionsPermission("read", anyParam, anyParam),
	"POST /pools/default/buckets/*/scopes":                   bucketPermission("write", "collections"),
	"POST /pools/default/buckets/*/scopes/*/collections":     collectionsPermission("write", scopeParam, allParam),
	"DELETE /pools/default/buckets/*/scopes/*":               collectionsPermission("write", scopeParam, allParam),
	"DELETE /pools/default/buckets/*/scopes/*/collections/*": collectionsPermission("write", scopeParam, allParam),
	"GET /whoami":                     nil,
	"GET /settings/rbac/roles":        plainPermission("read", "admin", "security"),
	"GET /settings/rbac/users":        plainPermission("read", "admin", "security"),
	"GET /settings/rbac/users/*":      plainPermission("read", "admin", "security"),
	"GET /settings/rbac/users/*/*":    plainPermission("read", "admin", "security"),
	"PUT /settings/rbac/users":        nil,
	"PUT /settings/rbac/users/*":      plainPermission("write", "admin", "security"),
	"PUT /settings/rbac/users/*/*":    plainPermission("write", "admin", "security"),
	"DELETE /settings/rbac/users":     nil,
	"DELETE /settings/rbac/users/*":   plainPermission("write", "admin", "security"),
	"DELETE /settings/rbac/users/*/*": plainPermission("write", "admin", "security"),
	"GET /settings/rbac/groups":       plainPermission("read", "admin", "security"),
	"GET /settings/rbac/groups/*":     plainPermission("read", "admin", "security"),
	"PUT /settings/rbac/groups":       nil,
	"PUT /settings/rbac/groups/*":     plainPermission("write", "admin", "security"),
	"DELETE /settings/rbac/groups":    nil,
	"DELETE /settings/rbac/groups/*":  plainPermission("write", "admin", "security"),
}

// checkMgmtPermission answers a request whose user is not authenticated or
// lacks permission, as menelaus_web:perform_action/2 does, or returns nil.
func checkMgmtPermission(source mock.MgmtService, req *mock.HTTPRequest, permission mgmtPermission, parts []string) *mock.HTTPResponse {
	if permission == nil {
		return nil
	}
	user := req.AuthenticatedUser(source.Node().Cluster().Users())
	if user == nil {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Header:     http.Header{"WWW-Authenticate": {`Basic realm="Couchbase Server Admin / REST"`}},
			Body:       bytes.NewReader(nil),
		}
	}
	required := permission(parts)
	if !user.IsAllowed(mock.ClusterResources{Cluster: source.Node().Cluster()}, required) {
		return forbiddenResponse(required)
	}
	ctx := req.Context
	if ctx == nil {
		ctx = context.Background()
	}
	req.Context = context.WithValue(ctx, requestUserKey{}, user)
	return nil
}

type requestUserKey struct{}

// requestUser returns the user checkMgmtPermission authenticated a request
// of a route that requires a permission as, the identity ns_server handles
// the request with.
func requestUser(req *mock.HTTPRequest) *mockauth.User {
	user, _ := req.Context.Value(requestUserKey{}).(*mockauth.User)
	return user
}
