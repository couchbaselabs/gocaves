package svcimpls

import (
	"bytes"
	"net/http"
	"strings"
	"sync"

	"github.com/couchbaselabs/gocaves/mock"
)

type mgmtImpl struct {
	// collectionsLock serialises collection management requests, as
	// ns_server runs each collections update from one work queue, and
	// keeps a bucket from being dropped during one.
	collectionsLock sync.Mutex
}

func (x *mgmtImpl) Register(h *hookHelper) {
	h.RegisterMgmtHandler("GET", "/", x.handlePing)
	h.RegisterMgmtHandler("GET", "/ui/index.html", x.handleIndex)
	h.RegisterMgmtHandler("GET", "/pools", x.handleGetAllPoolsConfig)
	h.RegisterMgmtHandler("GET", "/pools/default", x.handleGetPoolConfig)
	h.RegisterMgmtHandler("GET", "/pools/default/buckets", x.handleGetAllBucketConfigs)
	h.RegisterMgmtHandler("POST", "/pools/default/buckets/*/controller/doFlush", x.handleBucketFlush)
	h.RegisterMgmtHandler("POST", "/pools/default/buckets", x.handleAddBucketConfig)
	h.RegisterMgmtHandler("POST", "/pools/default/buckets/*", x.handleUpdateBucketConfig)
	h.RegisterMgmtHandler("DELETE", "/pools/default/buckets/*", x.handleDropBucketConfig)
	h.RegisterMgmtHandler("GET", "/pools/default/nodeServices", x.handleGetNodeServices)
	h.RegisterMgmtHandler("GET", "/pools/default/buckets/*", x.handleGetBucketConfig)
	h.RegisterMgmtHandler("GET", "/pools/default/b/*", x.handleGetTerseBucketConfig)
	h.RegisterMgmtHandler("GET", "/pools/default/bs/*", x.handleGetTerseBucketStreamingConfig)
	h.RegisterMgmtHandler("POST", "/pools/default/buckets/*/scopes", x.handleCreateScope)
	h.RegisterMgmtHandler("POST", "/pools/default/buckets/*/scopes/*/collections", x.handleCreateCollection)
	h.RegisterMgmtHandler("DELETE", "/pools/default/buckets/*/scopes/*", x.handleDropScope)
	h.RegisterMgmtHandler("DELETE", "/pools/default/buckets/*/scopes/*/collections/*", x.handleDropCollection)
	h.RegisterMgmtHandler("GET", "/pools/default/buckets/*/scopes", x.handleGetAllScopes)
	h.RegisterMgmtHandler("GET", "/pools/default/buckets/*/ddocs", x.handleGetAllDesignDocuments)
	h.RegisterMgmtHandler("PUT", "/settings/rbac/users/*/*", x.handleUpsertUser)
	h.RegisterMgmtHandler("GET", "/settings/rbac/users/*", x.handleGetAllUsers)
	h.RegisterMgmtHandler("GET", "/settings/rbac/users/*/*", x.handleGetUser)
	h.RegisterMgmtHandler("DELETE", "/settings/rbac/users/*/*", x.handleDropUser)
	h.RegisterMgmtHandler("GET", "/settings/rbac/roles", x.handleGetRoles)

	// Added last, this hook sees each request first.
	h.MgmtHooks.Add(func(source mock.MgmtService, req *mock.HTTPRequest, next func() *mock.HTTPResponse) *mock.HTTPResponse {
		if _, ok := mgmtTargetPath(req.Target); !ok {
			return serverErrorResponse()
		}
		return next()
	})
}

// mgmtPathTokens returns the path of a management request as ns_server's
// menelaus_web does: the target as sent, cut at the query or fragment,
// split at "/" with empty tokens dropped, and each token then unquoted by
// mochiweb, so an encoded "/" stays within its token.
func mgmtPathTokens(req *mock.HTTPRequest) []string {
	path, _ := mgmtTargetPath(req.Target)
	tokens := splitPath(path)
	for i, token := range tokens {
		tokens[i] = mock.MochiwebUnquote(token)
	}
	return tokens
}

// mgmtTargetPath returns the path of a request target as Erlang's HTTP
// packet parser gives it to mochiweb, and whether ns_server can serve it.
// An absolute-form target with an http or https scheme has the path from
// the first "/" after its authority, a query included; ns_server fails on
// any other target that does not start with "/".
func mgmtTargetPath(target string) (string, bool) {
	if strings.HasPrefix(target, "/") {
		return target, true
	}
	scheme, rest, ok := strings.Cut(target, "://")
	if !ok || !strings.EqualFold(scheme, "http") && !strings.EqualFold(scheme, "https") {
		return "", false
	}
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return rest[i:], true
	}
	return "", true
}

// serverErrorResponse answers as menelaus_util:reply_server_error does a
// request that failed unexpectedly.
func serverErrorResponse() *mock.HTTPResponse {
	return &mock.HTTPResponse{
		StatusCode: 500,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       bytes.NewReader([]byte(`["Unexpected server error, request logged."]`)),
	}
}

// splitPath cuts a request target at its query or fragment and splits it
// into the tokens string:tokens(Path, "/") gives.
func splitPath(target string) []string {
	if i := strings.IndexAny(target, "?#"); i >= 0 {
		target = target[:i]
	}
	var tokens []string
	for _, token := range strings.Split(target, "/") {
		if token != "" {
			tokens = append(tokens, token)
		}
	}
	return tokens
}

// matchPathTokens matches path tokens against the tokens of a route, in
// which "*" stands for any one token, and returns the tokens "*" matched.
func matchPathTokens(tpl, tokens []string) ([]string, bool) {
	if len(tpl) != len(tokens) {
		return nil, false
	}
	var parts []string
	for i, t := range tpl {
		switch {
		case t == "*":
			parts = append(parts, tokens[i])
		case t != tokens[i]:
			return nil, false
		}
	}
	return parts, true
}

// mgmtPathParts returns the decoded path tokens of a management request
// that the "*" of route tpl match.
func mgmtPathParts(req *mock.HTTPRequest, tpl string) []string {
	parts, _ := matchPathTokens(splitPath(tpl), mgmtPathTokens(req))
	return parts
}
