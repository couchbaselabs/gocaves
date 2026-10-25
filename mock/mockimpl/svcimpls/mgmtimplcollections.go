package svcimpls

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
)

// collectionsGlobalError answers as menelaus_web_collections:reply_global_error.
func collectionsGlobalError(statusCode int, msg string) *mock.HTTPResponse {
	return &mock.HTTPResponse{
		StatusCode: statusCode,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       bytes.NewReader([]byte(`{"errors":{"_":` + erlJSONString(msg) + `}}`)),
	}
}

func collectionsTextResponse(statusCode int, msg string) *mock.HTTPResponse {
	return &mock.HTTPResponse{
		StatusCode: statusCode,
		Header:     http.Header{"Content-Type": {"text/plain"}},
		Body:       bytes.NewReader([]byte(msg)),
	}
}

// collectionsNotFoundResponse answers a request for a missing bucket as
// menelaus_util:reply_not_found does.
func collectionsNotFoundResponse() *mock.HTTPResponse {
	return collectionsTextResponse(404, "Requested resource not found.\r\n")
}

func scopeNotFoundResponse(scope string) *mock.HTTPResponse {
	return collectionsGlobalError(404, erlFormat("Scope with name ~p is not found", scope))
}

// ns_server's MAX_SCOPES_SUPPORTED and MAX_COLLECTIONS_SUPPORTED, which
// collections:check_cluster_limits compares with the counts of every bucket.
const (
	maxScopesSupported      = 1200
	maxCollectionsSupported = 1200
)

// clusterCollectionCounts returns the number of scopes and collections in
// all buckets, without the _default scope and collection, which ns_server's
// num_scopes and num_collections counters leave out.
func clusterCollectionCounts(cluster mock.Cluster) (int, int) {
	numScopes, numCollections := 0, 0
	for _, bucket := range cluster.GetAllBuckets() {
		if bucket.BucketType() == mock.BucketTypeMemcached {
			continue
		}
		_, scopes := bucket.CollectionManifest().GetManifest()
		for _, scope := range scopes {
			if scope.UID != 0 {
				numScopes++
			}
			for _, col := range scope.Collections {
				if col.UID != 0 {
					numCollections++
				}
			}
		}
	}
	return numScopes, numCollections
}

func (x *mgmtImpl) handleCreateCollection(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	if !source.CheckAuthenticated(mockauth.PermissionClusterRead, "", "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}
	pathParts := mgmtPathParts(req, "/pools/default/buckets/*/scopes/*/collections")
	if len(pathParts) != 2 {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte("invalid path")),
		}
	}
	bucketName := pathParts[0]
	scope := pathParts[1]
	if !source.CheckAuthenticated(mockauth.PermissionBucketManage, bucketName, scope, "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}
	bucket := source.Node().Cluster().GetBucket(bucketName)
	if bucket == nil {
		return collectionsNotFoundResponse()
	}
	if bucket.BucketType() == mock.BucketTypeMemcached {
		return collectionsTextResponse(400, "Not allowed on this type of bucket")
	}
	// The body is read before the lock is taken, and the bucket looked up
	// again under it, so it is not dropped while the update runs.
	params := parsePost(req)
	x.collectionsLock.Lock()
	defer x.collectionsLock.Unlock()
	if bucket = source.Node().Cluster().GetBucket(bucketName); bucket == nil {
		return collectionsNotFoundResponse()
	}

	v := newFormValidator(params)
	maxTTL := v.validateMaxTTL()
	v.validateScopeName()
	v.unsupported()
	if resp := v.response(req); resp != nil {
		return resp
	}
	name, _ := paramValue(v.params, "name")
	manifest := bucket.CollectionManifest()

	_, _, err := manifest.GetByName(scope, name)
	switch err {
	case nil:
		return collectionsGlobalError(400,
			erlFormat("Collection with name ~p in scope ~p already exists", name, scope))
	case mock.ErrScopeNotFound:
		return scopeNotFoundResponse(scope)
	}
	if _, numCollections := clusterCollectionCounts(source.Node().Cluster()); numCollections >= maxCollectionsSupported {
		return collectionsGlobalError(400, "Maximum number of collections has been reached")
	}

	uid, err := manifest.AddCollection(scope, name, uint32(maxTTL))
	switch err {
	case mock.ErrCollectionExists:
		return collectionsGlobalError(400,
			erlFormat("Collection with name ~p in scope ~p already exists", name, scope))
	case mock.ErrScopeNotFound:
		return scopeNotFoundResponse(scope)
	}

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte(fmt.Sprintf(`{"uid": "%d"}`, uid))),
	}
}

func (x *mgmtImpl) handleCreateScope(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	if !source.CheckAuthenticated(mockauth.PermissionClusterRead, "", "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}
	pathParts := mgmtPathParts(req, "/pools/default/buckets/*/scopes")
	if len(pathParts) != 1 {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte("invalid path")),
		}
	}
	bucketName := pathParts[0]
	if !source.CheckAuthenticated(mockauth.PermissionBucketManage, bucketName, "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}
	bucket := source.Node().Cluster().GetBucket(bucketName)
	if bucket == nil {
		return collectionsNotFoundResponse()
	}
	if bucket.BucketType() == mock.BucketTypeMemcached {
		return collectionsTextResponse(400, "Not allowed on this type of bucket")
	}
	// The body is read before the lock is taken, and the bucket looked up
	// again under it, so it is not dropped while the update runs.
	params := parsePost(req)
	x.collectionsLock.Lock()
	defer x.collectionsLock.Unlock()
	if bucket = source.Node().Cluster().GetBucket(bucketName); bucket == nil {
		return collectionsNotFoundResponse()
	}

	v := newFormValidator(params)
	v.validateScopeName()
	v.unsupported()
	if resp := v.response(req); resp != nil {
		return resp
	}
	name, _ := paramValue(v.params, "name")

	manifest := bucket.CollectionManifest()

	if _, _, err := manifest.GetByName(name, ""); err != mock.ErrScopeNotFound {
		return collectionsGlobalError(400, erlFormat("Scope with name ~p already exists", name))
	}
	if numScopes, _ := clusterCollectionCounts(source.Node().Cluster()); numScopes >= maxScopesSupported {
		return collectionsGlobalError(400, "Maximum number of scopes has been reached")
	}

	uid, err := manifest.AddScope(name)
	switch err {
	case mock.ErrScopeExists:
		return collectionsGlobalError(400, erlFormat("Scope with name ~p already exists", name))
	}

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte(fmt.Sprintf(`{"uid": "%d"}`, uid))),
	}
}

func (x *mgmtImpl) handleDropCollection(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	if !source.CheckAuthenticated(mockauth.PermissionClusterRead, "", "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}
	pathParts := mgmtPathParts(req, "/pools/default/buckets/*/scopes/*/collections/*")
	if len(pathParts) != 3 {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte("invalid path")),
		}
	}
	bucketName := pathParts[0]
	scope := pathParts[1]
	collection := pathParts[2]
	if !source.CheckAuthenticated(mockauth.PermissionBucketManage, bucketName, scope, collection, req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}
	// The bucket is looked up under the lock, so it is not dropped while
	// the update runs.
	x.collectionsLock.Lock()
	defer x.collectionsLock.Unlock()
	bucket := source.Node().Cluster().GetBucket(bucketName)
	if bucket == nil {
		return collectionsNotFoundResponse()
	}
	if bucket.BucketType() == mock.BucketTypeMemcached {
		return collectionsTextResponse(400, "Not allowed on this type of bucket")
	}

	manifest := bucket.CollectionManifest()
	uid, err := manifest.DropCollection(scope, collection)
	switch err {
	case mock.ErrCollectionNotFound:
		return collectionsGlobalError(404,
			erlFormat("Collection with name ~p in scope ~p is not found", collection, scope))
	case mock.ErrScopeNotFound:
		return scopeNotFoundResponse(scope)
	}

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte(fmt.Sprintf(`{"uid": "%d"}`, uid))),
	}
}

func (x *mgmtImpl) handleDropScope(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	if !source.CheckAuthenticated(mockauth.PermissionClusterRead, "", "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}
	pathParts := mgmtPathParts(req, "/pools/default/buckets/*/scopes/*")
	if len(pathParts) != 2 {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte("invalid path")),
		}
	}
	bucketName := pathParts[0]
	scope := pathParts[1]
	if !source.CheckAuthenticated(mockauth.PermissionBucketManage, bucketName, scope, "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}
	// The bucket is looked up under the lock, so it is not dropped while
	// the update runs.
	x.collectionsLock.Lock()
	defer x.collectionsLock.Unlock()
	bucket := source.Node().Cluster().GetBucket(bucketName)
	if bucket == nil {
		return collectionsNotFoundResponse()
	}
	if bucket.BucketType() == mock.BucketTypeMemcached {
		return collectionsTextResponse(400, "Not allowed on this type of bucket")
	}

	if scope == "_default" {
		return collectionsGlobalError(400, "Deleting _default scope is not allowed")
	}

	manifest := bucket.CollectionManifest()
	uid, err := manifest.DropScope(scope)
	switch err {
	case mock.ErrScopeNotFound:
		return scopeNotFoundResponse(scope)
	}

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte(fmt.Sprintf(`{"uid": "%d"}`, uid))),
	}
}

func (x *mgmtImpl) handleGetAllScopes(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	if !source.CheckAuthenticated(mockauth.PermissionClusterRead, "", "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}
	pathParts := mgmtPathParts(req, "/pools/default/buckets/*/scopes")
	if len(pathParts) != 1 {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte("invalid path")),
		}
	}
	bucketName := pathParts[0]
	if !source.CheckAuthenticated(mockauth.PermissionBucketManage, bucketName, "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}
	bucket := source.Node().Cluster().GetBucket(bucketName)
	if bucket == nil {
		return collectionsNotFoundResponse()
	}

	manifest := bucket.CollectionManifest()
	uid, scopes := manifest.GetManifest()

	jsonMani := buildJSONManifest(uid, scopes)

	b, err := json.Marshal(jsonMani)
	if err != nil {
		return &mock.HTTPResponse{
			StatusCode: 500,
			Body:       bytes.NewReader([]byte(err.Error())),
		}
	}

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader(b),
	}
}

func buildJSONManifest(uid uint64, scopes []mock.CollectionManifestScope) jsonManifest {
	jsonMani := jsonManifest{
		UID:    strconv.Itoa(int(uid)),
		Scopes: make([]jsonScope, len(scopes)),
	}

	for i, scop := range scopes {
		jsonScop := jsonScope{
			UID:         strconv.Itoa(int(scop.UID)),
			Name:        scop.Name,
			Collections: make([]jsonCollection, len(scop.Collections)),
		}

		for j, col := range scop.Collections {
			jsonScop.Collections[j] = jsonCollection{
				UID:    strconv.Itoa(int(col.UID)),
				Name:   col.Name,
				MaxTTL: col.MaxTTL,
			}
		}
		jsonMani.Scopes[i] = jsonScop
	}

	return jsonMani
}

type jsonManifest struct {
	UID    string      `json:"uid"`
	Scopes []jsonScope `json:"scopes"`
}

type jsonScope struct {
	UID         string           `json:"uid"`
	Name        string           `json:"name"`
	Collections []jsonCollection `json:"collections"`
}

type jsonCollection struct {
	UID    string `json:"uid"`
	Name   string `json:"name"`
	MaxTTL uint32 `json:"maxTTL,omitempty"`
}
