package svcimpls

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/couchbaselabs/gocaves/contrib/pathparse"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
)

func (x *mgmtImpl) handleCreateCollection(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	if !source.CheckAuthenticated(mockauth.PermissionClusterRead, "", "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}
	pathParts := pathparse.ParseParts(req.URL.Path, "/pools/default/buckets/*/scopes/*/collections")
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
		return &mock.HTTPResponse{
			StatusCode: 404,
			Body:       bytes.NewReader([]byte("Requested resource not found.")),
		}
	}

	name := req.Form.Get("name")
	if name == "" {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte(`{"errors":{"name":"The value must be supplied"}`)),
		}
	}

	if strings.HasPrefix(name, "_") || strings.HasPrefix(name, "%") {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte(`{"errors":{"name":"First character must not be _ or %"}`)),
		}
	}

	maxTTLStr := req.Form.Get("maxTTL")
	var maxTTL int
	if maxTTLStr != "" {
		var err error
		maxTTL, err = strconv.Atoi(maxTTLStr)
		if err != nil {
			return &mock.HTTPResponse{
				StatusCode: 400,
				Body:       bytes.NewReader([]byte(`{"errors":{"maxTTL":"The value must be an integer"}`)),
			}
		}
	}
	manifest := bucket.CollectionManifest()

	uid, err := manifest.AddCollection(scope, name, uint32(maxTTL))
	switch err {
	case mock.ErrCollectionExists:
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body: bytes.NewReader([]byte(
				fmt.Sprintf(`{"errors":{"_": "Collection with name \"%s\" in scope \"%s\" already exists"}`,
					name,
					scope,
				))),
		}
	case mock.ErrScopeNotFound:
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body: bytes.NewReader([]byte(
				fmt.Sprintf(`{"errors":{"_": "Unknown error {error,{scope_not_found,\"%s\"}}"}`, scope))),
		}
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
	pathParts := pathparse.ParseParts(req.URL.Path, "/pools/default/buckets/*/scopes")
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
		return &mock.HTTPResponse{
			StatusCode: 404,
			Body:       bytes.NewReader([]byte("Requested resource not found.")),
		}
	}

	name := req.Form.Get("name")
	if name == "" {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte(`{"errors":{"name":"The value must be supplied"}`)),
		}
	}

	if strings.HasPrefix(name, "_") || strings.HasPrefix(name, "%") {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte(`{"errors":{"name":"First character must not be _ or %"}`)),
		}
	}

	manifest := bucket.CollectionManifest()

	uid, err := manifest.AddScope(name)
	switch err {
	case mock.ErrScopeExists:
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body: bytes.NewReader([]byte(
				fmt.Sprintf(`{"errors":{"_": "Scope with name \"%s\" already exists"}`, name))),
		}
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
	pathParts := pathparse.ParseParts(req.URL.Path, "/pools/default/buckets/*/scopes/*/collections/*")
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
	bucket := source.Node().Cluster().GetBucket(bucketName)
	if bucket == nil {
		return &mock.HTTPResponse{
			StatusCode: 404,
			Body:       bytes.NewReader([]byte("Requested resource not found.")),
		}
	}

	manifest := bucket.CollectionManifest()
	uid, err := manifest.DropCollection(scope, collection)
	switch err {
	case mock.ErrCollectionNotFound:
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body: bytes.NewReader([]byte(
				fmt.Sprintf(`{"errors":{"_": "Unknown error {error,{collection_not_found,\"%s\",\"%s\"}}"}`,
					scope,
					collection,
				))),
		}
	case mock.ErrScopeNotFound:
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body: bytes.NewReader([]byte(
				fmt.Sprintf(`{"errors":{"_": "Unknown error {error,{scope_not_found,\"%s\"}}"}`, scope))),
		}
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
	pathParts := pathparse.ParseParts(req.URL.Path, "/pools/default/buckets/*/scopes/*")
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
		return &mock.HTTPResponse{
			StatusCode: 404,
			Body:       bytes.NewReader([]byte("Requested resource not found.")),
		}
	}

	manifest := bucket.CollectionManifest()
	uid, err := manifest.DropScope(scope)
	switch err {
	case mock.ErrScopeNotFound:
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body: bytes.NewReader([]byte(
				fmt.Sprintf(`{"errors":{"_": "Unknown error {error,{scope_not_found,\"%s\"}}"}`, scope))),
		}
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
	pathParts := pathparse.ParseParts(req.URL.Path, "/pools/default/buckets/*/scopes")
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
		return &mock.HTTPResponse{
			StatusCode: 404,
			Body:       bytes.NewReader([]byte("Requested resource not found.")),
		}
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
