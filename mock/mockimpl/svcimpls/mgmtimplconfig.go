package svcimpls

import (
	"bytes"
	"github.com/couchbaselabs/gocaves/mock/mockauth"

	"github.com/couchbaselabs/gocaves/contrib/pathparse"
	"github.com/couchbaselabs/gocaves/mock"
)

type mgmtImplConfig struct {
}

func (x *mgmtImplConfig) Register(h *hookHelper) {
	h.RegisterMgmtHandler("GET", "/pools/default", x.handleGetPoolConfig)
	h.RegisterMgmtHandler("GET", "/pools/default/buckets/*", x.handleGetBucketConfig)
}

func (x *mgmtImplConfig) handleGetPoolConfig(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	if !source.CheckAuthenticated(mockauth.PermissionSettings, "", "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}
	cluster := source.Node().Cluster()

	clusterConfig := GenClusterConfig(cluster, source.Node())
	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader(clusterConfig),
	}
}

func (x *mgmtImplConfig) handleGetBucketConfig(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	pathParts := pathparse.ParseParts(req.URL.Path, "/pools/default/buckets/*")
	bucketName := pathParts[0]
	if !source.CheckAuthenticated(mockauth.PermissionSettings, bucketName, "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}

	bucket := source.Node().Cluster().GetBucket(bucketName)
	if bucket == nil {
		return &mock.HTTPResponse{
			StatusCode: 401,
		}
	}

	bucketConfig := GenBucketConfig(bucket, source.Node())
	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader(bucketConfig),
	}
}