package svcimpls

import (
	"bytes"

	"github.com/couchbaselabs/gocaves/hooks"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/pathparse"
)

type mgmtImplConfig struct {
}

func (x *mgmtImplConfig) Register(hooks *hooks.MgmtHookManager) {
	hooks.Expect().Method("GET").Path("/pools/default").Handler(x.handleGetPoolConfig)
	hooks.Expect().Method("GET").Path("/pools/default/buckets/*").Handler(x.handleGetBucketConfig)
}

func (x *mgmtImplConfig) handleGetPoolConfig(source mock.MgmtService, req *mock.HTTPRequest, next func() *mock.HTTPResponse) *mock.HTTPResponse {
	cluster := source.Node().Cluster()

	clusterConfig := GenClusterConfig(cluster, source.Node())
	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader(clusterConfig),
	}
}

func (x *mgmtImplConfig) handleGetBucketConfig(source mock.MgmtService, req *mock.HTTPRequest, next func() *mock.HTTPResponse) *mock.HTTPResponse {
	pathParts := pathparse.ParseParts(req.URL.Path, "/pools/default/buckets/*")
	bucketName := pathParts[0]

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
