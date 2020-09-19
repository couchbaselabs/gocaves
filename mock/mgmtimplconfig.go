package mock

import (
	"bytes"

	"github.com/couchbaselabs/gocaves/mock/servers"
)

type mgmtImplConfig struct {
}

func (x *mgmtImplConfig) Register(hooks *MgmtHookManager) {
	hooks.Expect().Method("GET").Path("/pools/default").Handler(x.handleGetPoolConfig)
	hooks.Expect().Method("GET").Path("/pools/default/buckets/*").Handler(x.handleGetBucketConfig)
}

func (x *mgmtImplConfig) handleGetPoolConfig(source *MgmtService, req *servers.HTTPRequest, next func() *servers.HTTPResponse) *servers.HTTPResponse {
	cluster := source.Node().Cluster()

	clusterConfig := cluster.GetConfig(source.Node())
	return &servers.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader(clusterConfig),
	}
}

func (x *mgmtImplConfig) handleGetBucketConfig(source *MgmtService, req *servers.HTTPRequest, next func() *servers.HTTPResponse) *servers.HTTPResponse {
	pathParts := ParsePathParts(req.URL.Path, "/pools/default/buckets/*")
	bucketName := pathParts[0]

	bucket := source.Node().Cluster().GetBucket(bucketName)
	if bucket == nil {
		return &servers.HTTPResponse{
			StatusCode: 401,
		}
	}

	bucketConfig := bucket.GetConfig(source.Node())
	return &servers.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader(bucketConfig),
	}
}
