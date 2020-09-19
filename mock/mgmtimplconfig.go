package mock

import (
	"log"

	"github.com/couchbaselabs/gocaves/mock/servers"
)

type mgmtImplConfig struct {
}

func (x *mgmtImplConfig) Register(hooks *MgmtHookManager) {
	hooks.Expect().Method("GET").Path("/buckets/*").Handler(x.handleGetConfig)
}

func (x *mgmtImplConfig) handleGetConfig(source *MgmtService, req *servers.HTTPRequest, next func() *servers.HTTPResponse) *servers.HTTPResponse {
	pathParts := ParsePathParts(req.URL.Path, "/buckets/*")
	bucketName := pathParts[0]

	bucket := source.Node().Cluster().GetBucket(bucketName)
	log.Printf("found bucket: %+v", bucket)

	log.Printf("got mgmt get config request for bucket `%s`", bucketName)
	return next()
}
