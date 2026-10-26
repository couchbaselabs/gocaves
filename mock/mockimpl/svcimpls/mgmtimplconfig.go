package svcimpls

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/couchbaselabs/gocaves/mock"
)

func (x *mgmtImpl) handleGetPoolConfig(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	if !source.CheckAuthenticated(req) {
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

func (x *mgmtImpl) handleGetBucketConfig(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	pathParts := mgmtPathParts(req, "/pools/default/buckets/*")
	bucketName := pathParts[0]
	if !source.CheckAuthenticated(req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}

	bucket := source.Node().Cluster().GetBucket(bucketName)
	if bucket == nil {
		return bucketNotFoundResponse()
	}

	bucketConfig := GenBucketConfig(bucket, source.Node())
	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader(bucketConfig),
	}
}

func (x *mgmtImpl) handleGetTerseBucketConfig(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	pathParts := mgmtPathParts(req, "/pools/default/b/*")
	bucketName := pathParts[0]
	if !source.CheckAuthenticated(req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}

	bucket := source.Node().Cluster().GetBucket(bucketName)
	if bucket == nil {
		return bucketNotFoundResponse()
	}

	bucketConfig := GenTerseBucketConfig(bucket, source.Node())
	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader(bucketConfig),
	}
}

type configHandler struct {
	configChan chan uint
}

func (c *configHandler) OnNewConfig(cfg uint) {
	c.configChan <- cfg
}

func (x *mgmtImpl) handleGetTerseBucketStreamingConfig(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	pathParts := mgmtPathParts(req, "/pools/default/bs/*")
	bucketName := pathParts[0]
	if !source.CheckAuthenticated(req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}

	bucket := source.Node().Cluster().GetBucket(bucketName)
	if bucket == nil {
		return bucketNotFoundResponse()
	}

	bucketConfig := GenTerseBucketConfig(bucket, source.Node())

	reader, writer := io.Pipe()
	watcher := &configHandler{}
	source.Node().Cluster().AddConfigWatcher(watcher)
	req.Header.Set("Transfer-Encoding", "chunked")

	go func() {
		for {
			_, err := writer.Write(bucketConfig)
			if err != nil {
				return
			}
			_, err = writer.Write([]byte("\n\n\n\n"))
			if err != nil {
				return
			}
			req.Flusher.Flush()

			select {
			case <-req.Context.Done():
				source.Node().Cluster().RemoveConfigWatcher(watcher)
				if err := writer.Close(); err != nil {
					log.Printf("failed to close streaming config writer: %s", err)
				}
				return
			case <-watcher.configChan:
			}
		}
	}()

	return &mock.HTTPResponse{
		Streaming:  true,
		Body:       reader,
		StatusCode: 200,
	}
}

func (x *mgmtImpl) handleGetAllBucketConfigs(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	if !source.CheckAuthenticated(req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}

	var configs [][]byte
	for _, bucket := range readableBuckets(source.Node().Cluster(), req) {
		configs = append(configs, GenBucketConfig(bucket, source.Node()))
	}

	configArr := []byte("[")
	configArr = append(configArr, bytes.Join(configs, []byte(","))...)
	configArr = append(configArr, ']')

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader(configArr),
	}
}

func (x *mgmtImpl) handleBucketFlush(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	pathParts := mgmtPathParts(req, "/pools/default/buckets/*/controller/doFlush")
	bucketName := pathParts[0]

	if !source.CheckAuthenticated(req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}

	bucket := source.Node().Cluster().GetBucket(bucketName)
	if bucket == nil {
		return bucketNotFoundResponse()
	}

	if !bucket.FlushEnabled() {
		return jsonResponse(400, `{"_":"Flush is disabled for the bucket"}`)
	}
	bucket.Flush()

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte(``)),
	}
}

func (x *mgmtImpl) handleAddBucketConfig(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	if !source.CheckAuthenticated(req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}

	// The body is read before the bucket changes are serialised.
	cluster := source.Node().Cluster()
	params := parsePost(req)
	var resp *mock.HTTPResponse
	cluster.ChangeBuckets(func() {
		resp = addBucket(cluster, req, params)
	})
	return resp
}

func addBucket(cluster mock.Cluster, req *mock.HTTPRequest, params []formParam) *mock.HTTPResponse {
	if len(cluster.GetAllBuckets()) >= maxBucketsCount {
		// ns_bucket:get_max_buckets
		return jsonResponse(400, fmt.Sprintf(`{"_":"Cannot create more than %d buckets"}`, maxBucketsCount))
	}

	res := parseBucketParams(params, nil, cluster)
	if resp := bucketParamsResponse(req, res, nil, cluster); resp != nil {
		return resp
	}

	name, _ := paramValue(params, "name")
	_, err := cluster.AddBucket(mock.NewBucketOptions{
		Name:                name,
		Type:                res.bucketType,
		NumReplicas:         res.numReplicas,
		FlushEnabled:        res.flushEnabled,
		RamQuota:            res.ramQuota,
		ReplicaIndexEnabled: res.replicaIndexEnabled,
		CompressionMode:     res.compressionMode,
		MaxTTL:              res.maxTTL,
		Settings:            res.settings,
	})
	switch {
	case err == mock.ErrBucketExists:
		// do_bucket_create answers a create that lost a race with another.
		return jsonResponse(400, `{"name":"Bucket with given name already exists"}`)
	case err != nil:
		return serverErrorResponse()
	}

	return &mock.HTTPResponse{
		StatusCode: 202,
		Header:     http.Header{"Location": {"/pools/default/buckets/" + mochiwebQuotePlus(name)}},
		Body:       bytes.NewReader([]byte{}),
	}
}

// bucketNotFoundResponse answers a request for a missing bucket as
// menelaus_util:reply_not_found does.
func bucketNotFoundResponse() *mock.HTTPResponse {
	return &mock.HTTPResponse{
		StatusCode: 404,
		Header:     http.Header{"Content-Type": {"text/plain"}},
		Body:       bytes.NewReader([]byte("Requested resource not found.\r\n")),
	}
}

func jsonResponse(statusCode int, body string) *mock.HTTPResponse {
	return &mock.HTTPResponse{
		StatusCode: statusCode,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       bytes.NewReader([]byte(body)),
	}
}

func (x *mgmtImpl) handleUpdateBucketConfig(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	pathParts := mgmtPathParts(req, "/pools/default/buckets/*")
	bucketName := pathParts[0]

	if !source.CheckAuthenticated(req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}

	// The body is read before the bucket changes are serialised, and the
	// bucket looked up again then.
	cluster := source.Node().Cluster()
	if cluster.GetBucket(bucketName) == nil {
		return bucketNotFoundResponse()
	}
	params := parsePost(req)
	var resp *mock.HTTPResponse
	cluster.ChangeBuckets(func() {
		resp = updateBucket(cluster, bucketName, req, params)
	})
	return resp
}

func updateBucket(cluster mock.Cluster, bucketName string, req *mock.HTTPRequest, params []formParam) *mock.HTTPResponse {
	bucket := cluster.GetBucket(bucketName)
	if bucket == nil {
		return bucketNotFoundResponse()
	}

	res := parseBucketParams(params, bucket, cluster)
	if resp := bucketParamsResponse(req, res, bucket, cluster); resp != nil {
		return resp
	}

	// ns_server stores the new settings in the bucket config, which gets a
	// new revision.
	if err := cluster.UpdateBucket(bucketName, mock.UpdateBucketOptions{
		NumReplicas:         &res.numReplicas,
		FlushEnabled:        &res.flushEnabled,
		RamQuota:            &res.ramQuota,
		ReplicaIndexEnabled: &res.replicaIndexEnabled,
		CompressionMode:     &res.compressionMode,
		MaxTTL:              &res.maxTTL,
		Settings:            &res.settings,
	}); err != nil {
		// The bucket was dropped since it was looked up.
		return bucketNotFoundResponse()
	}

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte{}),
	}
}

func (x *mgmtImpl) handleDropBucketConfig(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	pathParts := mgmtPathParts(req, "/pools/default/buckets/*")
	bucketName := pathParts[0]

	if !source.CheckAuthenticated(req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}

	// A collections update holds the lock while it changes the bucket.
	x.collectionsLock.Lock()
	defer x.collectionsLock.Unlock()
	cluster := source.Node().Cluster()
	var err error
	cluster.ChangeBuckets(func() {
		err = cluster.DeleteBucket(bucketName)
	})
	if err != nil {
		return bucketNotFoundResponse()
	}

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte{}),
	}
}

func (x *mgmtImpl) handleGetNodeServices(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	clusterConfig := GenTerseClusterConfig(source.Node().Cluster(), source.Node())
	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader(clusterConfig),
	}
}

func (x *mgmtImpl) handleGetAllPoolsConfig(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	if !source.CheckAuthenticated(req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}
	cluster := source.Node().Cluster()

	clusterConfig := GenPoolsConfig(cluster)
	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader(clusterConfig),
	}
}
