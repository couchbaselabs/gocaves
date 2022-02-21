package svcimpls

import (
	"bytes"
	"io"
	"strconv"

	"github.com/couchbaselabs/gocaves/contrib/pathparse"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
)

func (x *mgmtImpl) handleGetPoolConfig(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
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

func (x *mgmtImpl) handleGetBucketConfig(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
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

func (x *mgmtImpl) handleGetTerseBucketConfig(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	pathParts := pathparse.ParseParts(req.URL.Path, "/pools/default/b/*")
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
	pathParts := pathparse.ParseParts(req.URL.Path, "/pools/default/bs/*")
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
			Body:       bytes.NewReader([]byte{}),
		}
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
				writer.Close()
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
	if !source.CheckAuthenticated(mockauth.PermissionSettings, "", "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}

	buckets := source.Node().Cluster().GetAllBuckets()
	configs := make([][]byte, len(buckets))
	for i, bucket := range buckets {
		if !source.CheckAuthenticated(mockauth.PermissionSettings, bucket.Name(), "", "", req) {
			continue
		}

		configs[i] = GenBucketConfig(bucket, source.Node())
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
	pathParts := pathparse.ParseParts(req.URL.Path, "/pools/default/buckets/*/controller/doFlush")
	bucketName := pathParts[0]

	bucket := source.Node().Cluster().GetBucket(bucketName)
	if bucket == nil {
		return &mock.HTTPResponse{
			StatusCode: 404,
			Body:       bytes.NewReader([]byte("Requested resource not found")),
		}
	}
	bucket.Flush()

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte(``)),
	}
}

func (x *mgmtImpl) handleAddBucketConfig(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	bucketType := req.Form.Get("bucketType")
	flushEnabled := req.Form.Get("flushEnabled")
	name := req.Form.Get("name")
	ramQuotaMB := req.Form.Get("ramQuotaMB")
	replicaIndex := req.Form.Get("replicaIndex")
	replicaNumberStr := req.Form.Get("replicaNumber")

	var replicaNumber int
	if replicaNumberStr != "" {
		var err error
		replicaNumber, err = strconv.Atoi(replicaNumberStr)
		if err != nil {
			return &mock.HTTPResponse{
				StatusCode: 400,
				Body:       bytes.NewReader([]byte(`{"errors":{"replicaNumber":"The value must be an integer"}`)),
			}
		}
	}

	// TODO: Use the below at some point
	_ = flushEnabled
	_ = ramQuotaMB
	_ = replicaIndex

	_, err := source.Node().Cluster().AddBucket(mock.NewBucketOptions{
		Name:        name,
		Type:        mock.BucketTypeFromString(bucketType),
		NumReplicas: uint(replicaNumber),
	})
	if err != nil {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte(`{"errors":{"": ""}`)),
		}
	}

	return &mock.HTTPResponse{
		StatusCode: 202,
		Body:       bytes.NewReader([]byte(`{"":""}`)),
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
	if !source.CheckAuthenticated(mockauth.PermissionSettings, "", "", "", req) {
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
