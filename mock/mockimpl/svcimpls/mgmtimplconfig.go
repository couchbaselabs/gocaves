package svcimpls

import (
	"bytes"
	"github.com/couchbaselabs/gocaves/contrib/pathparse"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
	"io"
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

	configs := []byte{'['}
	buckets := source.Node().Cluster().GetAllBuckets()
	for _, bucket := range buckets {
		if !source.CheckAuthenticated(mockauth.PermissionSettings, bucket.Name(), "", "", req) {
			continue
		}

		configs = append(configs, GenBucketConfig(bucket, source.Node())...)
	}
	configs = append(configs, ']')

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader(configs),
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
