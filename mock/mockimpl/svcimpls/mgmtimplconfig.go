package svcimpls

import (
	"bytes"
	"errors"
	"io"
	"net/url"
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
			StatusCode: 404,
			Body:       bytes.NewReader([]byte("Requested resource not found")),
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
			StatusCode: 404,
			Body:       bytes.NewReader([]byte{}),
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
			StatusCode: 404,
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
			Body:       bytes.NewReader([]byte("Requested resource not found")),
		}
	}

	if !bucket.FlushEnabled() {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte(`{"_":"Flush is disabled for the bucket"}`)),
		}
	}
	bucket.Flush()

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte(``)),
	}
}

func (x *mgmtImpl) parseBucketSettings(values url.Values) (mock.NewBucketOptions, error) {

	flushEnabledStr := values.Get("flushEnabled")
	ramQuotaMBStr := values.Get("ramQuotaMB")
	replicaIndexStr := values.Get("replicaIndex")
	replicaNumberStr := values.Get("replicaNumber")
	compressionModeStr := values.Get("compressionMode")

	var replicaNumber int
	if replicaNumberStr != "" {
		var err error
		replicaNumber, err = strconv.Atoi(replicaNumberStr)
		if err != nil {
			return mock.NewBucketOptions{}, errors.New(`{"errors":{"replicaNumber":"The value must be an integer"}`)
		}
	}

	var flushEnabled bool
	if flushEnabledStr != "" {
		var err error
		flushEnabled, err = strconv.ParseBool(flushEnabledStr)
		if err != nil {
			return mock.NewBucketOptions{}, errors.New(`{"errors":{"flushEnabled":"flushenabled can only be 1 or 0"}`)
		}
	}

	if ramQuotaMBStr == "" {
		return mock.NewBucketOptions{}, errors.New(`{"errors":{"ramQuota":"The RAM Quota must be specified and must be a positive integer."}`)
	}
	ramQuotaMB, err := strconv.ParseUint(ramQuotaMBStr, 10, 0)
	if err != nil {
		return mock.NewBucketOptions{}, errors.New(`{"errors":{"ramQuota":"The RAM Quota must be specified and must be a positive integer."}`)
	}

	var replicaIndexEnabled bool
	if replicaIndexStr != "" {
		var err error
		replicaIndexEnabled, err = strconv.ParseBool(replicaIndexStr)
		if err != nil {
			return mock.NewBucketOptions{}, errors.New(`{"errors":{"replicaIndex":"replicaIndex can only be 1 or 0"}`)
		}
	}

	// TODO: validate compression mode
	if compressionModeStr == "" {
		compressionModeStr = "passive"
	}

	return mock.NewBucketOptions{
		NumReplicas:         uint(replicaNumber),
		FlushEnabled:        flushEnabled,
		RamQuota:            ramQuotaMB * 1024 * 1024,
		ReplicaIndexEnabled: replicaIndexEnabled,
		CompressionMode:     mock.CompressionMode(compressionModeStr),
	}, nil
}

func (x *mgmtImpl) handleAddBucketConfig(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	if !source.CheckAuthenticated(mockauth.PermissionClusterManage, "", "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}

	bucketType := req.Form.Get("bucketType")
	name := req.Form.Get("name")
	settings, err := x.parseBucketSettings(req.Form)
	if err != nil {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte(err.Error())),
		}
	}

	settings.Name = name
	settings.Type = mock.BucketTypeFromString(bucketType)

	_, err = source.Node().Cluster().AddBucket(settings)
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

func (x *mgmtImpl) handleUpdateBucketConfig(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	pathParts := pathparse.ParseParts(req.URL.Path, "/pools/default/buckets/*")
	bucketName := pathParts[0]

	if !source.CheckAuthenticated(mockauth.PermissionClusterManage, "", "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}

	bucket := source.Node().Cluster().GetBucket(bucketName)
	if bucket == nil {
		return &mock.HTTPResponse{
			StatusCode: 404,
			Body:       bytes.NewReader([]byte("Requested resource not found")),
		}
	}

	// The server just ignores bucket type if it's set.
	settings, err := x.parseBucketSettings(req.Form)
	if err != nil {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte(err.Error())),
		}
	}

	if err := bucket.Update(mock.UpdateBucketOptions{
		NumReplicas:         settings.NumReplicas,
		FlushEnabled:        settings.FlushEnabled,
		RamQuota:            settings.RamQuota,
		ReplicaIndexEnabled: settings.ReplicaIndexEnabled,
		CompressionMode:     settings.CompressionMode,
	}); err != nil {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte(`{"errors":{"": ""}`)),
		}
	}

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte(`{"":""}`)),
	}
}

func (x *mgmtImpl) handleDropBucketConfig(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	pathParts := pathparse.ParseParts(req.URL.Path, "/pools/default/buckets/*")
	bucketName := pathParts[0]

	if !source.CheckAuthenticated(mockauth.PermissionClusterManage, "", "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}

	bucket := source.Node().Cluster().GetBucket(bucketName)
	if bucket == nil {
		return &mock.HTTPResponse{
			StatusCode: 404,
			Body:       bytes.NewReader([]byte("Requested resource not found")),
		}
	}

	if err := source.Node().Cluster().DeleteBucket(bucketName); err != nil {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte(`{"errors":{"": ""}`)),
		}
	}

	return &mock.HTTPResponse{
		StatusCode: 200,
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
