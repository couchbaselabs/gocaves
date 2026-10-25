package mockimpl

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
)

// newBucketsTestCluster returns a cluster of one node with a memory quota
// of 1 GiB and the buckets of the Couchbase Server 7.0.5 cluster the
// answers below come from: "default", a couchbase bucket of 256 MiB with
// one replica, "eph", an ephemeral bucket of 100 MiB, and "mc", a
// memcached bucket of 100 MiB.
func newBucketsTestCluster(t *testing.T) mock.Cluster {
	cluster, err := NewCluster(mock.NewClusterOptions{InitialNode: mock.NewNodeOptions{}})
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })
	if _, err := cluster.AddBucket(mock.NewBucketOptions{Name: "default", Type: mock.BucketTypeCouchbase, NumReplicas: 1,
		RamQuota: 256 << 20, ReplicaIndexEnabled: true, CompressionMode: mock.CompressionModePassive}); err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	if _, err := cluster.AddBucket(mock.NewBucketOptions{Name: "eph", Type: mock.BucketTypeEphemeral, NumReplicas: 1,
		RamQuota: 100 << 20}); err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	if _, err := cluster.AddBucket(mock.NewBucketOptions{Name: "mc", Type: mock.BucketTypeMemcached, RamQuota: 100 << 20,
		CompressionMode: mock.CompressionModeOff}); err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	if err := cluster.Users().UpsertUser(mockauth.UpsertUserOptions{Username: "Administrator", Password: "password",
		Roles: []string{"admin"}}); err != nil {
		t.Fatalf("failed to add user: %v", err)
	}
	return cluster
}

// bucketsRequest sends a request to path under /pools/default/buckets, or
// to a path from /pools on, on the management port of the first node of
// cluster.
func bucketsRequest(t *testing.T, cluster mock.Cluster, method, path, query, body string) *http.Response {
	t.Helper()
	if !strings.HasPrefix(path, "/pools/") {
		path = "/pools/default/buckets" + path
	}
	reqURL := fmt.Sprintf("http://127.0.0.1:%d%s", cluster.Nodes()[0].MgmtService().ListenPort(), path)
	if query != "" {
		reqURL += "?" + query
	}
	req, err := http.NewRequest(method, reqURL, strings.NewReader(body))
	if err != nil {
		t.Fatalf("failed to build request: %v", err)
	}
	req.SetBasicAuth("Administrator", "password")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	return resp
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	return string(body)
}

// TestBucketParamsValidation checks bucket create and update against the
// answers of Couchbase Server 7.0.5, from menelaus_web_buckets
// parse_bucket_params: the errors, in order, and the RAM summary. The RAM
// a bucket uses and the disk summary come from the mock's model of usage.
func TestBucketParamsValidation(t *testing.T) {
	cluster := newBucketsTestCluster(t)

	tests := []struct {
		name   string
		path   string
		query  string
		body   string
		status int
		errors string
		// ram is the RAM summary but thisUsed: total, otherBuckets,
		// nodesCount, perNodeMegs, thisAlloc and free.
		ram [6]int64
	}{
		{"create: name of a bucket", "", "just_validate=1",
			"name=default",
			400, "{\"name\":\"Bucket with given name already exists\"}",
			[6]int64{1073741824, 478150656, 1, 256, 268435456, 327155712}},
		{"create: name of an ephemeral bucket", "", "just_validate=1",
			"name=eph&evictionPolicy=fullEviction",
			400, "{\"name\":\"Bucket with given name already exists\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: name of a bucket with other errors", "", "just_validate=1",
			"name=default&ramQuotaMB=10&evictionPolicy=bogus&maxTTL=-1",
			400, "{\"name\":\"Bucket with given name already exists\"," +
				"\"evictionPolicy\":\"Eviction policy must be either 'valueOnly' or 'fullEviction' for couchbase buckets\"," +
				"\"ramQuota\":\"RAM quota cannot be less than 100 MiB\",\"maxTTL\":\"Max TTL must be an integer between 0 and 2147483647\"}",
			[6]int64{1073741824, 478150656, 1, 10, 10485760, 585105408}},
		{"create: name of a bucket in other case", "", "just_validate=1",
			"name=Default",
			400, "{\"name\":\"Bucket with given name already exists\",\"ramQuota\":\"The RAM Quota must be specified and must be a positive integer.\"}",
			[6]int64{1073741824, 478150656, 1, 0, 0, 595591168}},
		{"create: maxTTL -1", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&maxTTL=-1",
			400, "{\"maxTTL\":\"Max TTL must be an integer between 0 and 2147483647\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: maxTTL above range", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&maxTTL=2147483648",
			400, "{\"maxTTL\":\"Max TTL must be an integer between 0 and 2147483647\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: maxTTL not a number", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&maxTTL=abc",
			400, "{\"maxTTL\":\"Max TTL must be an integer between 0 and 2147483647\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: maxTTL empty", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&maxTTL=",
			400, "{\"maxTTL\":\"Max TTL must be an integer between 0 and 2147483647\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: maxTTL with spaces", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&maxTTL=%207%20",
			200, "{}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: maxTTL ignored for memcached", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&bucketType=memcached&maxTTL=-1",
			200, "{}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: unknown bucket type", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&bucketType=bogus&maxTTL=-1",
			400, "{\"bucketType\":\"invalid bucket type\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: name missing", "", "just_validate=1",
			"ramQuotaMB=100",
			400, "{\"name\":\"Bucket name needs to be specified\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: name empty", "", "just_validate=1",
			"name=&ramQuotaMB=100",
			400, "{\"name\":\"Bucket name cannot be empty\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: name taken in other case", "", "just_validate=1",
			"name=DEFAULT&ramQuotaMB=100",
			400, "{\"name\":\"Bucket with given name already exists\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: name starting with a dot", "", "just_validate=1",
			"name=.x&ramQuotaMB=100",
			400, "{\"name\":\"Bucket name cannot start with dot.\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: name with a space", "", "just_validate=1",
			"name=a%20b&ramQuotaMB=100",
			400, "{\"name\":\"Bucket name can only contain characters in range A-Z, a-z, 0-9 as well as underscore, period, dash & percent. Consult the documentation.\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: reserved name", "", "just_validate=1",
			"name=_users.couch.x&ramQuotaMB=100",
			400, "{\"name\":\"This name is reserved for the internal use.\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: name too long", "", "just_validate=1",
			"name=" + strings.Repeat("a", 101) + "&ramQuotaMB=100",
			400, "{\"name\":\"Bucket name cannot exceed 100 characters\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: RAM quota missing", "", "just_validate=1",
			"name=nb",
			400, "{\"ramQuota\":\"The RAM Quota must be specified and must be a positive integer.\"}",
			[6]int64{1073741824, 478150656, 1, 0, 0, 595591168}},
		{"create: RAM quota negative", "", "just_validate=1",
			"name=nb&ramQuotaMB=-1",
			400, "{\"ramQuota\":\"The RAM Quota cannot be negative.\"}",
			[6]int64{1073741824, 478150656, 1, 0, 0, 595591168}},
		{"create: RAM quota below minimum", "", "just_validate=1",
			"name=nb&ramQuotaMB=99",
			400, "{\"ramQuota\":\"RAM quota cannot be less than 100 MiB\"}",
			[6]int64{1073741824, 478150656, 1, 99, 103809024, 491782144}},
		{"create: RAM quota beyond the cluster quota", "", "just_validate=1",
			"name=nb&ramQuotaMB=600",
			400, "{\"ramQuota\":\"RAM quota specified is too large to be provisioned into this cluster.\"}",
			[6]int64{1073741824, 478150656, 1, 600, 629145600, -33554432}},
		{"create: other buckets RAM quota", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&otherBucketsRamQuotaMB=500",
			400, "{\"ramQuota\":\"RAM quota specified is too large to be provisioned into this cluster.\"}",
			[6]int64{1073741824, 1002438656, 1, 100, 104857600, -33554432}},
		{"create: other buckets RAM quota invalid", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&otherBucketsRamQuotaMB=x",
			400, "{\"otherBucketsRamQuotaMB\":\"The other buckets RAM Quota must be a positive integer.\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: proxy port", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&proxyPort=11211",
			400, "{\"proxyPort\":\"Setting proxy port is not allowed on this version of the cluster\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: proxy port none", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&proxyPort=none",
			200, "{}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: memcached RAM quota below minimum", "", "just_validate=1",
			"name=nb&bucketType=memcached&ramQuotaMB=63",
			400, "{\"ramQuota\":\"RAM quota cannot be less than 64 MiB\"}",
			[6]int64{1073741824, 478150656, 1, 63, 66060288, 529530880}},
		{"create: replicaNumber for memcached", "", "just_validate=1",
			"name=nb&bucketType=memcached&ramQuotaMB=100&replicaNumber=1",
			400, "{\"replicaNumber\":\"replicaNumber is not valid for memcached buckets\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: replicaIndex for ephemeral", "", "just_validate=1",
			"name=nb&bucketType=ephemeral&ramQuotaMB=100&replicaIndex=1",
			400, "{\"replicaIndex\":\"replicaIndex not supported for ephemeral buckets\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: replica number too large", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&replicaNumber=4",
			400, "{\"replicaNumber\":\"Replica number larger than 3 is not supported.\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: replica warning", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&replicaNumber=1",
			400, "{\"replicaNumber\":\"Warning: you do not have enough data servers to support this number of replicas.\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: replica warning ignored", "", "just_validate=1&ignore_warnings=1",
			"name=nb&ramQuotaMB=100&replicaNumber=1",
			200, "{}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: threads too many", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&threadsNumber=9",
			400, "{\"threadsNumber\":\"The number of threads can't be greater than 8\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: threads too few", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&threadsNumber=1",
			400, "{\"threadsNumber\":\"The number of threads can't be less than 2\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: threads not a number", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&threadsNumber=x",
			400, "{\"threadsNumber\":\"The number of threads must be an integer between 2 and 8\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: couchbase eviction policy", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&evictionPolicy=noEviction",
			400, "{\"evictionPolicy\":\"Eviction policy must be either 'valueOnly' or 'fullEviction' for couchbase buckets\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: ephemeral eviction policy", "", "just_validate=1",
			"name=nb&bucketType=ephemeral&ramQuotaMB=100&evictionPolicy=fullEviction",
			400, "{\"evictionPolicy\":\"Eviction policy must be either 'noEviction' or 'nruEviction' for ephemeral buckets\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: magma", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&storageBackend=magma",
			400, "{\"storageBackend\":\"Magma is supported only in developer preview mode\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: unknown storage backend", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&storageBackend=x",
			400, "{\"storage_mode\":\"storage backend must be couchstore or magma\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: durability level", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&durabilityMinLevel=x",
			400, "{\"durability_min_level\":\"Durability minimum level must be one of 'none', 'majority', 'majorityAndPersistActive', or 'persistToMajority'\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: ephemeral durability level", "", "just_validate=1",
			"name=nb&bucketType=ephemeral&ramQuotaMB=100&durabilityMinLevel=persistToMajority",
			400, "{\"durability_min_level\":\"Durability minimum level must be either 'none' or 'majority' for ephemeral buckets\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: durability with 3 replicas", "", "just_validate=1&ignore_warnings=1",
			"name=nb&ramQuotaMB=100&replicaNumber=3&durabilityMinLevel=majority",
			400, "{\"durability_min_level\":\"Durability minimum level cannot be specified with 3 replicas\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: invalid durability with 3 replicas", "", "just_validate=1&ignore_warnings=1",
			"name=nb&ramQuotaMB=100&replicaNumber=3&durabilityMinLevel=x",
			400, "{\"durability_min_level\":\"Durability minimum level must be one of 'none', 'majority', 'majorityAndPersistActive', or 'persistToMajority'\",\"durability_min_level\":\"Durability minimum level cannot be specified with 3 replicas\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: pitr", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&pitrEnabled=true&pitrGranularity=10&pitrMaxHistoryAge=10",
			400, "{\"pitrEnabled\":\"Point in time recovery is only supported in developer preview mode\",\"pitrGranularity\":\"Point in time recovery is only supported in developer preview mode\",\"pitrMaxHistoryAge\":\"Point in time recovery is only supported in developer preview mode\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: fragmentation percentage", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&fragmentationPercentage=50",
			400, "{\"fragmentationPercentage\":\"Fragmentation percentage is only used with Magma\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: fragmentation percentage with magma", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&storageBackend=magma&fragmentationPercentage=5",
			400, "{\"storageBackend\":\"Magma is supported only in developer preview mode\",\"fragmentationPercentage\":\"Fragmentation percentage must be between 10 and 100, inclusive\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: conflict resolution", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&conflictResolutionType=x",
			400, "{\"conflictResolutionType\":\"Conflict resolution type must be 'seqno' or 'lww' or 'custom'\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: custom conflict resolution", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&conflictResolutionType=custom",
			400, "{\"conflictResolutionType\":\"Conflict resolution type 'custom' is supported only with developer preview enabled\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: drift thresholds", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&conflictResolutionType=lww&driftAheadThresholdMs=99&driftBehindThresholdMs=x",
			400, "{\"driftAheadThresholdMs\":\"The drift ahead threshold can't be less than 100ms\",\"driftBehindThresholdMs\":\"The drift behind threshold must be an integer not less than 100ms\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: drift thresholds without lww", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&driftAheadThresholdMs=99",
			200, "{}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: autoCompactionDefined invalid", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&autoCompactionDefined=x",
			400, "{\"autoCompactionDefined\":\"autoCompactionDefined is invalid\",\"autoCompactionDefined\":\"autoCompactionDefined is invalid\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: auto-compaction settings", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&autoCompactionDefined=true&databaseFragmentationThreshold%5Bsize%5D=0&parallelDBAndViewCompaction=x&allowedTimePeriod%5BfromHour%5D=24&viewFragmentationThreshold%5Bpercentage%5D=101&databaseFragmentationThreshold=1&viewFragmentationThreshold=2",
			400, "{\"viewFragmentationThreshold[percentage]\":\"view fragmentation is too large. Allowed range is 2 - 100\",\"parallelDBAndViewCompaction\":\"parallelDBAndViewCompaction is invalid\",\"allowedTimePeriod[fromHour]\":\"from hour is too large. Allowed range is 0 - 23\",\"databaseFragmentationThreshold[size]\":\"database fragmentation size is too small. Allowed range is 1 - infinity\",\"_\":\"Got unsupported fields: databaseFragmentationThreshold and viewFragmentationThreshold\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: parallel compaction missing", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&autoCompactionDefined=true",
			400, "{\"parallelDBAndViewCompaction\":\"parallelDBAndViewCompaction is missing\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: purge interval", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&autoCompactionDefined=true&parallelDBAndViewCompaction=true&purgeInterval=0.01",
			400, "{\"purgeInterval\":\"metadata purge interval is too small. Allowed range is 0.04 - 60\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: purge interval too large", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&autoCompactionDefined=true&parallelDBAndViewCompaction=true&purgeInterval=61",
			400, "{\"purgeInterval\":\"metadata purge interval is too large. Allowed range is 0.04 - 60\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: purge interval not a number", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&autoCompactionDefined=true&parallelDBAndViewCompaction=true&purgeInterval=2.",
			400, "{\"purgeInterval\":\"metadata purge interval must be a number\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: ephemeral purge interval", "", "just_validate=1",
			"name=nb&bucketType=ephemeral&ramQuotaMB=100&purgeInterval=0.0001",
			400, "{\"purgeInterval\":\"metadata purge interval is too small. Allowed range is 0.0007 - 60\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: ephemeral auto-compaction", "", "just_validate=1",
			"name=nb&bucketType=ephemeral&ramQuotaMB=100&autoCompactionDefined=false",
			400, "{\"autoCompactionDefined\":\"autoCompactionDefined must not be set for ephemeral buckets\"}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"create: errors in order", "", "",
			"compressionMode=bad&maxTTL=-1&replicaIndex=5&replicaNumber=x&ramQuotaMB=abc&name=&flushEnabled=2&threadsNumber=x&evictionPolicy=x&durabilityMinLevel=x&pitrEnabled=x&storageBackend=x&conflictResolutionType=x&autoCompactionDefined=x",
			400, "{\"flushEnabled\":\"flushEnabled can only be 1 or 0\",\"name\":\"Bucket name cannot be empty\",\"ramQuota\":\"The RAM Quota must be specified and must be a positive integer.\",\"autoCompactionDefined\":\"autoCompactionDefined is invalid\",\"conflictResolutionType\":\"Conflict resolution type must be 'seqno' or 'lww' or 'custom'\",\"replicaNumber\":\"The replica number must be specified and must be a non-negative integer.\",\"replicaIndex\":\"replicaIndex can only be 1 or 0\",\"threadsNumber\":\"The number of threads must be an integer between 2 and 8\",\"evictionPolicy\":\"Eviction policy must be either 'valueOnly' or 'fullEviction' for couchbase buckets\",\"storage_mode\":\"storage backend must be couchstore or magma\",\"durability_min_level\":\"Durability minimum level must be one of 'none', 'majority', 'majorityAndPersistActive', or 'persistToMajority'\",\"pitrEnabled\":\"Point in time recovery is only supported in developer preview mode\",\"maxTTL\":\"Max TTL must be an integer between 0 and 2147483647\",\"compressionMode\":\"compressionMode can be set to 'off', 'passive' or 'active'\",\"autoCompactionDefined\":\"autoCompactionDefined is invalid\"}",
			[6]int64{1073741824, 478150656, 1, 0, 0, 595591168}},
		{"create: valid", "", "just_validate=1",
			"name=nb&ramQuotaMB=100&replicaNumber=0&maxTTL=30",
			200, "{}",
			[6]int64{1073741824, 478150656, 1, 100, 104857600, 490733568}},
		{"update: maxTTL -1", "/default", "just_validate=1",
			"maxTTL=-1",
			400, "{\"maxTTL\":\"Max TTL must be an integer between 0 and 2147483647\"}",
			[6]int64{1073741824, 209715200, 1, 256, 268435456, 595591168}},
		{"update: maxTTL valid", "/default", "just_validate=1",
			"maxTTL=5",
			202, "{}",
			[6]int64{1073741824, 209715200, 1, 256, 268435456, 595591168}},
		{"update: replica warnings", "/default", "just_validate=1",
			"replicaNumber=2",
			400, "{\"replicaNumber\":\"Warning: you do not have enough data servers to support this number of replicas and changing replica number may require rebalance.\"}",
			[6]int64{1073741824, 209715200, 1, 256, 268435456, 595591168}},
		{"update: RAM quota below usage", "/default", "just_validate=1",
			"ramQuotaMB=abc",
			400, "{\"ramQuota\":\"RAM quota cannot be set below current usage.\",\"ramQuota\":\"The RAM Quota must be specified and must be a positive integer.\"}",
			[6]int64{1073741824, 209715200, 1, 0, 0, 864026624}},
		{"update: RAM quota beyond the cluster quota", "/default", "just_validate=1",
			"ramQuotaMB=900",
			400, "{\"ramQuota\":\"RAM quota specified is too large to be provisioned into this cluster.\"}",
			[6]int64{1073741824, 209715200, 1, 900, 943718400, -79691776}},
		{"update: conflict resolution", "/default", "just_validate=1",
			"conflictResolutionType=seqno",
			400, "{\"conflictResolutionType\":\"Conflict resolution type not allowed in update bucket\"}",
			[6]int64{1073741824, 209715200, 1, 256, 268435456, 595591168}},
		{"update: eviction policy", "/default", "just_validate=1",
			"evictionPolicy=fullEviction",
			202, "{}",
			[6]int64{1073741824, 209715200, 1, 256, 268435456, 595591168}},
		{"update: durability with 3 replicas", "/default", "just_validate=1&ignore_warnings=1",
			"replicaNumber=3&durabilityMinLevel=majority",
			400, "{\"durability_min_level\":\"Durability minimum level cannot be specified with 3 replicas\"}",
			[6]int64{1073741824, 209715200, 1, 256, 268435456, 595591168}},
		{"update: ephemeral eviction policy", "/eph", "just_validate=1",
			"evictionPolicy=nruEviction",
			400, "{\"evictionPolicy\":\"Eviction policy cannot be updated for ephemeral buckets\"}",
			[6]int64{1073741824, 373293056, 1, 100, 104857600, 595591168}},
		{"update: ephemeral auto-compaction", "/eph", "just_validate=1",
			"autoCompactionDefined=true",
			400, "{\"autoCompactionDefined\":\"autoCompactionDefined must not be set for ephemeral buckets\",\"parallelDBAndViewCompaction\":\"parallelDBAndViewCompaction is missing\"}",
			[6]int64{1073741824, 373293056, 1, 100, 104857600, 595591168}},
		{"update: memcached maxTTL ignored", "/mc", "just_validate=1",
			"maxTTL=-1",
			202, "{}",
			[6]int64{1073741824, 373293056, 1, 100, 104857600, 595591168}},
		{"update: memcached RAM quota change", "/mc", "just_validate=1",
			"ramQuotaMB=200",
			400, "{\"ramQuota\":\"cannot change quota of memcached buckets\"}",
			[6]int64{1073741824, 373293056, 1, 200, 209715200, 490733568}},
		{"update: memcached RAM quota below minimum", "/mc", "just_validate=1",
			"ramQuotaMB=50",
			400, "{\"ramQuota\":\"RAM quota cannot be less than 64 MiB\"}",
			[6]int64{1073741824, 373293056, 1, 50, 52428800, 648019968}},
	}
	for _, tt := range tests {
		resp := bucketsRequest(t, cluster, http.MethodPost, tt.path, tt.query, tt.body)
		body := readBody(t, resp)
		if resp.StatusCode != tt.status || resp.Header.Get("Content-Type") != "application/json" {
			t.Errorf("%s: expected %d application/json, got %d %s %s", tt.name, tt.status, resp.StatusCode,
				resp.Header.Get("Content-Type"), body)
			continue
		}
		summaries := strings.Index(body, `,"summaries":`)
		if !strings.HasPrefix(body, `{"errors":`) || summaries < 0 {
			t.Errorf("%s: unexpected body %s", tt.name, body)
			continue
		}
		if errs := body[len(`{"errors":`):summaries]; errs != tt.errors {
			t.Errorf("%s:\n got errors %s\nwant errors %s", tt.name, errs, tt.errors)
		}
		var parsed struct {
			Summaries struct {
				RAM struct {
					Total, OtherBuckets, NodesCount, PerNodeMegs, ThisAlloc, ThisUsed, Free int64
				} `json:"ramSummary"`
				HDD map[string]int64 `json:"hddSummary"`
			} `json:"summaries"`
		}
		if err := json.Unmarshal([]byte(body), &parsed); err != nil {
			t.Errorf("%s: body %s is not JSON: %v", tt.name, body, err)
			continue
		}
		ram := parsed.Summaries.RAM
		if got := [6]int64{ram.Total, ram.OtherBuckets, ram.NodesCount, ram.PerNodeMegs, ram.ThisAlloc, ram.Free}; got != tt.ram {
			t.Errorf("%s: expected RAM summary %v, got %v", tt.name, tt.ram, got)
		}
		wantUsed := map[string]int64{"/default": 38148176, "/eph": 15695864}[tt.path]
		if ram.ThisUsed != wantUsed {
			t.Errorf("%s: expected thisUsed %d, got %d", tt.name, wantUsed, ram.ThisUsed)
		}
		if len(parsed.Summaries.HDD) != 5 {
			t.Errorf("%s: unexpected disk summary %v", tt.name, parsed.Summaries.HDD)
		}
	}
	if len(cluster.GetAllBuckets()) != 3 {
		t.Errorf("a request that only validates created a bucket")
	}
}

// TestBucketMaxTTLStored checks that a bucket keeps the maxTTL it is created
// or updated with and reports it as "maxTTL" in its config, and that an
// update keeps the settings it does not name.
func TestBucketMaxTTLStored(t *testing.T) {
	cluster := newBucketsTestCluster(t)

	resp := bucketsRequest(t, cluster, http.MethodPost, "", "", "name=nb&ramQuotaMB=100&maxTTL=30&replicaNumber=0&flushEnabled=1")
	if body := readBody(t, resp); resp.StatusCode != http.StatusAccepted || body != "" ||
		resp.Header.Get("Location") != "/pools/default/buckets/nb" {
		t.Fatalf("create: expected 202 with Location /pools/default/buckets/nb, got %d %q %v", resp.StatusCode, body, resp.Header)
	}
	resp = bucketsRequest(t, cluster, http.MethodPost, "", "", "name=nm&ramQuotaMB=100&bucketType=memcached&maxTTL=30")
	if body := readBody(t, resp); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create memcached: expected 202, got %d %q", resp.StatusCode, body)
	}

	type bucketConfig struct {
		MaxTTL          uint32 `json:"maxTTL"`
		ReplicaNumber   uint   `json:"replicaNumber"`
		ReplicaIndex    bool   `json:"replicaIndex"`
		CompressionMode string `json:"compressionMode"`
	}
	getConfig := func(name string) bucketConfig {
		resp := bucketsRequest(t, cluster, http.MethodGet, "/"+name, "", "")
		var config bucketConfig
		if err := json.Unmarshal([]byte(readBody(t, resp)), &config); err != nil {
			t.Fatalf("failed to decode the config of %s: %v", name, err)
		}
		return config
	}
	if config, want := getConfig("nb"), (bucketConfig{30, 0, false, "passive"}); config != want {
		t.Errorf("created: expected %+v, got %+v", want, config)
	}
	if config, want := getConfig("nm"), (bucketConfig{0, 0, false, "off"}); config != want {
		t.Errorf("created memcached: expected %+v, got %+v", want, config)
	}

	resp = bucketsRequest(t, cluster, http.MethodPost, "/nb", "", "maxTTL=40")
	if body := readBody(t, resp); resp.StatusCode != http.StatusOK || body != "" {
		t.Fatalf("update: expected 200 with no body, got %d %q", resp.StatusCode, body)
	}
	resp = bucketsRequest(t, cluster, http.MethodPost, "/nb", "", "compressionMode=active")
	if body := readBody(t, resp); resp.StatusCode != http.StatusOK {
		t.Fatalf("update: expected 200, got %d %q", resp.StatusCode, body)
	}
	if config, want := getConfig("nb"), (bucketConfig{40, 0, false, "active"}); config != want {
		t.Errorf("updated: expected %+v, got %+v", want, config)
	}
	bucket := cluster.GetBucket("nb")
	if !bucket.FlushEnabled() || bucket.RamQuota() != 100<<20 {
		t.Errorf("update changed settings it did not name: flush %v, quota %d", bucket.FlushEnabled(), bucket.RamQuota())
	}
}

// TestBucketSettingsStored creates and updates buckets with the settings
// ns_server keeps and checks the bucket config against what Couchbase
// Server 7.0.5 reports for the same requests.
func TestBucketSettingsStored(t *testing.T) {
	cluster := newBucketsTestCluster(t)

	tests := []struct {
		create, update   string
		created, updated string
	}{
		{
			"name=c1&ramQuotaMB=100",
			"maxTTL=5&threadsNumber=4&evictionPolicy=fullEviction&durabilityMinLevel=majority",
			`{"bucketType":"membase","evictionPolicy":"valueOnly","storageBackend":"couchstore","durabilityMinLevel":"none","conflictResolutionType":"seqno","threadsNumber":3,"autoCompactionSettings":false,"fragmentationPercentage":50,"pitrEnabled":false,"pitrGranularity":600,"pitrMaxHistoryAge":86400,"maxTTL":0,"compressionMode":"passive","replicaIndex":true,"replicaNumber":1,"quota":{"ram":104857600,"rawRAM":104857600}}`,
			`{"bucketType":"membase","evictionPolicy":"fullEviction","storageBackend":"couchstore","durabilityMinLevel":"majority","conflictResolutionType":"seqno","threadsNumber":4,"autoCompactionSettings":false,"fragmentationPercentage":50,"pitrEnabled":false,"pitrGranularity":600,"pitrMaxHistoryAge":86400,"maxTTL":5,"compressionMode":"passive","replicaIndex":true,"replicaNumber":1,"quota":{"ram":104857600,"rawRAM":104857600}}`,
		},
		{
			"name=c2&ramQuotaMB=100&bucketType=ephemeral&evictionPolicy=nruEviction&purgeInterval=0.5&durabilityMinLevel=majority",
			"purgeInterval=2&replicaNumber=0",
			`{"bucketType":"ephemeral","evictionPolicy":"nruEviction","storageBackend":"undefined","durabilityMinLevel":"majority","conflictResolutionType":"seqno","threadsNumber":3,"purgeInterval":0.5,"fragmentationPercentage":50,"pitrEnabled":false,"pitrGranularity":600,"pitrMaxHistoryAge":86400,"maxTTL":0,"compressionMode":"passive","replicaNumber":1,"quota":{"ram":104857600,"rawRAM":104857600}}`,
			`{"bucketType":"ephemeral","evictionPolicy":"nruEviction","storageBackend":"undefined","durabilityMinLevel":"majority","conflictResolutionType":"seqno","threadsNumber":3,"purgeInterval":2,"fragmentationPercentage":50,"pitrEnabled":false,"pitrGranularity":600,"pitrMaxHistoryAge":86400,"maxTTL":0,"compressionMode":"passive","replicaNumber":0,"quota":{"ram":104857600,"rawRAM":104857600}}`,
		},
		{
			"name=c3&ramQuotaMB=100&conflictResolutionType=lww&driftBehindThresholdMs=300&replicaNumber=0&flushEnabled=1",
			"driftAheadThresholdMs=200",
			`{"bucketType":"membase","evictionPolicy":"valueOnly","storageBackend":"couchstore","durabilityMinLevel":"none","conflictResolutionType":"lww","driftAheadThresholdMs":5000,"driftBehindThresholdMs":300,"threadsNumber":3,"autoCompactionSettings":false,"fragmentationPercentage":50,"pitrEnabled":false,"pitrGranularity":600,"pitrMaxHistoryAge":86400,"maxTTL":0,"compressionMode":"passive","replicaIndex":false,"replicaNumber":0,"quota":{"ram":104857600,"rawRAM":104857600}}`,
			`{"bucketType":"membase","evictionPolicy":"valueOnly","storageBackend":"couchstore","durabilityMinLevel":"none","conflictResolutionType":"lww","driftAheadThresholdMs":200,"driftBehindThresholdMs":300,"threadsNumber":3,"autoCompactionSettings":false,"fragmentationPercentage":50,"pitrEnabled":false,"pitrGranularity":600,"pitrMaxHistoryAge":86400,"maxTTL":0,"compressionMode":"passive","replicaIndex":false,"replicaNumber":0,"quota":{"ram":104857600,"rawRAM":104857600}}`,
		},
		{
			"name=c4&ramQuotaMB=100&autoCompactionDefined=true&parallelDBAndViewCompaction=true&databaseFragmentationThreshold%5Bpercentage%5D=40",
			"autoCompactionDefined=true&parallelDBAndViewCompaction=false&purgeInterval=1.5&allowedTimePeriod%5BfromHour%5D=1&allowedTimePeriod%5BtoHour%5D=2&allowedTimePeriod%5BfromMinute%5D=3&allowedTimePeriod%5BtoMinute%5D=4&allowedTimePeriod%5BabortOutside%5D=false",
			`{"bucketType":"membase","evictionPolicy":"valueOnly","storageBackend":"couchstore","durabilityMinLevel":"none","conflictResolutionType":"seqno","threadsNumber":3,"purgeInterval":3,"autoCompactionSettings":{"parallelDBAndViewCompaction":true,"databaseFragmentationThreshold":{"percentage":40,"size":"undefined"},"viewFragmentationThreshold":{"percentage":"undefined","size":"undefined"}},"fragmentationPercentage":50,"pitrEnabled":false,"pitrGranularity":600,"pitrMaxHistoryAge":86400,"maxTTL":0,"compressionMode":"passive","replicaIndex":true,"replicaNumber":1,"quota":{"ram":104857600,"rawRAM":104857600}}`,
			`{"bucketType":"membase","evictionPolicy":"valueOnly","storageBackend":"couchstore","durabilityMinLevel":"none","conflictResolutionType":"seqno","threadsNumber":3,"purgeInterval":1.5,"autoCompactionSettings":{"parallelDBAndViewCompaction":false,"allowedTimePeriod":{"fromHour":1,"toHour":2,"fromMinute":3,"toMinute":4,"abortOutside":false},"databaseFragmentationThreshold":{"percentage":"undefined","size":"undefined"},"viewFragmentationThreshold":{"percentage":"undefined","size":"undefined"}},"fragmentationPercentage":50,"pitrEnabled":false,"pitrGranularity":600,"pitrMaxHistoryAge":86400,"maxTTL":0,"compressionMode":"passive","replicaIndex":true,"replicaNumber":1,"quota":{"ram":104857600,"rawRAM":104857600}}`,
		},
		{
			"name=c5&ramQuotaMB=100&bucketType=memcached&flushEnabled=1",
			"flushEnabled=0",
			`{"bucketType":"memcached","evictionPolicy":"valueOnly","storageBackend":"undefined","durabilityMinLevel":"none","conflictResolutionType":"seqno","threadsNumber":3,"autoCompactionSettings":false,"fragmentationPercentage":50,"maxTTL":0,"compressionMode":"off","replicaNumber":0,"quota":{"ram":104857600,"rawRAM":104857600}}`,
			`{"bucketType":"memcached","evictionPolicy":"valueOnly","storageBackend":"undefined","durabilityMinLevel":"none","conflictResolutionType":"seqno","threadsNumber":3,"autoCompactionSettings":false,"fragmentationPercentage":50,"maxTTL":0,"compressionMode":"off","replicaNumber":0,"quota":{"ram":104857600,"rawRAM":104857600}}`,
		},
		{
			"name=c6&ramQuotaMB=100&autoCompactionDefined=true&parallelDBAndViewCompaction=true&purgeInterval=0.05",
			"autoCompactionDefined=false",
			`{"bucketType":"membase","evictionPolicy":"valueOnly","storageBackend":"couchstore","durabilityMinLevel":"none","conflictResolutionType":"seqno","threadsNumber":3,"purgeInterval":0.05,"autoCompactionSettings":{"parallelDBAndViewCompaction":true,"databaseFragmentationThreshold":{"percentage":"undefined","size":"undefined"},"viewFragmentationThreshold":{"percentage":"undefined","size":"undefined"}},"fragmentationPercentage":50,"pitrEnabled":false,"pitrGranularity":600,"pitrMaxHistoryAge":86400,"maxTTL":0,"compressionMode":"passive","replicaIndex":true,"replicaNumber":1,"quota":{"ram":104857600,"rawRAM":104857600}}`,
			`{"bucketType":"membase","evictionPolicy":"valueOnly","storageBackend":"couchstore","durabilityMinLevel":"none","conflictResolutionType":"seqno","threadsNumber":3,"autoCompactionSettings":false,"fragmentationPercentage":50,"pitrEnabled":false,"pitrGranularity":600,"pitrMaxHistoryAge":86400,"maxTTL":0,"compressionMode":"passive","replicaIndex":true,"replicaNumber":1,"quota":{"ram":104857600,"rawRAM":104857600}}`,
		},
	}
	check := func(name, stage, want string) {
		t.Helper()
		var config, expected map[string]interface{}
		if err := json.Unmarshal([]byte(readBody(t, bucketsRequest(t, cluster, http.MethodGet, "/"+name, "", ""))), &config); err != nil {
			t.Fatalf("%s: failed to decode the config: %v", name, err)
		}
		if err := json.Unmarshal([]byte(want), &expected); err != nil {
			t.Fatalf("bad expectation %s: %v", want, err)
		}
		for _, key := range []string{"bucketType", "evictionPolicy", "storageBackend", "durabilityMinLevel",
			"conflictResolutionType", "driftAheadThresholdMs", "driftBehindThresholdMs", "threadsNumber",
			"purgeInterval", "autoCompactionSettings", "fragmentationPercentage", "pitrEnabled", "pitrGranularity",
			"pitrMaxHistoryAge", "maxTTL", "compressionMode", "replicaIndex", "replicaNumber", "quota"} {
			got, gotOK := config[key]
			exp, expOK := expected[key]
			if gotOK != expOK || !reflect.DeepEqual(got, exp) {
				t.Errorf("%s %s: %s: expected %v (%v), got %v (%v)", name, stage, key, exp, expOK, got, gotOK)
			}
		}
	}
	for _, tt := range tests {
		name := strings.TrimPrefix(strings.SplitN(tt.create, "&", 2)[0], "name=")
		if resp := bucketsRequest(t, cluster, http.MethodPost, "", "", tt.create); resp.StatusCode != http.StatusAccepted {
			t.Fatalf("create %s: got %d %s", tt.create, resp.StatusCode, readBody(t, resp))
		}
		check(name, "created", tt.created)
		if resp := bucketsRequest(t, cluster, http.MethodPost, "/"+name, "", tt.update); resp.StatusCode != http.StatusOK {
			t.Fatalf("update %s: got %d %s", tt.update, resp.StatusCode, readBody(t, resp))
		}
		check(name, "updated", tt.updated)
		if resp := bucketsRequest(t, cluster, http.MethodDelete, "/"+name, "", ""); resp.StatusCode != http.StatusOK {
			t.Fatalf("drop %s: got %d %s", name, resp.StatusCode, readBody(t, resp))
		}
	}
}

// TestBucketRoutes checks answers of the bucket routes that do not depend
// on the parameters, as Couchbase Server 7.0.5 gives them.
func TestBucketRoutes(t *testing.T) {
	cluster := newBucketsTestCluster(t)
	port := cluster.Nodes()[0].MgmtService().ListenPort()

	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/pools/default", port), nil)
	req.SetBasicAuth("Administrator", "password")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	var pool struct {
		MemoryQuota   uint64 `json:"memoryQuota"`
		StorageTotals struct {
			RAM map[string]uint64 `json:"ram"`
		} `json:"storageTotals"`
	}
	if err := json.Unmarshal([]byte(readBody(t, resp)), &pool); err != nil {
		t.Fatalf("failed to decode /pools/default: %v", err)
	}
	ram := pool.StorageTotals.RAM
	if pool.MemoryQuota != 1024 || ram["quotaTotal"] != 1<<30 || ram["quotaTotalPerNode"] != 1<<30 ||
		ram["quotaUsed"] != 456<<20 || ram["quotaUsedPerNode"] != 456<<20 {
		t.Errorf("unexpected memory quota %d and RAM totals %v", pool.MemoryQuota, ram)
	}

	for _, r := range []struct{ method, path string }{
		{http.MethodGet, "/nob"},
		{http.MethodPost, "/nob"},
		{http.MethodDelete, "/nob"},
		{http.MethodPost, "/nob/controller/doFlush"},
		{http.MethodGet, "/nob/ddocs"},
		{http.MethodGet, "/pools/default/b/nob"},
		{http.MethodGet, "/pools/default/bs/nob"},
	} {
		resp := bucketsRequest(t, cluster, r.method, r.path, "", "")
		body := readBody(t, resp)
		if resp.StatusCode != http.StatusNotFound || body != "Requested resource not found.\r\n" ||
			resp.Header.Get("Content-Type") != "text/plain" {
			t.Errorf("%s %s: expected 404 text/plain %q, got %d %s %q", r.method, r.path,
				"Requested resource not found.\r\n", resp.StatusCode, resp.Header.Get("Content-Type"), body)
		}
	}

	resp = bucketsRequest(t, cluster, http.MethodPost, "", "", "name=a%25b&ramQuotaMB=100")
	if body := readBody(t, resp); resp.StatusCode != http.StatusAccepted || resp.Header.Get("Location") != "/pools/default/buckets/a%25b" {
		t.Errorf("create a%%b: expected 202 with Location /pools/default/buckets/a%%25b, got %d %s %v", resp.StatusCode, body, resp.Header)
	}

	for i := len(cluster.GetAllBuckets()); i < 30; i++ {
		if _, err := cluster.AddBucket(mock.NewBucketOptions{Name: fmt.Sprintf("b%d", i), Type: mock.BucketTypeMemcached}); err != nil {
			t.Fatalf("failed to add bucket: %v", err)
		}
	}
	resp = bucketsRequest(t, cluster, http.MethodPost, "", "", "name=nb&ramQuotaMB=100")
	if body := readBody(t, resp); resp.StatusCode != http.StatusBadRequest || body != `{"_":"Cannot create more than 30 buckets"}` {
		t.Errorf("31st bucket: expected 400 Cannot create more than 30 buckets, got %d %s", resp.StatusCode, body)
	}
}

// TestStorageUsageReported checks that the usage the mock reports is the
// same everywhere, as ns_server derives it: each node's interesting stats
// sum the buckets on it, the storage totals sum the nodes, and a bucket's
// basic stats sum its nodes, with the fields Couchbase Server 7.0.5 gives
// each bucket type.
func TestStorageUsageReported(t *testing.T) {
	cluster, err := NewCluster(mock.NewClusterOptions{InitialNode: mock.NewNodeOptions{}})
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })
	if _, err := cluster.AddNode(mock.NewNodeOptions{}); err != nil {
		t.Fatalf("failed to add node: %v", err)
	}
	for _, opts := range []mock.NewBucketOptions{
		{Name: "default", Type: mock.BucketTypeCouchbase, NumReplicas: 1, RamQuota: 256 << 20},
		{Name: "eph", Type: mock.BucketTypeEphemeral, NumReplicas: 1, RamQuota: 100 << 20},
		{Name: "mc", Type: mock.BucketTypeMemcached, RamQuota: 100 << 20},
	} {
		if _, err := cluster.AddBucket(opts); err != nil {
			t.Fatalf("failed to add bucket: %v", err)
		}
	}
	if err := cluster.Users().UpsertUser(mockauth.UpsertUserOptions{Username: "Administrator", Password: "password",
		Roles: []string{"admin"}}); err != nil {
		t.Fatalf("failed to add user: %v", err)
	}

	get := func(path string, v interface{}) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d%s", cluster.Nodes()[0].MgmtService().ListenPort(), path), nil)
		req.SetBasicAuth("Administrator", "password")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		if err := json.Unmarshal([]byte(readBody(t, resp)), v); err != nil {
			t.Fatalf("failed to decode %s: %v", path, err)
		}
	}

	const (
		couchbaseMem, couchbaseDisk, couchbaseData = 38148176, 4277712, 4249600
		ephemeralMem, ephemeralDisk                = 15695864, 34
	)
	var pool struct {
		Nodes []struct {
			InterestingStats map[string]float64 `json:"interestingStats"`
		} `json:"nodes"`
		StorageTotals struct {
			RAM map[string]float64 `json:"ram"`
			HDD map[string]float64 `json:"hdd"`
		} `json:"storageTotals"`
	}
	get("/pools/default", &pool)
	if len(pool.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(pool.Nodes))
	}
	for _, node := range pool.Nodes {
		stats := node.InterestingStats
		if stats["mem_used"] != couchbaseMem+ephemeralMem || stats["couch_docs_actual_disk_size"] != couchbaseDisk+ephemeralDisk ||
			stats["couch_docs_data_size"] != couchbaseData {
			t.Errorf("unexpected interesting stats %v", stats)
		}
	}
	if got := pool.StorageTotals.RAM["usedByData"]; got != 2*(couchbaseMem+ephemeralMem) {
		t.Errorf("expected ram usedByData %d, got %v", 2*(couchbaseMem+ephemeralMem), got)
	}
	if got := pool.StorageTotals.HDD["usedByData"]; got != 2*(couchbaseDisk+ephemeralDisk) {
		t.Errorf("expected hdd usedByData %d, got %v", 2*(couchbaseDisk+ephemeralDisk), got)
	}

	for _, want := range []struct {
		bucket string
		stats  map[string]float64
	}{
		{"default", map[string]float64{"quotaPercentUsed": float64(2*couchbaseMem) * 100 / float64(2*256<<20),
			"opsPerSec": 0, "diskFetches": 0, "itemCount": 0, "diskUsed": 2 * couchbaseDisk, "dataUsed": 2 * couchbaseData,
			"memUsed": 2 * couchbaseMem, "vbActiveNumNonResident": 0}},
		{"eph", map[string]float64{"quotaPercentUsed": float64(2*ephemeralMem) * 100 / float64(2*100<<20),
			"opsPerSec": 0, "diskFetches": 0, "itemCount": 0, "diskUsed": 2 * ephemeralDisk, "dataUsed": 0,
			"memUsed": 2 * ephemeralMem, "vbActiveNumNonResident": 0}},
		{"mc", map[string]float64{"quotaPercentUsed": 0, "opsPerSec": 0, "hitRatio": 0, "itemCount": 0, "memUsed": 0}},
	} {
		var config struct {
			BasicStats map[string]float64 `json:"basicStats"`
			Quota      map[string]float64 `json:"quota"`
		}
		get("/pools/default/buckets/"+want.bucket, &config)
		if !reflect.DeepEqual(config.BasicStats, want.stats) {
			t.Errorf("%s: expected basic stats %v, got %v", want.bucket, want.stats, config.BasicStats)
		}
		if config.Quota["ram"] != 2*config.Quota["rawRAM"] {
			t.Errorf("%s: expected the RAM quota of both nodes, got %v", want.bucket, config.Quota)
		}
	}
}

// TestBucketUpdatePublishesConfig checks that a bucket update through the
// REST API gives the bucket config a new revision, as ns_server's does.
func TestBucketUpdatePublishesConfig(t *testing.T) {
	cluster := newBucketsTestCluster(t)
	rev := func() float64 {
		var config struct {
			Rev float64 `json:"rev"`
		}
		req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/pools/default/b/default", cluster.Nodes()[0].MgmtService().ListenPort()), nil)
		req.SetBasicAuth("Administrator", "password")
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		if err := json.Unmarshal([]byte(readBody(t, r)), &config); err != nil {
			t.Fatalf("failed to decode the terse config: %v", err)
		}
		return config.Rev
	}
	before := rev()
	if resp := bucketsRequest(t, cluster, http.MethodPost, "/default", "", "maxTTL=5"); resp.StatusCode != http.StatusOK {
		t.Fatalf("update: got %d %s", resp.StatusCode, readBody(t, resp))
	}
	if after := rev(); after <= before {
		t.Fatalf("expected the config revision to grow from %v, got %v", before, after)
	}
}

// TestBucketRAMSummaryLargeQuotas checks RAM quotas beyond 64 bits against
// the answers of Couchbase Server 7.0.5, whose summary is computed on
// Erlang integers of any size, and that no such quota is stored on a
// cluster of more nodes.
func TestBucketRAMSummaryLargeQuotas(t *testing.T) {
	cluster := newBucketsTestCluster(t)
	const tooLarge = `"ramQuota":"RAM quota specified is too large to be provisioned into this cluster."`
	tests := []struct {
		path, body, errors string
		// ram is the RAM summary but thisUsed.
		ram [6]string
	}{
		{"", "name=nb&ramQuotaMB=8796093022208", `{` + tooLarge + `}`,
			[6]string{"1073741824", "478150656", "1", "8796093022208", "9223372036854775808", "-9223372036259184640"}},
		{"", "name=nb&ramQuotaMB=17592186044415", `{` + tooLarge + `}`,
			[6]string{"1073741824", "478150656", "1", "17592186044415", "18446744073708503040", "-18446744073112911872"}},
		{"", "name=nb&ramQuotaMB=100&otherBucketsRamQuotaMB=8796093022208", `{` + tooLarge + `}`,
			[6]string{"1073741824", "9223372037332926464", "1", "100", "104857600", "-9223372036364042240"}},
		{"", "name=nb&ramQuotaMB=100&otherBucketsRamQuotaMB=8796093022207", `{` + tooLarge + `}`,
			[6]string{"1073741824", "9223372037331877888", "1", "100", "104857600", "-9223372036362993664"}},
		{"", "name=nb&ramQuotaMB=100&otherBucketsRamQuotaMB=99999999999999999999999", `{` + tooLarge + `}`,
			[6]string{"1073741824", "104857600000000000000477102080", "1", "100", "104857600", "-104857599999999999999508217856"}},
		{"", "name=nb&ramQuotaMB=99999999999999999999999&otherBucketsRamQuotaMB=-1",
			`{` + tooLarge + `,"otherBucketsRamQuotaMB":"The other buckets RAM Quota must be a positive integer."}`,
			[6]string{"1073741824", "478150656", "1", "99999999999999999999999", "104857599999999999999998951424", "-104857599999999999999403360256"}},
		{"/default", "ramQuotaMB=8796093022208", `{` + tooLarge + `}`,
			[6]string{"1073741824", "209715200", "1", "8796093022208", "9223372036854775808", "-9223372035990749184"}},
		{"/default", "ramQuotaMB=100&otherBucketsRamQuotaMB=8796093022208", `{` + tooLarge + `}`,
			[6]string{"1073741824", "9223372037064491008", "1", "100", "104857600", "-9223372036095606784"}},
	}
	for _, tt := range tests {
		resp := bucketsRequest(t, cluster, http.MethodPost, tt.path, "just_validate=1", tt.body)
		body := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest || !strings.HasPrefix(body, `{"errors":`+tt.errors+`,`) {
			t.Errorf("%s %s: expected 400 with errors %s, got %d %s", tt.path, tt.body, tt.errors, resp.StatusCode, body)
			continue
		}
		if got := ramSummaryText(t, body); got != tt.ram {
			t.Errorf("%s %s: expected RAM summary %v, got %v", tt.path, tt.body, tt.ram, got)
		}
	}

	for nodes := 2; nodes <= 3; nodes++ {
		if _, err := cluster.AddNode(mock.NewNodeOptions{}); err != nil {
			t.Fatalf("failed to add node: %v", err)
		}
		resp := bucketsRequest(t, cluster, http.MethodPost, "", "", "name=nb&ramQuotaMB=8796093022208")
		body := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest || !strings.HasPrefix(body, `{"errors":{`+tooLarge+`},`) {
			t.Errorf("%d nodes: expected 400 too large, got %d %s", nodes, resp.StatusCode, body)
			continue
		}
		if got := ramSummaryText(t, body); got[2] != fmt.Sprint(nodes) || got[4] != map[int]string{2: "18446744073709551616", 3: "27670116110564327424"}[nodes] {
			t.Errorf("%d nodes: unexpected RAM summary %v", nodes, got)
		}
		if bucket := cluster.GetBucket("nb"); bucket != nil {
			t.Errorf("%d nodes: the bucket was created with quota %d", nodes, bucket.RamQuota())
		}
	}
}

// ramSummaryText returns the RAM summary in body but thisUsed, each number
// as the body has it.
func ramSummaryText(t *testing.T, body string) [6]string {
	t.Helper()
	var parsed struct {
		Summaries struct {
			RAM struct {
				Total, OtherBuckets, NodesCount, PerNodeMegs, ThisAlloc, Free json.Number
			} `json:"ramSummary"`
		} `json:"summaries"`
	}
	dec := json.NewDecoder(strings.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&parsed); err != nil {
		t.Fatalf("body %s is not JSON: %v", body, err)
	}
	ram := parsed.Summaries.RAM
	return [6]string{ram.Total.String(), ram.OtherBuckets.String(), ram.NodesCount.String(),
		ram.PerNodeMegs.String(), ram.ThisAlloc.String(), ram.Free.String()}
}

// TestBucketDropFlushAndDdocs checks the answers to a bucket drop and to a
// flush of a bucket without flush, and the ddocs entry and design document
// list of each bucket type, against Couchbase Server 7.0.5.
func TestBucketDropFlushAndDdocs(t *testing.T) {
	cluster := newBucketsTestCluster(t)

	resp := bucketsRequest(t, cluster, http.MethodPost, "/default/controller/doFlush", "", "")
	if body := readBody(t, resp); resp.StatusCode != http.StatusBadRequest || body != `{"_":"Flush is disabled for the bucket"}` ||
		resp.Header.Get("Content-Type") != "application/json" {
		t.Errorf("flush: expected 400 application/json, got %d %s %s", resp.StatusCode, resp.Header.Get("Content-Type"), body)
	}

	resp = bucketsRequest(t, cluster, http.MethodPost, "", "", "name=a%25b&ramQuotaMB=100")
	if body := readBody(t, resp); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create a%%b: expected 202, got %d %s", resp.StatusCode, body)
	}
	for path, want := range map[string]string{
		"/default": `{"uri":"/pools/default/buckets/default/ddocs"}`,
		"/a%25b":   `{"uri":"/pools/default/buckets/a%25b/ddocs"}`,
		"/eph":     ``,
		"/mc":      ``,
	} {
		for _, prefix := range []string{"", "/pools/default/b"} {
			resp := bucketsRequest(t, cluster, http.MethodGet, prefix+path, "", "")
			var config struct {
				Ddocs json.RawMessage `json:"ddocs"`
			}
			if err := json.Unmarshal([]byte(readBody(t, resp)), &config); err != nil {
				t.Fatalf("GET %s%s: %v", prefix, path, err)
			}
			if string(config.Ddocs) != want {
				t.Errorf("GET %s%s: expected ddocs %s, got %s", prefix, path, want, config.Ddocs)
			}
		}
	}

	for path, want := range map[string]string{
		"/default/ddocs": `200 {"rows":[]}`,
		"/eph/ddocs":     `400 {"error":"no_ddocs_service"}`,
		"/mc/ddocs":      `400 {"error":"no_ddocs_service"}`,
	} {
		resp := bucketsRequest(t, cluster, http.MethodGet, path, "", "")
		body := readBody(t, resp)
		if got := fmt.Sprintf("%d %s", resp.StatusCode, body); got != want || resp.Header.Get("Content-Type") != "application/json" {
			t.Errorf("GET %s: expected %s as application/json, got %s as %s", path, want, got, resp.Header.Get("Content-Type"))
		}
	}

	resp = bucketsRequest(t, cluster, http.MethodDelete, "/eph", "", "")
	if body := readBody(t, resp); resp.StatusCode != http.StatusOK || body != "" || resp.Header.Get("Content-Type") != "" {
		t.Errorf("drop: expected 200 with no body, got %d %s %q", resp.StatusCode, resp.Header.Get("Content-Type"), body)
	}
}

// TestBucketChangesSerialised sends bucket changes at the same time, which
// ns_server makes one at a time: concurrent updates of different settings
// all apply, and concurrent creates stop at 30 buckets.
func TestBucketChangesSerialised(t *testing.T) {
	cluster, err := NewCluster(mock.NewClusterOptions{InitialNode: mock.NewNodeOptions{}, MemoryQuota: 16 << 30})
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })
	if err := cluster.Users().UpsertUser(mockauth.UpsertUserOptions{Username: "Administrator", Password: "password",
		Roles: []string{"admin"}}); err != nil {
		t.Fatalf("failed to add user: %v", err)
	}
	if _, err := cluster.AddBucket(mock.NewBucketOptions{Name: "default", Type: mock.BucketTypeCouchbase, RamQuota: 100 << 20}); err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}

	const rounds = 50
	var wg sync.WaitGroup
	for _, param := range []string{"maxTTL", "threadsNumber", "flushEnabled"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 1; i <= rounds; i++ {
				value := map[string]int{"maxTTL": i, "threadsNumber": 2 + i%7, "flushEnabled": i % 2}[param]
				resp := bucketsRequest(t, cluster, http.MethodPost, "/default", "", fmt.Sprintf("%s=%d", param, value))
				if body := readBody(t, resp); resp.StatusCode != http.StatusOK {
					t.Errorf("update %s: expected 200, got %d %s", param, resp.StatusCode, body)
				}
			}
		}()
	}
	wg.Wait()
	bucket := cluster.GetBucket("default")
	if bucket.MaxTTL() != rounds || bucket.Settings().NumThreads != json.Number(fmt.Sprint(2+rounds%7)) || bucket.FlushEnabled() != (rounds%2 == 1) {
		t.Errorf("an update was lost: maxTTL %d, threadsNumber %s, flushEnabled %t",
			bucket.MaxTTL(), bucket.Settings().NumThreads, bucket.FlushEnabled())
	}

	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			readBody(t, bucketsRequest(t, cluster, http.MethodPost, "", "", fmt.Sprintf("name=b%d&ramQuotaMB=100", i)))
		}()
	}
	wg.Wait()
	if n := len(cluster.GetAllBuckets()); n != 30 {
		t.Errorf("expected 30 buckets, got %d", n)
	}
}

// TestStalledBodyBlocksNoChange checks that a bucket update or scope create
// whose body has not all arrived holds no lock: changes to other buckets go
// on meanwhile.
func TestStalledBodyBlocksNoChange(t *testing.T) {
	cluster := newBucketsTestCluster(t)
	addr := fmt.Sprintf("127.0.0.1:%d", cluster.Nodes()[0].MgmtService().ListenPort())
	auth := base64.StdEncoding.EncodeToString([]byte("Administrator:password"))
	for _, path := range []string{"/pools/default/buckets/default", "/pools/default/buckets/default/scopes"} {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("failed to dial: %v", err)
		}
		if _, err := fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: x\r\nAuthorization: Basic %s\r\n"+
			"Content-Type: application/x-www-form-urlencoded\r\nContent-Length: 100\r\n\r\nname=", path, auth); err != nil {
			t.Fatalf("failed to write: %v", err)
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			readBody(t, bucketsRequest(t, cluster, http.MethodPost, "/eph", "", "maxTTL=5"))
			readBody(t, bucketsRequest(t, cluster, http.MethodPost, "/eph/scopes", "", "name=s"))
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("%s: a stalled body blocked changes to another bucket", path)
		}
		_ = conn.Close()
		<-done
	}
	if cluster.GetBucket("eph").MaxTTL() != 5 {
		t.Errorf("the update of eph was not applied")
	}
}
