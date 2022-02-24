package svcimpls

import (
	"encoding/json"
	"fmt"

	"github.com/couchbaselabs/gocaves/mock"
)

// GenBucketConfig returns the current config for a bucket.
func GenBucketConfig(b mock.Bucket, reqNode mock.ClusterNode) []byte {
	kvNodes, vbMap, allNodes := b.GetVbServerInfo(reqNode)

	config := make(map[string]interface{})
	config["name"] = b.Name()
	config["uuid"] = b.ID()

	config["bucketType"] = b.BucketType().Name()

	if b.BucketType() != mock.BucketTypeMemcached {
		config["collectionsManifestUid"] = fmt.Sprintf("%d", b.CollectionManifest().Rev)
		config["durabilityMinLevel"] = "none"

		config["ddocs"] = map[string]interface{}{
			"uri": fmt.Sprintf("/pools/default/%s/default/ddocs", b.Name()),
		}
	}
	config["evictionPolicy"] = "valueOnly"
	config["storageBackend"] = "couchstore"
	config["saslPassword"] = "f5461fdf070ba44b7f1ca2f18bd7bb28"
	config["compressionMode"] = string(b.CompressionMode())
	config["replicaIndex"] = b.ReplicaIndexEnabled()
	config["replicaNumber"] = b.NumReplicas()
	config["threadsNumber"] = 1
	config["authType"] = "sasl"
	config["autoCompactionSettings"] = false
	config["fragmentationPercentage"] = 50
	config["conflictResolutionType"] = "seqno"
	config["maxTTL"] = 0

	config["localRandomKeyUri"] = fmt.Sprintf("/pools/default/buckets/%s/localRandomKey", b.Name())
	config["uri"] = fmt.Sprintf("/pools/default/buckets/%s?bucket_uuid=%s", b.Name(), b.ID())
	config["streamingUri"] = fmt.Sprintf("/pools/default/bucketsStreaming/%s?bucket_uuid=%s", b.Name(), b.ID())

	config["basicStats"] = map[string]interface{}{
		"quotaPercentUsed":       40,
		"opsPerSec":              0,
		"diskFetches":            0,
		"itemCount":              0,
		"diskUsed":               4277712,
		"dataUsed":               4249600,
		"memUsed":                38148176,
		"vbActiveNumNonResident": 0,
	}

	config["quota"] = map[string]interface{}{
		"ram":    b.RamQuota(),
		"rawRAM": b.RamQuota(),
	}

	config["bucketCapabilitiesVer"] = ""
	config["bucketCapabilities"] = []string{
		"collections",
		"durableWrite",
		"tombstonedUserXAttrs",
		"couchapi",
		"dcp",
		"cbhello",
		"touch",
		"cccp",
		"xdcrCheckpointing",
		"nodesExt",
		"xattr",
	}

	controllers := map[string]interface{}{
		"compactAll":    fmt.Sprintf("/pools/default/buckets/%s/controller/compactBucket", b.Name()),
		"compactDB":     fmt.Sprintf("/pools/default/buckets/%s/controller/compactDatabases", b.Name()),
		"purgeDeletes":  fmt.Sprintf("/pools/default/buckets/%s/controller/unsafePurgeBucket", b.Name()),
		"startRecovery": fmt.Sprintf("/pools/default/buckets/%s/controller/startRecovery", b.Name()),
	}
	if b.FlushEnabled() {
		controllers["flush"] = fmt.Sprintf("/pools/default/buckets/%s/controller/doFlush", b.Name())
	}

	config["controllers"] = controllers
	config["stats"] = map[string]interface{}{
		"uri":              fmt.Sprintf("/pools/default/buckets/%s/stats", b.Name()),
		"directoryURI":     fmt.Sprintf("/pools/default/buckets/%s/stats/Directory", b.Name()),
		"nodeStatsListURI": fmt.Sprintf("/pools/default/buckets/%s/nodes", b.Name()),
	}

	nodesConfig := make([]interface{}, 0)
	for _, server := range allNodes {
		nodeConfig := GenClusterNodeConfig(server, reqNode, b)
		nodesConfig = append(nodesConfig, json.RawMessage(nodeConfig))
	}
	config["nodes"] = nodesConfig

	if b.BucketType() == mock.BucketTypeMemcached {
		config["nodeLocator"] = "ketama"
	} else {
		config["nodeLocator"] = "vbucket"
		vbConfig := make(map[string]interface{})
		vbConfig["hashAlgorithm"] = "CRC"
		vbConfig["numReplicas"] = b.NumReplicas()
		vbConfig["vBucketMap"] = vbMap

		var vbServerList []interface{}
		for _, node := range kvNodes {
			address := fmt.Sprintf("%s:%d", node.KvService().Hostname(), node.KvService().ListenPort())
			vbServerList = append(vbServerList, address)
		}
		vbConfig["serverList"] = vbServerList

		config["vBucketServerMap"] = vbConfig
	}

	configBytes, _ := json.Marshal(config)
	return configBytes
}

// GenTerseBucketConfig returns the current mini config for a bucket.
func GenTerseBucketConfig(b mock.Bucket, reqNode mock.ClusterNode) []byte {
	kvNodes, vbMap, allNodes := b.GetVbServerInfo(reqNode)

	config := make(map[string]interface{})
	config["rev"] = b.ConfigRev()
	config["name"] = b.Name()
	config["uuid"] = b.ID()

	if b.BucketType() != mock.BucketTypeMemcached {
		config["collectionsManifestUid"] = fmt.Sprintf("%d", b.CollectionManifest().Rev)
	}

	config["uri"] = fmt.Sprintf("/pools/default/buckets/%s?bucket_uuid=%s", b.Name(), b.ID())
	config["streamingUri"] = fmt.Sprintf("/pools/default/bucketsStreaming/%s?bucket_uuid=%s", b.Name(), b.ID())

	if b.BucketType() != mock.BucketTypeMemcached {
		config["ddocs"] = map[string]interface{}{
			"uri": fmt.Sprintf("/pools/default/%s/default/ddocs", b.Name()),
		}
	}

	config["clusterCapabilitiesVer"] = []int{1, 0}

	config["clusterCapabilities"] = map[string]interface{}{
		"n1ql": []string{
			"costBasedOptimizer",
			"indexAdvisor",
			"javaScriptFunctions",
			"inlineFunctions",
			"enhancedPreparedStatements",
		},
	}

	config["bucketCapabilitiesVer"] = ""
	config["bucketCapabilities"] = []string{
		"collections",
		"durableWrite",
		"tombstonedUserXAttrs",
		"couchapi",
		"dcp",
		"cbhello",
		"touch",
		"cccp",
		"xdcrCheckpointing",
		"nodesExt",
		"xattr",
	}

	nodesConfig := make([]interface{}, 0)
	nodesExtConfig := make([]interface{}, 0)
	for _, server := range allNodes {
		nodeConfig := GenTerseClusterNodeConfig(server, reqNode, b)
		nodesConfig = append(nodesConfig, json.RawMessage(nodeConfig))

		nodeExtConfig := GenExtClusterNodeConfig(server, reqNode, b)
		nodesExtConfig = append(nodesExtConfig, json.RawMessage(nodeExtConfig))
	}
	config["nodes"] = nodesConfig
	config["nodesExt"] = nodesExtConfig

	if b.BucketType() == mock.BucketTypeMemcached {
		config["nodeLocator"] = "ketama"
	} else {
		config["nodeLocator"] = "vbucket"

		vbConfig := make(map[string]interface{})
		vbConfig["hashAlgorithm"] = "CRC"
		vbConfig["numReplicas"] = b.NumReplicas()
		vbConfig["vBucketMap"] = vbMap

		var vbServerList []interface{}
		for _, node := range kvNodes {
			address := fmt.Sprintf("%s:%d", node.KvService().Hostname(), node.KvService().ListenPort())
			vbServerList = append(vbServerList, address)
		}
		vbConfig["serverList"] = vbServerList

		config["vBucketServerMap"] = vbConfig
	}

	configBytes, _ := json.Marshal(config)
	return configBytes
}
