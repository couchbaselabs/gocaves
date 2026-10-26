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
		config["collectionsManifestUid"] = fmt.Sprintf("%x", b.CollectionManifest().UID())
	}
	if b.BucketType() == mock.BucketTypeCouchbase {
		config["ddocs"] = ddocsConfig(b)
	}
	settings := b.Settings()
	config["evictionPolicy"] = settings.EvictionPolicy
	config["storageBackend"] = settings.StorageBackend
	config["durabilityMinLevel"] = settings.DurabilityMinLevel
	config["saslPassword"] = "f5461fdf070ba44b7f1ca2f18bd7bb28"
	config["compressionMode"] = string(b.CompressionMode())
	config["replicaNumber"] = b.NumReplicas()
	config["threadsNumber"] = settings.NumThreads
	config["authType"] = "sasl"
	config["fragmentationPercentage"] = 50
	config["conflictResolutionType"] = settings.ConflictResolutionType
	if settings.ConflictResolutionType == "lww" {
		config["driftAheadThresholdMs"] = settings.DriftAheadThresholdMs
		config["driftBehindThresholdMs"] = settings.DriftBehindThresholdMs
	}
	config["maxTTL"] = b.MaxTTL()
	// menelaus_web_buckets:build_bucket_info reports these by bucket type.
	switch b.BucketType() {
	case mock.BucketTypeCouchbase:
		config["replicaIndex"] = b.ReplicaIndexEnabled()
		config["autoCompactionSettings"] = false
		if ac := settings.AutoCompaction; ac != nil {
			config["autoCompactionSettings"] = autoCompactionSettingsJSON(ac)
			// compaction_api:get_purge_interval(global) defaults to 3 days.
			config["purgeInterval"] = json.Number("3")
			if settings.PurgeInterval != "" {
				config["purgeInterval"] = settings.PurgeInterval
			}
		}
	case mock.BucketTypeEphemeral:
		config["purgeInterval"] = settings.PurgeInterval
	case mock.BucketTypeMemcached:
		config["autoCompactionSettings"] = false
	}
	if b.BucketType() != mock.BucketTypeMemcached {
		config["pitrEnabled"] = false
		config["pitrGranularity"] = 600
		config["pitrMaxHistoryAge"] = 86400
	}

	config["localRandomKeyUri"] = fmt.Sprintf("/pools/default/buckets/%s/localRandomKey", b.Name())
	config["uri"] = fmt.Sprintf("/pools/default/buckets/%s?bucket_uuid=%s", b.Name(), b.ID())
	config["streamingUri"] = fmt.Sprintf("/pools/default/bucketsStreaming/%s?bucket_uuid=%s", b.Name(), b.ID())

	// menelaus_stats:basic_stats sums the usage of the bucket's nodes; a
	// memcached bucket has no disk.
	nodes := uint64(len(bucketServers(b)))
	usage := nodeBucketUsage(b)
	quota := b.RamQuota() * nodes
	quotaPercentUsed := interface{}(0)
	if quota > 0 && usage.mem > 0 {
		quotaPercentUsed = float64(usage.mem*nodes) * 100 / float64(quota)
	}
	if b.BucketType() == mock.BucketTypeMemcached {
		config["basicStats"] = map[string]interface{}{
			"quotaPercentUsed": quotaPercentUsed,
			"opsPerSec":        0,
			"hitRatio":         0,
			"itemCount":        0,
			"memUsed":          usage.mem * nodes,
		}
	} else {
		config["basicStats"] = map[string]interface{}{
			"quotaPercentUsed":       quotaPercentUsed,
			"opsPerSec":              0,
			"diskFetches":            0,
			"itemCount":              0,
			"diskUsed":               usage.disk * nodes,
			"dataUsed":               usage.data * nodes,
			"memUsed":                usage.mem * nodes,
			"vbActiveNumNonResident": 0,
		}
	}

	// ns_bucket:ram_quota is the quota of all the bucket's nodes.
	config["quota"] = map[string]interface{}{
		"ram":    quota,
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
		config["collectionsManifestUid"] = fmt.Sprintf("%x", b.CollectionManifest().UID())
	}

	config["uri"] = fmt.Sprintf("/pools/default/buckets/%s?bucket_uuid=%s", b.Name(), b.ID())
	config["streamingUri"] = fmt.Sprintf("/pools/default/bucketsStreaming/%s?bucket_uuid=%s", b.Name(), b.ID())

	if b.BucketType() == mock.BucketTypeCouchbase {
		config["ddocs"] = ddocsConfig(b)
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

// autoCompactionSettingsJSON is menelaus_web_autocompaction:
// build_bucket_settings.
func autoCompactionSettingsJSON(ac *mock.AutoCompactionSettings) map[string]interface{} {
	threshold := func(percentage, size *json.Number) map[string]interface{} {
		value := func(n *json.Number) interface{} {
			if n == nil {
				return "undefined"
			}
			return *n
		}
		return map[string]interface{}{"percentage": value(percentage), "size": value(size)}
	}
	settings := map[string]interface{}{
		"parallelDBAndViewCompaction":    ac.ParallelDBAndViewCompaction,
		"databaseFragmentationThreshold": threshold(ac.DatabaseFragmentationPercentage, ac.DatabaseFragmentationSize),
		"viewFragmentationThreshold":     threshold(ac.ViewFragmentationPercentage, ac.ViewFragmentationSize),
	}
	if p := ac.AllowedTimePeriod; p != nil {
		settings["allowedTimePeriod"] = map[string]interface{}{
			"fromHour":     p.FromHour,
			"toHour":       p.ToHour,
			"fromMinute":   p.FromMinute,
			"toMinute":     p.ToMinute,
			"abortOutside": p.AbortOutside,
		}
	}
	return settings
}

// ddocsConfig is the ddocs entry ns_server gives a bucket that can hold
// design documents, a couchbase bucket.
func ddocsConfig(b mock.Bucket) map[string]interface{} {
	return map[string]interface{}{
		"uri": "/pools/default/buckets/" + mochiwebQuotePlus(b.Name()) + "/ddocs",
	}
}
