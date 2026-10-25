package svcimpls

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/couchbaselabs/gocaves/mock"
)

const mib = 1024 * 1024

// The memory and disk the mock reports a node using, which it does not
// measure; the usage of the buckets on it is bucketUsage.
const (
	nodeMemoryTotal    = 49093763072
	nodeMemoryFree     = 44611338240
	nodeDiskTotal      = 107374182400
	nodeDiskUsed       = 53687091200
	maxBucketsCount    = 30
	ephemeralPurgeMin  = "0.0007"
	persistentPurgeMin = "0.04"
)

// bucketUsage is the memory and disk a bucket uses on each of its nodes,
// as the node's interesting stats mem_used, couch_docs_actual_disk_size and
// couch_docs_data_size report it. The mock does not measure it; the values
// are those of an empty bucket of each type on Couchbase Server 7.0.5. A
// memcached bucket reports none.
type bucketUsage struct {
	mem, disk, data uint64
}

func nodeBucketUsage(b mock.Bucket) bucketUsage {
	switch b.BucketType() {
	case mock.BucketTypeCouchbase:
		return bucketUsage{mem: 38148176, disk: 4277712, data: 4249600}
	case mock.BucketTypeEphemeral:
		return bucketUsage{mem: 15695864, disk: 34}
	}
	return bucketUsage{}
}

// bucketServers are the nodes a bucket is on, as ns_bucket:get_servers
// lists them: the nodes of its vbucket map, or, for a memcached bucket,
// every node running the data service.
func bucketServers(b mock.Bucket) []mock.ClusterNode {
	kvNodes, _, allNodes := b.GetVbServerInfo(nil)
	if b.BucketType() != mock.BucketTypeMemcached {
		return kvNodes
	}
	var servers []mock.ClusterNode
	for _, node := range allNodes {
		if node.KvService() != nil {
			servers = append(servers, node)
		}
	}
	return servers
}

// kvNodeCount is the number of nodes running the data service.
func kvNodeCount(cluster mock.Cluster) uint64 {
	var nodes uint64
	for _, node := range cluster.Nodes() {
		if node.KvService() != nil {
			nodes++
		}
	}
	return nodes
}

// nodeUsage sums the usage of the buckets on a node, as the node's
// interesting stats report it.
func nodeUsage(cluster mock.Cluster, node mock.ClusterNode) bucketUsage {
	var usage bucketUsage
	for _, bucket := range cluster.GetAllBuckets() {
		for _, server := range bucketServers(bucket) {
			if server.ID() == node.ID() {
				u := nodeBucketUsage(bucket)
				usage.mem += u.mem
				usage.disk += u.disk
				usage.data += u.data
			}
		}
	}
	return usage
}

// clusterStorageTotals is ns_storage_conf:cluster_storage_info over the
// nodes running the data service.
type clusterStorageTotals struct {
	nodesCount uint64
	ram        storageTotalsRAM
	hdd        storageTotalsHDD
}

type storageTotalsRAM struct {
	Total             uint64 `json:"total"`
	QuotaTotal        uint64 `json:"quotaTotal"`
	QuotaUsed         uint64 `json:"quotaUsed"`
	Used              uint64 `json:"used"`
	UsedByData        uint64 `json:"usedByData"`
	QuotaUsedPerNode  uint64 `json:"quotaUsedPerNode"`
	QuotaTotalPerNode uint64 `json:"quotaTotalPerNode"`
}

type storageTotalsHDD struct {
	Total      uint64 `json:"total"`
	QuotaTotal uint64 `json:"quotaTotal"`
	Used       uint64 `json:"used"`
	UsedByData uint64 `json:"usedByData"`
	Free       uint64 `json:"free"`
}

func getClusterStorageTotals(cluster mock.Cluster) clusterStorageTotals {
	nodes := kvNodeCount(cluster)
	var quotaUsedPerNode, memByData, diskByData uint64
	for _, bucket := range cluster.GetAllBuckets() {
		quotaUsedPerNode += bucket.RamQuota()
	}
	// The data usage is the sum of the nodes' interesting stats.
	for _, node := range cluster.Nodes() {
		usage := nodeUsage(cluster, node)
		memByData += usage.mem
		diskByData += usage.disk
	}
	quotaTotalPerNode := cluster.MemoryQuota()
	ramUsed := nodes * (nodeMemoryTotal - nodeMemoryFree)
	if ramUsed < memByData {
		ramUsed = memByData
	}
	diskUsed := nodes * nodeDiskUsed
	if diskUsed < diskByData {
		diskUsed = diskByData
	}
	return clusterStorageTotals{
		nodesCount: nodes,
		ram: storageTotalsRAM{
			Total:             nodes * nodeMemoryTotal,
			QuotaTotal:        quotaTotalPerNode * nodes,
			QuotaUsed:         quotaUsedPerNode * nodes,
			Used:              ramUsed,
			UsedByData:        memByData,
			QuotaUsedPerNode:  quotaUsedPerNode,
			QuotaTotalPerNode: quotaTotalPerNode,
		},
		hdd: storageTotalsHDD{
			Total:      nodes * nodeDiskTotal,
			QuotaTotal: nodes * nodeDiskTotal,
			Used:       diskUsed,
			UsedByData: diskByData,
			Free:       (nodeDiskTotal - nodeDiskUsed) * nodes,
		},
	}
}

// bucketParams is what ns_server 7.0's menelaus_web_buckets
// parse_bucket_params makes of the parameters of a bucket create or update.
type bucketParams struct {
	bucketType          mock.BucketType
	numReplicas         uint
	numReplicasSet      bool
	flushEnabled        bool
	ramQuota            uint64   // as stored, which an accepted quota fits
	ramQuotaBytes       *big.Int // as given, whatever its size
	ramQuotaSet         bool
	otherBucketsRAM     *big.Int
	replicaIndexEnabled bool
	compressionMode     mock.CompressionMode
	maxTTL              uint32
	settings            mock.BucketSettings
	// errors are in the order parse_bucket_params reports them, the RAM
	// errors first.
	errors        []formParam
	ramSummary    ramSummary
	hddSummary    hddSummary
	isNew         bool
	durabilitySet bool
}

// ramSummary is computed on integers of any size, as Erlang's are.
type ramSummary struct {
	Total        *big.Int `json:"total"`
	OtherBuckets *big.Int `json:"otherBuckets"`
	NodesCount   *big.Int `json:"nodesCount"`
	PerNodeMegs  *big.Int `json:"perNodeMegs"`
	ThisAlloc    *big.Int `json:"thisAlloc"`
	ThisUsed     *big.Int `json:"thisUsed"`
	Free         *big.Int `json:"free"`
}

type hddSummary struct {
	Total        int64 `json:"total"`
	OtherData    int64 `json:"otherData"`
	OtherBuckets int64 `json:"otherBuckets"`
	ThisUsed     int64 `json:"thisUsed"`
	Free         int64 `json:"free"`
}

const (
	numberOK = iota
	numberInvalid
	numberTooSmall
	numberTooLarge
)

var (
	erlIntegerRe = regexp.MustCompile(`^[+-]?[0-9]+$`)
	erlFloatRe   = regexp.MustCompile(`^[+-]?[0-9]+\.[0-9]+([eE][+-]?[0-9]+)?$`)
)

// parseValidateInteger is menelaus_util:parse_validate_number with
// list_to_integer: spaces are stripped from both ends and the rest must be
// an integer of any size. A nil bound is none.
func parseValidateInteger(value string, present bool, min, max *big.Int) (*big.Int, int) {
	value = strings.Trim(value, " ")
	if !present || !erlIntegerRe.MatchString(value) {
		return nil, numberInvalid
	}
	n, _ := new(big.Int).SetString(value, 10)
	switch {
	case min != nil && n.Cmp(min) < 0:
		return n, numberTooSmall
	case max != nil && n.Cmp(max) > 0:
		return n, numberTooLarge
	}
	return n, numberOK
}

// parseValidateFloat is menelaus_util:parse_validate_number with
// list_to_float, which takes an integer or an Erlang float, and returns
// the number as ns_server's JSON encoder writes it.
func parseValidateFloat(value string, present bool, min, max float64) (json.Number, int) {
	value = strings.Trim(value, " ")
	if !present {
		return "", numberInvalid
	}
	var f float64
	var text string
	switch {
	case erlIntegerRe.MatchString(value):
		n, _ := new(big.Int).SetString(value, 10)
		f, _ = new(big.Float).SetInt(n).Float64()
		text = n.String()
	case erlFloatRe.MatchString(value):
		f, _ = strconv.ParseFloat(value, 64)
		text = strconv.FormatFloat(f, 'g', 16, 64)
	default:
		return "", numberInvalid
	}
	switch {
	case f < min:
		return "", numberTooSmall
	case f > max:
		return "", numberTooLarge
	}
	return json.Number(text), numberOK
}

func bigInt(n int64) *big.Int {
	return big.NewInt(n)
}

// parseBucketParams validates the parameters of a bucket create, with
// current nil, or of an update of current, as parse_bucket_params does:
// each parameter in the order ns_server checks it, a missing one taking
// its default on create and keeping the bucket's value on update. gocaves
// runs as Enterprise Edition without developer preview.
func parseBucketParams(params []formParam, current mock.Bucket, cluster mock.Cluster) bucketParams {
	isNew := current == nil
	res := bucketParams{isNew: isNew, ramQuotaBytes: new(big.Int), otherBucketsRAM: new(big.Int)}
	var errs []formParam
	fail := func(key, msg string) {
		errs = append(errs, formParam{key, msg})
	}
	get := func(key string) (string, bool) {
		return paramValue(params, key)
	}

	invalidType := false
	typeName, typeGiven := get("bucketType")
	if isNew {
		res.bucketType = mock.BucketTypeCouchbase
		switch {
		case !typeGiven, typeName == "membase", typeName == "couchbase":
		case typeName == "ephemeral":
			res.bucketType = mock.BucketTypeEphemeral
		case typeName == "memcached":
			res.bucketType = mock.BucketTypeMemcached
		default:
			invalidType = true
		}
	} else {
		res.bucketType = current.BucketType()
		res.numReplicas = current.NumReplicas()
		res.flushEnabled = current.FlushEnabled()
		res.ramQuota = current.RamQuota()
		res.ramQuotaBytes.SetUint64(res.ramQuota)
		res.ramQuotaSet = true
		res.replicaIndexEnabled = current.ReplicaIndexEnabled()
		res.compressionMode = current.CompressionMode()
		res.maxTTL = current.MaxTTL()
		res.settings = current.Settings()
	}
	isEphemeral := res.bucketType == mock.BucketTypeEphemeral
	isMemcached := res.bucketType == mock.BucketTypeMemcached

	// validate_common_params
	if value, ok := get("flushEnabled"); ok || isNew {
		switch {
		case !ok, value == "0":
			res.flushEnabled = false
		case value == "1":
			res.flushEnabled = true
		default:
			fail("flushEnabled", "flushEnabled can only be 1 or 0")
		}
	}
	// init_bucket_validation_context takes the bucket of the name, if
	// there is one, as the bucket config of a create too: its quota stands
	// in for a missing ramQuota, and its servers are counted.
	configBucket := current
	if isNew {
		name, ok := get("name")
		if msg := validateBucketName(name, ok, cluster); msg != "" {
			fail("name", msg)
		}
		configBucket = cluster.GetBucket(name)
	}
	ramQuota, ok := get("ramQuota")
	if !ok {
		ramQuota, ok = get("ramQuotaMB")
	}
	switch {
	case !ok && isNew && configBucket != nil:
		res.ramQuota = configBucket.RamQuota()
		res.ramQuotaBytes.SetUint64(res.ramQuota)
		res.ramQuotaSet = true
	case ok || isNew:
		res.ramQuotaSet = false
		n, status := parseValidateInteger(ramQuota, ok, bigInt(0), nil)
		switch status {
		case numberInvalid:
			fail("ramQuota", "The RAM Quota must be specified and must be a positive integer.")
		case numberTooSmall:
			fail("ramQuota", "The RAM Quota cannot be negative.")
		default:
			res.ramQuotaBytes.Mul(n, bigInt(mib))
			res.ramQuota = 0
			if res.ramQuotaBytes.IsUint64() {
				res.ramQuota = res.ramQuotaBytes.Uint64()
			}
			res.ramQuotaSet = true
		}
	}
	if value, ok := get("otherBucketsRamQuotaMB"); ok {
		n, status := parseValidateInteger(value, true, bigInt(0), nil)
		if status != numberOK {
			fail("otherBucketsRamQuotaMB", "The other buckets RAM Quota must be a positive integer.")
		} else {
			res.otherBucketsRAM.Mul(n, bigInt(mib))
		}
	}
	if value, ok := get("proxyPort"); ok && value != "none" {
		fail("proxyPort", "Setting proxy port is not allowed on this version of the cluster")
	}
	quotaSizeError := func(min uint64) {
		if !res.ramQuotaSet {
			return
		}
		switch {
		case res.ramQuotaBytes.Cmp(new(big.Int).SetUint64(min*mib)) < 0:
			fail("ramQuota", fmt.Sprintf("RAM quota cannot be less than %d MiB", min))
		case isMemcached && !isNew && res.ramQuotaBytes.Cmp(new(big.Int).SetUint64(current.RamQuota())) != 0:
			fail("ramQuota", "cannot change quota of memcached buckets")
		}
	}

	switch {
	case invalidType:
		fail("bucketType", "invalid bucket type")
		errs = append(errs, parseAutoCompaction(params, &res.settings)...)
	case isMemcached:
		if isNew {
			res.compressionMode = mock.CompressionModeOff
		}
		if _, ok := get("replicaNumber"); ok {
			fail("replicaNumber", "replicaNumber is not valid for memcached buckets")
		}
		quotaSizeError(64)
	default:
		errs = append(errs, parsePurgeInterval(params, isEphemeral, isNew, &res.settings)...)
		errs = append(errs, parseConflictResolution(params, isNew, &res.settings)...)

		if value, ok := get("replicaNumber"); ok {
			n, status := parseValidateInteger(value, true, bigInt(0), bigInt(3))
			switch status {
			case numberInvalid:
				fail("replicaNumber", "The replica number must be specified and must be a non-negative integer.")
			case numberTooSmall:
				fail("replicaNumber", "The replica number cannot be negative.")
			case numberTooLarge:
				fail("replicaNumber", "Replica number larger than 3 is not supported.")
			default:
				res.numReplicas = uint(n.Uint64())
				res.numReplicasSet = true
			}
		} else if isNew {
			// ns_bucket:new_bucket_default_params
			res.numReplicas = 1
		}
		if isNew {
			replicaIndex, ok := get("replicaIndex")
			if isEphemeral {
				if ok {
					fail("replicaIndex", "replicaIndex not supported for ephemeral buckets")
				}
			} else {
				if !ok {
					replicaIndex = "1"
					if res.numReplicasSet && res.numReplicas == 0 {
						replicaIndex = "0"
					}
				}
				switch replicaIndex {
				case "0":
					res.replicaIndexEnabled = false
				case "1":
					res.replicaIndexEnabled = true
				default:
					fail("replicaIndex", "replicaIndex can only be 1 or 0")
				}
			}
		}
		if value, ok := get("threadsNumber"); ok || isNew {
			if !ok {
				value = "3"
			}
			n, status := parseValidateInteger(value, true, bigInt(2), bigInt(8))
			switch status {
			case numberInvalid:
				fail("threadsNumber", "The number of threads must be an integer between 2 and 8")
			case numberTooSmall:
				fail("threadsNumber", "The number of threads can't be less than 2")
			case numberTooLarge:
				fail("threadsNumber", "The number of threads can't be greater than 8")
			default:
				res.settings.NumThreads = json.Number(n.String())
			}
		}
		parseEvictionPolicy(params, isEphemeral, isNew, &res.settings, fail)
		quotaSizeError(100)
		if isNew && !isEphemeral {
			backend, ok := get("storageBackend")
			switch {
			case !ok, backend == "couchstore":
				res.settings.StorageBackend = "couchstore"
			case backend == "magma":
				fail("storageBackend", "Magma is supported only in developer preview mode")
			default:
				fail("storage_mode", "storage backend must be couchstore or magma")
			}
		}
		if value, ok := get("durabilityMinLevel"); ok || isNew {
			if !ok {
				value = "none"
			}
			valid := value == "none" || value == "majority"
			msg := "Durability minimum level must be either 'none' or 'majority' for ephemeral buckets"
			if !isEphemeral {
				valid = valid || value == "majorityAndPersistActive" || value == "persistToMajority"
				msg = "Durability minimum level must be one of 'none', 'majority', " +
					"'majorityAndPersistActive', or 'persistToMajority'"
			}
			if valid {
				res.settings.DurabilityMinLevel = value
				res.durabilitySet = true
			} else {
				fail("durability_min_level", msg)
			}
		}
		for _, key := range []string{"pitrEnabled", "pitrGranularity", "pitrMaxHistoryAge"} {
			if _, ok := get(key); ok {
				fail(key, "Point in time recovery is only supported in developer preview mode")
			}
		}
		if _, ok := get("fragmentationPercentage"); ok {
			backend, _ := get("storageBackend")
			if isNew && backend == "magma" {
				parseFragPercent(params, fail)
			} else {
				fail("fragmentationPercentage", "Fragmentation percentage is only used with Magma")
			}
		}
		if value, ok := get("maxTTL"); ok || isNew {
			if !ok {
				value = "0"
			}
			n, status := parseValidateInteger(value, true, bigInt(0), bigInt(math.MaxInt32))
			if status != numberOK {
				fail("maxTTL", fmt.Sprintf("Max TTL must be an integer between 0 and %d", math.MaxInt32))
			} else {
				res.maxTTL = uint32(n.Uint64())
			}
		}
		if value, ok := get("compressionMode"); ok || isNew {
			if !ok {
				value = "passive"
			}
			switch value {
			case "off", "passive", "active":
				res.compressionMode = mock.CompressionMode(value)
			default:
				fail("compressionMode", "compressionMode can be set to 'off', 'passive' or 'active'")
			}
		}
		errs = append(errs, parseAutoCompaction(params, &res.settings)...)

		// additional_bucket_params_validation takes each value from the
		// parameters, or else from the bucket being updated; a level that
		// failed on create is undefined, which is not none.
		if res.numReplicas == 3 && (res.numReplicasSet || !isNew) &&
			(!res.durabilitySet && isNew || res.settings.DurabilityMinLevel != "none") {
			fail("durability_min_level", "Durability minimum level cannot be specified with 3 replicas")
		}
	}

	res.summarise(current, configBucket, cluster)
	var ramErrors []formParam
	switch {
	case res.ramSummary.Free.Sign() < 0:
		ramErrors = append(ramErrors, formParam{"ramQuota", "RAM quota specified is too large to be provisioned into this cluster."})
	case res.ramSummary.ThisAlloc.Cmp(res.ramSummary.ThisUsed) < 0:
		ramErrors = append(ramErrors, formParam{"ramQuota", "RAM quota cannot be set below current usage."})
	}
	res.errors = append(ramErrors, errs...)
	return res
}

// summarise builds the RAM and disk summaries of interpret_ram_quota and
// interpret_hdd_quota.
func (res *bucketParams) summarise(current, configBucket mock.Bucket, cluster mock.Cluster) {
	totals := getClusterStorageTotals(cluster)
	nodes := int64(totals.nodesCount)
	if configBucket != nil {
		nodes = int64(len(bucketServers(configBucket)))
	}
	ramQuota := new(big.Int)
	if res.ramQuotaSet {
		ramQuota.Set(res.ramQuotaBytes)
	}
	bigNodes := bigInt(nodes)
	perNodes := func(n uint64) *big.Int {
		return new(big.Int).Mul(new(big.Int).SetUint64(n), bigNodes)
	}
	otherBuckets := perNodes(totals.ram.QuotaUsedPerNode)
	otherBuckets.Add(otherBuckets, new(big.Int).Mul(res.otherBucketsRAM, bigNodes))
	thisUsedRAM, thisUsedDisk := new(big.Int), int64(0)
	if current != nil {
		otherBuckets.Sub(otherBuckets, perNodes(current.RamQuota()))
		usage := nodeBucketUsage(current)
		thisUsedRAM = perNodes(usage.mem)
		thisUsedDisk = int64(usage.disk) * nodes
	}
	total := perNodes(totals.ram.QuotaTotalPerNode)
	thisAlloc := new(big.Int).Mul(ramQuota, bigNodes)
	free := new(big.Int).Sub(total, otherBuckets)
	res.ramSummary = ramSummary{
		Total:        total,
		OtherBuckets: otherBuckets,
		NodesCount:   bigNodes,
		PerNodeMegs:  new(big.Int).Quo(ramQuota, bigInt(mib)),
		ThisAlloc:    thisAlloc,
		ThisUsed:     thisUsedRAM,
		Free:         free.Sub(free, thisAlloc),
	}
	otherData := int64(totals.hdd.Used) - int64(totals.hdd.UsedByData)
	otherDiskBuckets := int64(totals.hdd.UsedByData) - thisUsedDisk
	res.hddSummary = hddSummary{
		Total:        int64(totals.hdd.Total),
		OtherData:    otherData,
		OtherBuckets: otherDiskBuckets,
		ThisUsed:     thisUsedDisk,
		Free:         int64(totals.hdd.Total) - otherData - otherDiskBuckets,
	}
}

func parseEvictionPolicy(params []formParam, isEphemeral, isNew bool, settings *mock.BucketSettings, fail func(string, string)) {
	value, ok := paramValue(params, "evictionPolicy")
	switch {
	case isEphemeral && !isNew:
		if ok && value != settings.EvictionPolicy {
			fail("evictionPolicy", "Eviction policy cannot be updated for ephemeral buckets")
		}
	case isEphemeral:
		if !ok {
			value = "noEviction"
		}
		if value != "noEviction" && value != "nruEviction" {
			fail("evictionPolicy", "Eviction policy must be either 'noEviction' or 'nruEviction' for ephemeral buckets")
			return
		}
		settings.EvictionPolicy = value
	case ok || isNew:
		if !ok {
			value = "valueOnly"
		}
		if value != "valueOnly" && value != "fullEviction" {
			fail("evictionPolicy", "Eviction policy must be either 'valueOnly' or 'fullEviction' for couchbase buckets")
			return
		}
		settings.EvictionPolicy = value
	}
}

func parseFragPercent(params []formParam, fail func(string, string)) {
	value, _ := paramValue(params, "fragmentationPercentage")
	if _, status := parseValidateInteger(value, true, bigInt(10), bigInt(100)); status != numberOK {
		fail("fragmentationPercentage", "Fragmentation percentage must be between 10 and 100, inclusive")
	}
}

// parseBooleanField is menelaus_util:parse_validate_boolean_field.
func parseBooleanField(params []formParam, key string) (value, present bool, errs []formParam) {
	v, ok := paramValue(params, key)
	switch {
	case !ok:
		return false, false, nil
	case v == "true":
		return true, true, nil
	case v == "false":
		return false, true, nil
	}
	return false, true, []formParam{{key, key + " is invalid"}}
}

// parsePurgeInterval is validate_bucket_purge_interval.
func parsePurgeInterval(params []formParam, isEphemeral, isNew bool, settings *mock.BucketSettings) []formParam {
	if isEphemeral {
		if _, ok := paramValue(params, "autoCompactionDefined"); ok {
			return []formParam{{"autoCompactionDefined", "autoCompactionDefined must not be set for ephemeral buckets"}}
		}
		return parsePurgeIntervalValue(params, 0.0007, ephemeralPurgeMin, settings)
	}
	defined, present, errs := parseBooleanField(params, "autoCompactionDefined")
	switch {
	case !present:
		return nil
	case errs != nil:
		return errs
	case !defined:
		settings.PurgeInterval = ""
		return nil
	}
	return parsePurgeIntervalValue(params, 0.04, persistentPurgeMin, settings)
}

func parsePurgeIntervalValue(params []formParam, min float64, minText string, settings *mock.BucketSettings) []formParam {
	value, ok := paramValue(params, "purgeInterval")
	if !ok {
		return nil
	}
	n, status := parseValidateFloat(value, true, min, 60)
	switch status {
	case numberInvalid:
		return []formParam{{"purgeInterval", "metadata purge interval must be a number"}}
	case numberTooSmall:
		return []formParam{{"purgeInterval", "metadata purge interval is too small. Allowed range is " + minText + " - 60"}}
	case numberTooLarge:
		return []formParam{{"purgeInterval", "metadata purge interval is too large. Allowed range is " + minText + " - 60"}}
	}
	settings.PurgeInterval = n
	return nil
}

// parseConflictResolution is get_conflict_resolution_type_and_thresholds.
func parseConflictResolution(params []formParam, isNew bool, settings *mock.BucketSettings) []formParam {
	value, ok := paramValue(params, "conflictResolutionType")
	if !isNew {
		if ok {
			return []formParam{{"conflictResolutionType", "Conflict resolution type not allowed in update bucket"}}
		}
		if settings.ConflictResolutionType != "lww" {
			return nil
		}
		return parseDriftThresholds(params, isNew, settings)
	}
	switch {
	case !ok, value == "seqno":
		settings.ConflictResolutionType = "seqno"
		return nil
	case value == "lww":
		settings.ConflictResolutionType = "lww"
		return parseDriftThresholds(params, isNew, settings)
	case value == "custom":
		return []formParam{{"conflictResolutionType", "Conflict resolution type 'custom' is supported only with developer preview enabled"}}
	}
	return []formParam{{"conflictResolutionType", "Conflict resolution type must be 'seqno' or 'lww' or 'custom'"}}
}

func parseDriftThresholds(params []formParam, isNew bool, settings *mock.BucketSettings) []formParam {
	var errs []formParam
	for _, t := range []struct {
		key, name string
		dst       *json.Number
	}{
		{"driftAheadThresholdMs", "ahead", &settings.DriftAheadThresholdMs},
		{"driftBehindThresholdMs", "behind", &settings.DriftBehindThresholdMs},
	} {
		value, ok := paramValue(params, t.key)
		if !ok && !isNew {
			continue
		}
		if !ok {
			value = "5000"
		}
		n, status := parseValidateInteger(value, true, bigInt(100), nil)
		switch status {
		case numberInvalid:
			errs = append(errs, formParam{t.key, "The drift " + t.name + " threshold must be an integer not less than 100ms"})
		case numberTooSmall:
			errs = append(errs, formParam{t.key, "The drift " + t.name + " threshold can't be less than 100ms"})
		default:
			*t.dst = json.Number(n.String())
		}
	}
	return errs
}

// parseAutoCompaction is validate_bucket_auto_compaction_settings with
// menelaus_web_autocompaction:parse_validate_settings.
func parseAutoCompaction(params []formParam, settings *mock.BucketSettings) []formParam {
	defined, present, errs := parseBooleanField(params, "autoCompactionDefined")
	switch {
	case !present:
		return nil
	case errs != nil:
		return errs
	case !defined:
		settings.AutoCompaction = nil
		return nil
	}

	numberField := func(key, human string, min int64, max *big.Int, dst **json.Number) []formParam {
		value, ok := paramValue(params, key)
		if !ok {
			return nil
		}
		n, status := parseValidateInteger(value, true, bigInt(min), max)
		maxText := "infinity"
		if max != nil {
			maxText = max.String()
		}
		switch status {
		case numberInvalid:
			return []formParam{{key, human + " must be an integer"}}
		case numberTooSmall:
			return []formParam{{key, fmt.Sprintf("%s is too small. Allowed range is %d - %s", human, min, maxText)}}
		case numberTooLarge:
			return []formParam{{key, fmt.Sprintf("%s is too large. Allowed range is %d - %s", human, min, maxText)}}
		}
		v := json.Number(n.String())
		*dst = &v
		return nil
	}

	ac := &mock.AutoCompactionSettings{}
	var all []formParam
	all = append(all, numberField("databaseFragmentationThreshold[percentage]", "database fragmentation", 2, bigInt(100), &ac.DatabaseFragmentationPercentage)...)
	all = append(all, numberField("viewFragmentationThreshold[percentage]", "view fragmentation", 2, bigInt(100), &ac.ViewFragmentationPercentage)...)

	parallel, present, perrs := parseBooleanField(params, "parallelDBAndViewCompaction")
	switch {
	case !present:
		all = append(all, formParam{"parallelDBAndViewCompaction", "parallelDBAndViewCompaction is missing"})
	case perrs != nil:
		all = append(all, perrs...)
	default:
		ac.ParallelDBAndViewCompaction = parallel
	}

	// parse_and_validate_time_interval reports only the errors when any
	// field fails.
	period := &mock.AllowedTimePeriod{}
	var periodErrs []formParam
	var periodSet bool
	for _, f := range []struct {
		key, human string
		max        int64
		dst        *json.Number
	}{
		{"allowedTimePeriod[fromHour]", "from hour", 23, &period.FromHour},
		{"allowedTimePeriod[toHour]", "to hour", 23, &period.ToHour},
		{"allowedTimePeriod[fromMinute]", "from minute", 59, &period.FromMinute},
		{"allowedTimePeriod[toMinute]", "to minute", 59, &period.ToMinute},
	} {
		var v *json.Number
		e := numberField(f.key, f.human, 0, bigInt(f.max), &v)
		periodErrs = append(periodErrs, e...)
		if v != nil {
			*f.dst = *v
			periodSet = true
		}
	}
	abort, abortPresent, aerrs := parseBooleanField(params, "allowedTimePeriod[abortOutside]")
	periodErrs = append(periodErrs, aerrs...)
	if abortPresent && aerrs == nil {
		period.AbortOutside = abort
		periodSet = true
	}
	all = append(all, periodErrs...)
	if periodSet && periodErrs == nil {
		ac.AllowedTimePeriod = period
	}

	all = append(all, numberField("databaseFragmentationThreshold[size]", "database fragmentation size", 1, nil, &ac.DatabaseFragmentationSize)...)
	all = append(all, numberField("viewFragmentationThreshold[size]", "view fragmentation size", 1, nil, &ac.ViewFragmentationSize)...)

	var unsupported []string
	for _, key := range []string{"databaseFragmentationThreshold", "viewFragmentationThreshold"} {
		if _, ok := paramValue(params, key); ok {
			unsupported = append(unsupported, key)
		}
	}
	if unsupported != nil {
		all = append(all, formParam{"_", "Got unsupported fields: " + strings.Join(unsupported, " and ")})
	}
	if all != nil {
		return all
	}
	settings.AutoCompaction = ac
	return nil
}

// validateBucketName follows menelaus_web_buckets:validate_bucket_name for
// a new bucket.
func validateBucketName(name string, ok bool, cluster mock.Cluster) string {
	switch {
	case !ok:
		return "Bucket name needs to be specified"
	case name == "":
		return "Bucket name cannot be empty"
	case len(name) > 100:
		return "Bucket name cannot exceed 100 characters"
	case name[0] == '.':
		return "Bucket name cannot start with dot."
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' ||
			c == '.' || c == '%' || c == '_' || c == '-') {
			return "Bucket name can only contain characters in range A-Z, a-z, 0-9 as well as " +
				"underscore, period, dash & percent. Consult the documentation."
		}
	}
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "_users.couch.") || strings.HasPrefix(lower, "_replicator.couch.") {
		return "This name is reserved for the internal use."
	}
	for _, bucket := range cluster.GetAllBuckets() {
		if strings.EqualFold(bucket.Name(), name) {
			return "Bucket with given name already exists"
		}
	}
	return ""
}

// bucketParamsResponse answers a bucket create or update whose parameters
// failed validation, or that only validates them, as
// menelaus_web_buckets does, with the RAM and disk summaries; it returns
// nil when the request is to be carried out. ns_server adds its warnings
// about replicaNumber to the errors unless ignore_warnings=1 is in the
// query.
func bucketParamsResponse(req *mock.HTTPRequest, res bucketParams, current mock.Bucket, cluster mock.Cluster) *mock.HTTPResponse {
	query := parseQS(req.URL.RawQuery)
	justValidate, _ := paramValue(query, "just_validate")
	ignoreWarnings, _ := paramValue(query, "ignore_warnings")
	if justValidate != "1" && len(res.errors) == 0 {
		return nil
	}

	errs := res.errors
	if ignoreWarnings != "1" && res.numReplicasSet {
		var warnings []string
		if uint64(res.numReplicas) >= getClusterStorageTotals(cluster).nodesCount {
			warnings = append(warnings, "you do not have enough data servers to support this number of replicas")
		}
		if current != nil && current.NumReplicas() != res.numReplicas {
			warnings = append(warnings, "changing replica number may require rebalance")
		}
		if len(warnings) > 0 {
			errs = append(errs, formParam{"replicaNumber", "Warning: " + strings.Join(warnings, " and ") + "."})
		}
	}

	statusCode := 400
	if len(res.errors) == 0 {
		switch {
		case ignoreWarnings == "1", len(errs) == 0 && current == nil:
			statusCode = 200
		case len(errs) == 0:
			statusCode = 202
		}
	}
	var body bytes.Buffer
	body.WriteString(`{"errors":{`)
	for i, e := range errs {
		if i > 0 {
			body.WriteByte(',')
		}
		body.WriteString(erlJSONString(e.key))
		body.WriteByte(':')
		body.WriteString(erlJSONString(e.value))
	}
	body.WriteString(`},"summaries":{"ramSummary":`)
	ram, _ := json.Marshal(res.ramSummary)
	body.Write(ram)
	body.WriteString(`,"hddSummary":`)
	hdd, _ := json.Marshal(res.hddSummary)
	body.Write(hdd)
	body.WriteString(`}}`)
	return &mock.HTTPResponse{
		StatusCode: statusCode,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       bytes.NewReader(body.Bytes()),
	}
}

// mochiwebQuotePlus is mochiweb_util:quote_plus.
func mochiwebQuotePlus(s string) string {
	const hexDigits = "0123456789ABCDEF"
	var out strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '.', c == '-', c == '~', c == '_':
			out.WriteByte(c)
		case c == ' ':
			out.WriteByte('+')
		default:
			out.WriteByte('%')
			out.WriteByte(hexDigits[c>>4])
			out.WriteByte(hexDigits[c&0xf])
		}
	}
	return out.String()
}
