package mockimpl

import (
	"encoding/json"
	"fmt"
)

// GetConfig returns the config data for this node.
func (n *ClusterNode) GetConfig(reqNode *ClusterNode, forBucket *Bucket) []byte {
	config := make(map[string]interface{})

	// TODO(brett19): Add the right handling here for TLS.
	if forBucket != nil {
		// This is inexplicably URL encoded for god knows what reason
		config["couchApiBase"] = fmt.Sprintf("http://%s:%d/%s%%2B%s",
			n.viewService.Hostname(), n.viewService.ListenPort(), forBucket.Name(), forBucket.ID())
		config["couchApiBaseHTTPS"] = fmt.Sprintf("http://%s:%d/%s%%2B%s",
			"invalid-address", 0, forBucket.Name(), forBucket.ID())
	} else {
		config["couchApiBase"] = fmt.Sprintf("http://%s:%d/",
			n.viewService.Hostname(), n.viewService.ListenPort())
		config["couchApiBaseHTTPS"] = fmt.Sprintf("http://%s:%d/",
			"invalid-address", 0)
	}

	// TODO(brett19): Generate something reasonable for the otpNode field
	config["otpNode"] = "ns_NOPE@cb.local"
	config["thisNode"] = n == reqNode
	config["hostname"] = fmt.Sprintf("%s:%d", n.mgmtService.Hostname(), n.mgmtService.ListenPort())
	config["configuredHostname"] = fmt.Sprintf("%s:%d", n.mgmtService.Hostname(), n.mgmtService.ListenPort())
	config["nodeUUID"] = n.ID()
	config["recoveryType"] = "none"

	if forBucket != nil {
		config["replication"] = 0
	}

	servicePorts := map[string]interface{}{
		"distTCP": 32767,
		"distTLS": 32767,
	}

	servicesList := []string{
		"index",
	}

	if n.kvService != nil {
		servicesList = append(servicesList, "kv")

		servicePorts["direct"] = n.kvService.ListenPort()
	}

	if n.mgmtService != nil {
		// TODO(brett19): Add SSL support for mgmt here
		servicePorts["httpsMgmt"] = 32767
	}

	if n.viewService != nil {
		// TODO(brett19): Add SSL support for views here
		servicePorts["httpsCAPI"] = 32767
	}

	if n.queryService != nil {
		servicesList = append(servicesList, "n1ql")
	}

	if n.analyticsService != nil {
		servicesList = append(servicesList, "cbas")
	}

	config["ports"] = servicePorts
	config["services"] = servicesList
	config["nodeEncryption"] = false
	config["addressFamily"] = "inet"
	config["externalListeners"] = []interface{}{
		map[string]interface{}{
			"afamily":        "inet",
			"nodeEncryption": false,
		},
		map[string]interface{}{
			"afamily":        "inet6",
			"nodeEncryption": false,
		},
	}

	config["clusterCompatibility"] = 458752
	config["version"] = "7.0.0-3016-enterprise"
	config["os"] = "x86_64-unknown-linux-gnu"
	config["cpuCount"] = 24

	config["clusterMembership"] = "active"
	config["status"] = "healthy"
	config["uptime"] = "383443"
	config["memoryTotal"] = 49093763072
	config["memoryFree"] = 44611338240
	config["mcdMemoryReserved"] = 37455
	config["mcdMemoryAllocated"] = 37455

	config["systemStats"] = map[string]interface{}{
		"cpu_utilization_rate": 1.386554621848739,
		"cpu_stolen_rate":      0.1260504201680672,
		"swap_total":           2046816256,
		"swap_used":            237715456,
		"mem_total":            49093763072,
		"mem_free":             44611338240,
		"mem_limit":            49093763072,
		"cpu_cores_available":  24,
		"allocstall":           0,
	}

	config["interestingStats"] = map[string]interface{}{
		"cmd_get":                      0,
		"couch_docs_actual_disk_size":  4277712,
		"couch_docs_data_size":         4249600,
		"couch_spatial_data_size":      0,
		"couch_spatial_disk_size":      0,
		"couch_views_actual_disk_size": 0,
		"couch_views_data_size":        0,
		"curr_items":                   0,
		"curr_items_tot":               0,
		"ep_bg_fetched":                0,
		"get_hits":                     0,
		"mem_used":                     38148176,
		"ops":                          0,
		"vb_active_num_non_resident":   0,
		"vb_replica_curr_items":        0,
	}

	configBytes, _ := json.Marshal(config)
	return configBytes
}

// GetTerseConfig returns the mini config data for this node.
func (n *ClusterNode) GetTerseConfig(reqNode *ClusterNode, forBucket *Bucket) []byte {
	config := make(map[string]interface{})

	// TODO(brett19): Add the right stuff here for views.
	if forBucket != nil {
		// This is inexplicably URL encoded for god knows what reason
		config["couchApiBase"] = fmt.Sprintf("http://%s:%d/%s%%2B%s",
			"invalid-address", 0, forBucket.Name(), forBucket.ID())
	} else {
		config["couchApiBase"] = fmt.Sprintf("http://%s:%d/",
			"invalid-address", 0)
	}

	config["hostname"] = fmt.Sprintf("%s:%d", n.mgmtService.Hostname(), n.mgmtService.ListenPort())

	servicePorts := map[string]interface{}{}

	if n.kvService != nil {
		servicePorts["direct"] = n.kvService.ListenPort()
	}

	config["ports"] = servicePorts

	configBytes, _ := json.Marshal(config)
	return configBytes
}

// GetExtConfig returns the extended config data for this node.
func (n *ClusterNode) GetExtConfig(reqNode *ClusterNode, forBucket *Bucket) []byte {
	config := make(map[string]interface{})

	servicePorts := map[string]interface{}{
		// We don't actually support these, so we give invalid ports
		"indexAdmin":         32767,
		"indexScan":          32767,
		"indexHttp":          32767,
		"indexHttps":         32767,
		"indexStreamInit":    32767,
		"indexStreamCatchup": 32767,
		"indexStreamMaint":   32767,
		"projector":          32767,
	}

	if n.kvService != nil {
		servicePorts["kv"] = n.kvService.ListenPort()

		// TODO(brett19): Add SSL support for kv service here
		servicePorts["kvSSL"] = 32767
	}

	if n.mgmtService != nil {
		servicePorts["mgmt"] = n.mgmtService.ListenPort()

		// TODO(brett19): Add SSL support for mgmt service here
		servicePorts["mgmtSSL"] = 32767
	}

	if n.viewService != nil {
		servicePorts["capi"] = n.viewService.ListenPort()

		// TODO(brett19): Add SSL support for views service here
		servicePorts["capiSSL"] = 32767
	}

	if n.queryService != nil {
		servicePorts["n1ql"] = n.queryService.ListenPort()

		// TODO(brett19): Add SSL support for n1ql service here
		servicePorts["n1qlSSL"] = 32767
	}

	if n.analyticsService != nil {
		servicePorts["cbas"] = n.analyticsService.ListenPort()

		// TODO(brett19): Add SSL support for analytics service here
		servicePorts["cbasSSL"] = 32767
	}

	config["services"] = servicePorts
	config["thisNode"] = n == reqNode

	configBytes, _ := json.Marshal(config)
	return configBytes
}
