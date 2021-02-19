package svcimpls

import (
	"encoding/json"
	"fmt"

	"github.com/couchbaselabs/gocaves/mock"
)

// GenClusterNodeConfig returns the config data for a cluster node.
func GenClusterNodeConfig(n mock.ClusterNode, reqNode mock.ClusterNode, forBucket mock.Bucket) []byte {
	config := make(map[string]interface{})

	if forBucket != nil {
		// This is inexplicably URL encoded for god knows what reason
		if n.ViewService() != nil && n.ViewService().ListenPort() > 0 {
			config["couchApiBase"] = fmt.Sprintf("http://%s:%d/%s%%2B%s",
				n.ViewService().Hostname(), n.ViewService().ListenPort(), forBucket.Name(), forBucket.ID())
		}
		if n.ViewService() != nil && n.ViewService().ListenPortTLS() > 0 {
			config["couchApiBaseHTTPS"] = fmt.Sprintf("http://%s:%d/%s%%2B%s",
				n.ViewService().Hostname(), n.ViewService().ListenPortTLS(), forBucket.Name(), forBucket.ID())
		}
	} else {
		if n.ViewService() != nil && n.ViewService().ListenPort() > 0 {
			config["couchApiBase"] = fmt.Sprintf("http://%s:%d/",
				n.ViewService().Hostname(), n.ViewService().ListenPort())
		}
		if n.ViewService() != nil && n.ViewService().ListenPortTLS() > 0 {
			config["couchApiBaseHTTPS"] = fmt.Sprintf("http://%s:%d/",
				n.ViewService().Hostname(), n.ViewService().ListenPortTLS())
		}
	}

	// TODO(brett19): Generate something reasonable for the otpNode field
	config["otpNode"] = "ns_NOPE@cb.local"
	config["thisNode"] = n == reqNode
	config["hostname"] = fmt.Sprintf("%s:%d", n.MgmtService().Hostname(), n.MgmtService().ListenPort())
	config["configuredHostname"] = fmt.Sprintf("%s:%d", n.MgmtService().Hostname(), n.MgmtService().ListenPort())
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

	if n.KvService() != nil {
		servicesList = append(servicesList, "kv")

		servicePorts["direct"] = n.KvService().ListenPort()
	}

	if n.MgmtService() != nil {
		servicePorts["httpsMgmt"] = n.MgmtService().ListenPortTLS()
	}

	if n.ViewService() != nil {
		servicePorts["httpsCAPI"] = n.ViewService().ListenPortTLS()
	}

	if n.QueryService() != nil {
		servicesList = append(servicesList, "n1ql")
	}

	if n.AnalyticsService() != nil {
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

// GenTerseClusterNodeConfig returns the terse config data for a cluster node.
func GenTerseClusterNodeConfig(n mock.ClusterNode, reqNode mock.ClusterNode, forBucket mock.Bucket) []byte {
	config := make(map[string]interface{})

	if forBucket != nil {
		// This is inexplicably URL encoded for god knows what reason
		if n.ViewService() != nil && n.ViewService().ListenPort() > 0 {
			config["couchApiBase"] = fmt.Sprintf("http://%s:%d/%s%%2B%s",
				n.ViewService().Hostname(), n.ViewService().ListenPort(), forBucket.Name(), forBucket.ID())
		}
	} else {
		if n.ViewService() != nil && n.ViewService().ListenPort() > 0 {
			config["couchApiBase"] = fmt.Sprintf("http://%s:%d/",
				n.ViewService().Hostname(), n.ViewService().ListenPort())
		}
	}

	config["hostname"] = fmt.Sprintf("%s:%d", n.MgmtService().Hostname(), n.MgmtService().ListenPort())

	servicePorts := map[string]interface{}{}

	if n.KvService() != nil {
		servicePorts["direct"] = n.KvService().ListenPort()
	}

	config["ports"] = servicePorts

	configBytes, _ := json.Marshal(config)
	return configBytes
}

// GenExtClusterNodeConfig returns the extended config data for a cluster node.
func GenExtClusterNodeConfig(n mock.ClusterNode, reqNode mock.ClusterNode, forBucket mock.Bucket) []byte {
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

	if n.KvService() != nil && n.KvService().ListenPort() > 0 {
		servicePorts["kv"] = n.KvService().ListenPort()
	}
	if n.KvService() != nil && n.KvService().ListenPortTLS() > 0 {
		servicePorts["kvSSL"] = n.KvService().ListenPortTLS()
	}

	if n.MgmtService() != nil && n.MgmtService().ListenPort() > 0 {
		servicePorts["mgmt"] = n.MgmtService().ListenPort()
	}
	if n.MgmtService() != nil && n.MgmtService().ListenPortTLS() > 0 {
		servicePorts["mgmtSSL"] = n.MgmtService().ListenPortTLS()
	}

	if n.ViewService() != nil && n.ViewService().ListenPort() > 0 {
		servicePorts["capi"] = n.ViewService().ListenPort()
	}
	if n.ViewService() != nil && n.ViewService().ListenPortTLS() > 0 {
		servicePorts["capiSSL"] = n.ViewService().ListenPortTLS()
	}

	if n.QueryService() != nil && n.QueryService().ListenPort() > 0 {
		servicePorts["n1ql"] = n.QueryService().ListenPort()
	}
	if n.QueryService() != nil && n.QueryService().ListenPortTLS() > 0 {
		servicePorts["n1qlSSL"] = n.QueryService().ListenPortTLS()
	}

	if n.AnalyticsService() != nil && n.AnalyticsService().ListenPort() > 0 {
		servicePorts["cbas"] = n.AnalyticsService().ListenPort()
	}
	if n.AnalyticsService() != nil && n.AnalyticsService().ListenPortTLS() > 0 {
		servicePorts["cbasSSL"] = n.AnalyticsService().ListenPortTLS()
	}

	if n.SearchService() != nil && n.SearchService().ListenPort() > 0 {
		servicePorts["fts"] = n.SearchService().ListenPort()
	}
	if n.SearchService() != nil && n.SearchService().ListenPortTLS() > 0 {
		servicePorts["ftsSSL"] = n.SearchService().ListenPortTLS()
	}

	config["services"] = servicePorts
	config["thisNode"] = n == reqNode

	configBytes, _ := json.Marshal(config)
	return configBytes
}
