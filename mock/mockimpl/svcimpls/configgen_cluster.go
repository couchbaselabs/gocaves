package svcimpls

import (
	"encoding/json"

	"github.com/couchbaselabs/gocaves/mock"
)

// GenClusterConfig returns the current config for this cluster.
func GenClusterConfig(c mock.Cluster, reqNode mock.ClusterNode) []byte {
	config := make(map[string]interface{})

	config["name"] = "default"

	nodesConfig := make([]interface{}, 0)
	for _, server := range c.Nodes() {
		nodeConfig := GenClusterNodeConfig(server, reqNode, nil)
		nodesConfig = append(nodesConfig, json.RawMessage(nodeConfig))
	}
	config["nodes"] = nodesConfig

	configBytes, _ := json.Marshal(config)
	return configBytes
}

// GenTerseClusterConfig returns the current mini config for this cluster.
func GenTerseClusterConfig(c mock.Cluster, reqNode mock.ClusterNode) []byte {
	config := make(map[string]interface{})

	config["rev"] = c.ConfigRev()

	nodesConfig := make([]interface{}, 0)
	for _, server := range c.Nodes() {
		nodeConfig := GenExtClusterNodeConfig(server, reqNode, nil)
		nodesConfig = append(nodesConfig, json.RawMessage(nodeConfig))
	}
	config["nodesExt"] = nodesConfig

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

	configBytes, _ := json.Marshal(config)
	return configBytes
}
