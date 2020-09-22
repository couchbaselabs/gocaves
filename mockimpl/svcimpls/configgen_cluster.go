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
