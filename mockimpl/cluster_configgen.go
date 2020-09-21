package mockimpl

import "encoding/json"

// GetConfig returns the current config for this cluster.
func (c *Cluster) GetConfig(reqNode *ClusterNode) []byte {
	config := make(map[string]interface{})

	config["name"] = "default"

	nodesConfig := make([]interface{}, 0)
	for _, server := range c.nodes {
		nodeConfig := server.GetConfig(reqNode, nil)
		nodesConfig = append(nodesConfig, json.RawMessage(nodeConfig))
	}
	config["nodes"] = nodesConfig

	configBytes, _ := json.Marshal(config)
	return configBytes
}
