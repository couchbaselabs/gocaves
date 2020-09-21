package mockimpl

type uniqueClusterNodeList []*ClusterNode

func (l *uniqueClusterNodeList) GetByID(allNodes []*ClusterNode, id string) int {
	for nodeListIdx, node := range *l {
		if node.id == id {
			return nodeListIdx
		}
	}

	for _, node := range allNodes {
		if node.id == id {
			nodeListIdx := len(*l)
			*l = append(*l, node)
			return nodeListIdx
		}
	}

	return -1
}
