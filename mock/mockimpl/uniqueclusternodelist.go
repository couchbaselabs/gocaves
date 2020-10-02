package mockimpl

import "github.com/couchbaselabs/gocaves/mock"

type uniqueClusterNodeList []mock.ClusterNode

func (l *uniqueClusterNodeList) GetByID(allNodes []*clusterNodeInst, id string) int {
	for nodeListIdx, node := range *l {
		if node.ID() == id {
			return nodeListIdx
		}
	}

	for _, node := range allNodes {
		if node.ID() == id {
			nodeListIdx := len(*l)
			*l = append(*l, node)
			return nodeListIdx
		}
	}

	return -1
}
