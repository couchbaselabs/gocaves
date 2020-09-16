package mock

import "log"

// NewNodeOptions allows the specification of initial options for a new node.
type NewNodeOptions struct {
	EnabledServices []ServiceType
}

// ClusterNode specifies a node within a cluster instance.
type ClusterNode struct {
	cluster *Cluster

	kvService        *KvService
	mgmtService      *MgmtService
	viewsService     *ViewsService
	queryService     *QueryService
	searchService    *SearchService
	analyticsService *AnalyticsService
}

// NewClusterNode creates a new ClusterNode instance
func NewClusterNode(parent *Cluster, opts NewNodeOptions) (*ClusterNode, error) {
	node := &ClusterNode{
		cluster: parent,
	}

	if serviceTypeListContains(opts.EnabledServices, ServiceTypeKeyValue) {
		kvService, err := NewKvService(node, NewKvServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start kv service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.kvService = kvService
	}

	if serviceTypeListContains(opts.EnabledServices, ServiceTypeMgmt) {
		mgmtService, err := NewMgmtService(node, NewMgmtServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start mgmt service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.mgmtService = mgmtService
	}

	log.Printf("new cluster node created")
	return node, nil
}

func (n *ClusterNode) cleanup() {
	if n.kvService != nil {
		n.kvService.Close()
		n.kvService = nil
	}
}
