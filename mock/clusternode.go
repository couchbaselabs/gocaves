package mock

import "log"

// ServiceType represents the various service types.
type ServiceType uint

// This represents the various service types that a particular
// node could have enabled.
const (
	ServiceTypeMgmt      = ServiceType(1)
	ServiceTypeKeyValue  = ServiceType(2)
	ServiceTypeViews     = ServiceType(3)
	ServiceTypeQuery     = ServiceType(4)
	ServiceTypeSearch    = ServiceType(5)
	ServiceTypeAnalytics = ServiceType(6)
)

func serviceTypeListContains(list []ServiceType, service ServiceType) bool {
	// An empty list acts like a completely full list
	if len(list) == 0 {
		return true
	}

	// Check if we actually have the service in the list
	for _, listSvc := range list {
		if listSvc == service {
			return true
		}
	}
	return false
}

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
