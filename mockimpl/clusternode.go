package mockimpl

import (
	"log"

	"github.com/google/uuid"
)

// NewNodeOptions allows the specification of initial options for a new node.
type NewNodeOptions struct {
	EnabledServices []ServiceType
}

// ClusterNode specifies a node within a cluster instance.
type ClusterNode struct {
	cluster *Cluster
	id      string

	kvService        *KvService
	mgmtService      *MgmtService
	viewService      *ViewService
	queryService     *QueryService
	searchService    *SearchService
	analyticsService *AnalyticsService
}

// NewClusterNode creates a new ClusterNode instance
func NewClusterNode(parent *Cluster, opts NewNodeOptions) (*ClusterNode, error) {
	node := &ClusterNode{
		id:      uuid.New().String(),
		cluster: parent,
	}

	if serviceTypeListContains(opts.EnabledServices, ServiceTypeKeyValue) {
		kvService, err := newKvService(node, newKvServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start kv service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.kvService = kvService
	}

	if serviceTypeListContains(opts.EnabledServices, ServiceTypeMgmt) {
		mgmtService, err := newMgmtService(node, newMgmtServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start mgmt service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.mgmtService = mgmtService
	}

	if serviceTypeListContains(opts.EnabledServices, ServiceTypeViews) {
		viewService, err := newViewService(node, newViewServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start view service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.viewService = viewService
	}

	if serviceTypeListContains(opts.EnabledServices, ServiceTypeQuery) {
		queryService, err := newQueryService(node, newQueryServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start query service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.queryService = queryService
	}

	if serviceTypeListContains(opts.EnabledServices, ServiceTypeSearch) {
		searchService, err := newSearchService(node, newSearchServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start search service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.searchService = searchService
	}

	if serviceTypeListContains(opts.EnabledServices, ServiceTypeAnalytics) {
		analyticsService, err := newAnalyticsService(node, newAnalyticsServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start analytics service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.analyticsService = analyticsService
	}

	log.Printf("new cluster node created")
	return node, nil
}

// ID returns the uuid of this node.
func (n *ClusterNode) ID() string {
	return n.id
}

// Cluster returns the Cluster this node is part of.
func (n *ClusterNode) Cluster() *Cluster {
	return n.cluster
}

func (n *ClusterNode) cleanup() {
	if n.kvService != nil {
		n.kvService.Close()
		n.kvService = nil
	}
}
