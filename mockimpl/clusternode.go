package mockimpl

import (
	"log"

	"github.com/couchbaselabs/gocaves/mock"
	"github.com/google/uuid"
)

// clusterNodeInst specifies a node within a cluster instance.
type clusterNodeInst struct {
	cluster *clusterInst
	id      string

	kvService        *kvService
	mgmtService      *mgmtService
	viewService      *viewService
	queryService     *queryService
	searchService    *searchService
	analyticsService *analyticsService
}

// newClusterNode creates a new ClusterNode instance
func newClusterNode(parent *clusterInst, opts mock.NewNodeOptions) (*clusterNodeInst, error) {
	node := &clusterNodeInst{
		id:      uuid.New().String(),
		cluster: parent,
	}

	if serviceTypeListContains(opts.EnabledServices, mock.ServiceTypeKeyValue) {
		kvService, err := newKvService(node, newKvServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start kv service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.kvService = kvService
	}

	if serviceTypeListContains(opts.EnabledServices, mock.ServiceTypeMgmt) {
		mgmtService, err := newMgmtService(node, newMgmtServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start mgmt service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.mgmtService = mgmtService
	}

	if serviceTypeListContains(opts.EnabledServices, mock.ServiceTypeViews) {
		viewService, err := newViewService(node, newViewServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start view service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.viewService = viewService
	}

	if serviceTypeListContains(opts.EnabledServices, mock.ServiceTypeQuery) {
		queryService, err := newQueryService(node, newQueryServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start query service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.queryService = queryService
	}

	if serviceTypeListContains(opts.EnabledServices, mock.ServiceTypeSearch) {
		searchService, err := newSearchService(node, newSearchServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start search service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.searchService = searchService
	}

	if serviceTypeListContains(opts.EnabledServices, mock.ServiceTypeAnalytics) {
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
func (n *clusterNodeInst) ID() string {
	return n.id
}

// Cluster returns the Cluster this node is part of.
func (n *clusterNodeInst) Cluster() mock.Cluster {
	return n.cluster
}

// KvService returns the kv service for this node.
func (n *clusterNodeInst) KvService() mock.KvService {
	return n.kvService
}

// MgmtService returns the mgmt service for this node.
func (n *clusterNodeInst) MgmtService() mock.MgmtService {
	return n.mgmtService
}

// ViewService returns the views service for this node.
func (n *clusterNodeInst) ViewService() mock.ViewService {
	return n.viewService
}

// QueryService returns the query service for this node.
func (n *clusterNodeInst) QueryService() mock.QueryService {
	return n.queryService
}

// SearchService returns the search service for this node.
func (n *clusterNodeInst) SearchService() mock.SearchService {
	return n.analyticsService
}

// AnalyticsService returns the analytics service for this node.
func (n *clusterNodeInst) AnalyticsService() mock.AnalyticsService {
	return n.analyticsService
}

func (n *clusterNodeInst) cleanup() {
	if n.kvService != nil {
		n.kvService.Close()
		n.kvService = nil
	}
}
