package mockimpl

import (
	"log"

	"github.com/couchbaselabs/gocaves/mock"
	"github.com/google/uuid"
)

// clusterNodeInst specifies a node within a cluster instance.
type clusterNodeInst struct {
	cluster         *clusterInst
	enabledFeatures []mock.ClusterNodeFeature
	id              string
	errMap          *mock.ErrorMap
	hostname        string

	kvService        *kvService
	mgmtService      *mgmtService
	viewService      *viewService
	queryService     *queryService
	searchService    *searchService
	analyticsService *analyticsService
}

func validateFeatures([]mock.ClusterNodeFeature) error {
	// This function is designed to avoid having two features enabled which
	// cause conflicting behaviours or unimplementable combinations.

	return nil
}

// newClusterNode creates a new ClusterNode instance
func newClusterNode(parent *clusterInst, opts mock.NewNodeOptions) (*clusterNodeInst, error) {
	err := validateFeatures(opts.Features)
	if err != nil {
		return nil, err
	}

	node := &clusterNodeInst{
		id:              uuid.New().String(),
		enabledFeatures: opts.Features,
		cluster:         parent,
		hostname:        "127.0.0.1",
	}

	node.errMap, err = mock.NewErrorMap()
	if err != nil {
		log.Printf("cluster node failed to load error map: %s", err)
		node.cleanup()
		return nil, err
	}

	if serviceTypeListContains(opts.Services, mock.ServiceTypeKeyValue) {
		kvService, err := newKvService(node, newKvServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start kv service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.kvService = kvService
	}

	if serviceTypeListContains(opts.Services, mock.ServiceTypeMgmt) {
		mgmtService, err := newMgmtService(node, newMgmtServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start mgmt service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.mgmtService = mgmtService
	}

	if serviceTypeListContains(opts.Services, mock.ServiceTypeViews) {
		viewService, err := newViewService(node, newViewServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start view service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.viewService = viewService
	}

	if serviceTypeListContains(opts.Services, mock.ServiceTypeQuery) {
		queryService, err := newQueryService(node, newQueryServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start query service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.queryService = queryService
	}

	if serviceTypeListContains(opts.Services, mock.ServiceTypeSearch) {
		searchService, err := newSearchService(node, newSearchServiceOptions{})
		if err != nil {
			log.Printf("cluster node failed to start search service: %s", err)
			node.cleanup()
			return nil, err
		}

		node.searchService = searchService
	}

	if serviceTypeListContains(opts.Services, mock.ServiceTypeAnalytics) {
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

// HasFeature will indicate whether this cluster node has a specific feature enabled.
func (n *clusterNodeInst) HasFeature(feature mock.ClusterNodeFeature) bool {
	return clusterFeatureListContains(n.enabledFeatures, feature)
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
	if n.kvService == nil {
		return nil
	}
	return n.kvService
}

// MgmtService returns the mgmt service for this node.
func (n *clusterNodeInst) MgmtService() mock.MgmtService {
	if n.mgmtService == nil {
		return nil
	}
	return n.mgmtService
}

// ViewService returns the views service for this node.
func (n *clusterNodeInst) ViewService() mock.ViewService {
	if n.viewService == nil {
		return nil
	}
	return n.viewService
}

// QueryService returns the query service for this node.
func (n *clusterNodeInst) QueryService() mock.QueryService {
	if n.queryService == nil {
		return nil
	}
	return n.queryService
}

// SearchService returns the search service for this node.
func (n *clusterNodeInst) SearchService() mock.SearchService {
	if n.searchService == nil {
		return nil
	}
	return n.searchService
}

// AnalyticsService returns the analytics service for this node.
func (n *clusterNodeInst) AnalyticsService() mock.AnalyticsService {
	if n.analyticsService == nil {
		return nil
	}
	return n.analyticsService
}

// ErrorMap returns the error map for this node.
func (n *clusterNodeInst) ErrorMap() *mock.ErrorMap {
	return n.errMap
}

func (n *clusterNodeInst) Hostname() string {
	return n.hostname
}

func (n *clusterNodeInst) cleanup() {
	if n.kvService != nil {
		n.kvService.Close()
		n.kvService = nil
	}
}
