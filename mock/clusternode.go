package mock

// NewNodeOptions allows the specification of initial options for a new node.
type NewNodeOptions struct {
	EnabledServices []ServiceType
}

// ClusterNode specifies a node within a cluster instance.
type ClusterNode interface {
	// ID returns the uuid of this node.
	ID() string

	// Cluster returns the Cluster this node is part of.
	Cluster() Cluster

	// KvService returns the kv service for this node.
	KvService() KvService

	// MgmtService returns the mgmt service for this node.
	MgmtService() MgmtService

	// ViewService returns the views service for this node.
	ViewService() ViewService

	// QueryService returns the query service for this node.
	QueryService() QueryService

	// SearchService returns the search service for this node.
	SearchService() SearchService

	// AnalyticsService returns the analytics service for this node.
	AnalyticsService() AnalyticsService

	// ErrorMap returns the error map for this node.
	ErrorMap() *ErrorMap
}
