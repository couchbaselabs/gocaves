package mock

// ViewService represents a views service running somewhere in the cluster.
type ViewService interface {
	// Node returns the node which owns this service.
	Node() ClusterNode

	// Hostname returns the hostname where this service can be accessed.
	Hostname() string

	// ListenPort returns the port this service is listening on.
	ListenPort() int

	// Close will shut down this service once it is no longer needed.
	Close() error
}
