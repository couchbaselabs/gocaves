package mock

// KvService represents an instance of the kv service.
type KvService interface {
	// Node returns the ClusterNode which owns this service.
	Node() ClusterNode

	// Hostname returns the hostname where this service can be accessed.
	Hostname() string

	// ListenPort returns the port this service is listening on.
	ListenPort() int

	// ListenPortTLS returns the TLS port this service is listening on.
	ListenPortTLS() int

	// GetAllClients returns a list of all the clients connected to this service.
	GetAllClients() []KvClient

	// Close will shut down this service once it is no longer needed.
	Close() error
}
