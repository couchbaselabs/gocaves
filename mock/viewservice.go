package mock

import (
	"github.com/couchbaselabs/gocaves/mock/mockauth"
	"github.com/couchbaselabs/gocaves/mock/mockmr"
)

// ViewService represents a views service running somewhere in the cluster.
type ViewService interface {
	// Node returns the node which owns this service.
	Node() ClusterNode

	// Hostname returns the hostname where this service can be accessed.
	Hostname() string

	// ListenPort returns the port this service is listening on.
	ListenPort() int

	// ListenPortTLS returns the TLS port this service is listening on.
	ListenPortTLS() int

	// Close will shut down this service once it is no longer needed.
	Close() error

	// CheckAuthenticated verifies that the currently authenticated user has the specified permissions.
	CheckAuthenticated(permission mockauth.Permission, bucket, scope, collection string, request *HTTPRequest) bool
}

// ViewIndexManager represents information about the view indexes of a bucket.
type ViewIndexManager interface {
	// UpsertDesignDocument creates or updates a design document.
	UpsertDesignDocument(name string, opts mockmr.UpsertDesignDocumentOptions) error

	// DropDesignDocument removes a design document.
	DropDesignDocument(name string) error

	// GetDesignDocument retrieves a single design document.
	GetDesignDocument(name string) (*mockmr.DesignDocument, error)

	// GetAllDesignDocuments retrieves all design documents.
	GetAllDesignDocuments() []*mockmr.DesignDocument

	// Execute executes a query.
	Execute(opts mockmr.ExecuteOptions) (int, *mockmr.ExecuteResults, error)
}
