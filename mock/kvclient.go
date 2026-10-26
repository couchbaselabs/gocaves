package mock

import (
	"net"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/contrib/scramserver"
)

// KvClient represents all the state about a connected kv client.
type KvClient interface {
	// LocalAddr returns the local address of this client.
	LocalAddr() net.Addr

	// RemoteAddr returns the remote address of this client.
	RemoteAddr() net.Addr

	// IsTLS returns whether this client is connected via TLS
	IsTLS() bool

	// Source returns the KvService which owns this client.
	Source() KvService

	// ScramServer returns a SCRAM server object specific to this user.
	ScramServer() *scramserver.ScramServer

	// SetAuthenticatedUserName sets the name of the user who is authenticated,
	// and marks the client authenticated.
	SetAuthenticatedUserName(userName string)

	// AuthenticatedUserName gets the name of the user who is authenticated,
	// or was until a restart of the authentication.
	AuthenticatedUserName() string

	// RestartAuthentication marks the client not authenticated, as
	// kv_engine's Connection::restartAuthentication does on a SASL_AUTH. The
	// privileges the user has on its bucket stay.
	RestartAuthentication()

	// IsAuthenticated reports whether the client is authenticated.
	IsAuthenticated() bool

	// SetSelectedBucketName sets the currently selected bucket's name.
	SetSelectedBucketName(bucketName string)

	// SelectedBucketName returns the currently selected bucket's name.
	SelectedBucketName() string

	// SelectedBucket returns the currently selected bucket.
	SelectedBucket() Bucket

	// SetFeatures sets the list of support features for this client.
	SetFeatures(features []memd.HelloFeature)

	// HasFeature indicates whether or not this client supports a feature.
	HasFeature(feature memd.HelloFeature) bool

	// WritePacket tries to write data to the underlying connection.
	WritePacket(pak *memd.Packet) error

	// Closed reports whether the connection has been closed or lost.
	Closed() bool

	// RunDetached runs fn on its own goroutine, apart from the connection's
	// dispatcher. It must be called from a request handler, on the dispatcher
	// goroutine. A request on an ordered connection, or a barrier under
	// unordered execution, waits for every fn started before it.
	RunDetached(fn func())

	// Close attempts to close the connection.
	Close() error
}
