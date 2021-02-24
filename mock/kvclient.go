package mock

import (
	"net"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/contrib/scramserver"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
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

	// SetAuthenticatedUserName sets the name of the user who is authenticated.
	SetAuthenticatedUserName(userName string)

	// AuthenticatedUserName gets the name of the user who is authenticated.
	AuthenticatedUserName() string

	// CheckAuthenticated verifies that the currently authenticated user has the specified permissions.
	CheckAuthenticated(permission mockauth.Permission, collectionID uint32) bool

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

	// Close attempts to close the connection.
	Close() error
}
