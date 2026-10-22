package servers

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/contrib/ctxstore"
)

// MemdClient represents a connected memd client.
type MemdClient struct {
	parent   *MemdServer
	conn     net.Conn
	mconn    *memd.Conn
	ctxStore ctxstore.Store

	closeWaitCh chan struct{}

	lock    sync.Mutex
	started bool
	closed  bool
}

var (
	errClientClosed  = errors.New("memd client closed before it started")
	errClientStarted = errors.New("memd client already started")
)

// NewMemdClient allows the creation of a new memd client
func newMemdClient(parent *MemdServer, conn net.Conn) *MemdClient {
	mconn := memd.NewConn(conn)

	return &MemdClient{
		parent:      parent,
		conn:        conn,
		mconn:       mconn,
		closeWaitCh: make(chan struct{}),
	}
}

// LocalAddr returns the local address of this client.
func (c *MemdClient) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr returns the remote address of this client.
func (c *MemdClient) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// WritePacket writes a packet to the connection.
func (c *MemdClient) WritePacket(pak *memd.Packet) error {
	// In order to support various hello features, we detect when there is a hello response
	// packet sent, and then automatically enable the appropriate protocol features when
	// we do that.
	if pak.Magic == memd.CmdMagicRes && pak.Command == memd.CmdHello {
		numFeatures := len(pak.Value) / 2
		for featureIdx := 0; featureIdx < numFeatures; featureIdx++ {
			featureCodeID := binary.BigEndian.Uint16(pak.Value[featureIdx*2:])
			featureCode := memd.HelloFeature(featureCodeID)
			c.mconn.EnableFeature(featureCode)
		}
	}

	// Actually write the packet.  Note that it is critical that the features we enable above
	// don't actually affect how the HELLO packet is being written.
	return c.mconn.WritePacket(pak)
}

// start reads packets from the connection until it closes. The client must
// be registered with its handlers first, or its first packets have no client
// state to be dispatched to. It fails if the client was closed during
// registration, or was already started.
func (c *MemdClient) start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.closed {
		return errClientClosed
	}
	if c.started {
		return errClientStarted
	}
	c.started = true

	go func() {
		for {
			pak, _, err := c.mconn.ReadPacket()
			if err != nil {
				break
			}

			c.parent.handleClientRequest(c, pak)
		}

		c.parent.handleClientDisconnect(c)

		close(c.closeWaitCh)
	}()

	return nil
}

// Close will forcefully disconnect a client
func (c *MemdClient) Close() error {
	c.lock.Lock()
	c.closed = true
	started := c.started
	c.lock.Unlock()

	// Close the underlying connection first
	err := c.conn.Close()

	// Then wait for our reader thread to terminate. A client closed during
	// registration has no reader to wait for.
	if started {
		<-c.closeWaitCh
	}

	// Finally we can return the error that occurred.
	return err
}

// GetContext gets arbitrary context associated with this client
func (c *MemdClient) GetContext(valuePtr interface{}) {
	c.ctxStore.Get(valuePtr)
}
