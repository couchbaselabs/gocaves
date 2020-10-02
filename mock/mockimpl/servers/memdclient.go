package servers

import (
	"encoding/binary"
	"net"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/contrib/ctxstore"
)

// MemdClient represents a connected memd client.
type MemdClient struct {
	parent   *MemdServer
	conn     net.Conn
	mconn    *memd.Conn
	ctxStore ctxstore.Store

	closeWaitCh chan struct{}
}

// NewMemdClient allows the creation of a new memd client
func newMemdClient(parent *MemdServer, conn net.Conn) (*MemdClient, error) {
	mconn := memd.NewConn(conn)

	cli := &MemdClient{
		parent: parent,
		conn:   conn,
		mconn:  mconn,
	}

	err := cli.start()
	if err != nil {
		return nil, err
	}

	return cli, nil
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

func (c *MemdClient) start() error {
	c.closeWaitCh = make(chan struct{})

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
	// Close the underlying connection first
	err := c.conn.Close()

	// Then wait for our reader thread to terminate
	<-c.closeWaitCh

	// Finally we can return the error that occurred.
	return err
}

// GetContext gets arbitrary context associated with this client
func (c *MemdClient) GetContext(valuePtr interface{}) {
	c.ctxStore.Get(valuePtr)
}
