package mock

import (
	"net"
)

// KvClient represents a connected kv client.
type KvClient struct {
	kvService *KvService
	conn      net.Conn

	ctxStore ctxStore
}

// NewKvClient allows the creation of a new kv client
func NewKvClient(parent *KvService, conn net.Conn) (*KvClient, error) {
	cli := &KvClient{
		kvService: parent,
		conn:      conn,
	}

	err := cli.start()
	if err != nil {
		return nil, err
	}

	return cli, nil
}

func (c *KvClient) start() error {
	waitCh := make(chan error)

	go func() {

	}()

	err := <-waitCh
	close(waitCh)
	return err
}

// Close will forcefully disconnect a client
func (c *KvClient) Close() error {
	return c.conn.Close()
}

// GetContext fetches a particular structure of data from the KvClient
func (c *KvClient) GetContext(valuePtr interface{}) {
	c.ctxStore.Get(valuePtr)
}
