package api

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
)

// Client represents a connected client.
type Client struct {
	conn    net.Conn
	handler HandlerFunc
	reader  *bufio.Reader
	closeCh chan struct{}
}

func newServerClient(conn net.Conn, handler HandlerFunc) (*Client, error) {
	cli := &Client{
		conn:    conn,
		handler: handler,
		reader:  bufio.NewReader(conn),
		closeCh: make(chan struct{}, 1),
	}

	err := cli.start()
	if err != nil {
		return nil, err
	}

	return cli, nil
}

// ConnectAsServerOptions provides options when creating an API client.
type ConnectAsServerOptions struct {
	Address string
	Handler HandlerFunc
}

// ConnectAsServer creates a new API client connection, but acts
// as if we are the server-side.  This is used so the client can
// kill this process automatically.
func ConnectAsServer(opts ConnectAsServerOptions) (*Client, error) {
	conn, err := net.Dial("tcp", opts.Address)
	if err != nil {
		return nil, err
	}

	cli := &Client{
		conn:    conn,
		handler: opts.Handler,
		reader:  bufio.NewReader(conn),
		closeCh: make(chan struct{}, 1),
	}

	err = cli.writePacket(&CmdHello{})
	if err != nil {
		return nil, err
	}

	err = cli.start()
	if err != nil {
		return nil, err
	}

	return cli, nil
}

func (c *Client) readPacket() (interface{}, error) {
	pktBytes, err := c.reader.ReadSlice(0)
	if err != nil {
		return nil, err
	}

	pktBytes = pktBytes[:len(pktBytes)-1]

	var pkt cmdDecoder
	err = json.Unmarshal(pktBytes, &pkt)
	if err != nil {
		return nil, err
	}

	return pkt.command, nil
}

func (c *Client) writePacket(pak interface{}) error {
	pktBytes, err := EncodeCommandPacket(pak)
	if err != nil {
		return err
	}

	pktBytes = append(pktBytes, byte(0))

	_, err = c.conn.Write(pktBytes)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) start() error {
	go func() {

		for {
			pkt, err := c.readPacket()
			if err != nil {
				if errors.Is(err, io.EOF) {
					continue
				}

				log.Printf("failed to read request: %s", err)
				break
			}

			resCmd := c.handler(pkt)

			if resCmd == nil {
				log.Printf("handler returned no response, disconnecting client")
				break
			}

			err = c.writePacket(resCmd)
			if err != nil {
				if errors.Is(err, io.EOF) {
					continue
				}

				log.Printf("failed to write response: %s", err)
				break
			}
		}

		c.conn.Close()

		log.Printf("api client disconnected: %p", c)

		close(c.closeCh)
	}()

	return nil
}

// WaitForClose will wait until this client disconnects
func (c *Client) WaitForClose() {
	<-c.closeCh
}
