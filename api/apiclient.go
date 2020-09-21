package api

import (
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
	encoder *json.Encoder
	decoder *json.Decoder
	closeCh chan struct{}
}

func newServerClient(conn net.Conn, handler HandlerFunc) (*Client, error) {
	cli := &Client{
		conn:    conn,
		handler: handler,
		decoder: json.NewDecoder(conn),
		encoder: json.NewEncoder(conn),
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
		decoder: json.NewDecoder(conn),
		encoder: json.NewEncoder(conn),
		closeCh: make(chan struct{}, 1),
	}

	helloBytes, _ := encodeCommandPacket(&CmdHello{})
	cli.encoder.Encode(json.RawMessage(helloBytes))

	err = cli.start()
	if err != nil {
		return nil, err
	}

	return cli, nil
}

func (c *Client) start() error {
	go func() {

		for {
			var pkt cmdDecoder
			err := c.decoder.Decode(&pkt)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				log.Printf("failed to decode request: %s", err)
				break
			}

			resCmd := c.handler(pkt.command)

			if resCmd == nil {
				log.Printf("handler returned no response, disconnecting client")
				break
			}

			resBytes, err := encodeCommandPacket(resCmd)
			if err != nil {
				log.Printf("failed to encode response: %s", err)
				break
			}

			err = c.encoder.Encode(json.RawMessage(resBytes))
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				log.Printf("failed to encode response: %s", err)
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
