package linkmode

import (
	"fmt"
	"io"
	"log"
	"net"
)

// Main wraps the linkmode cmd
type Main struct {
	SdkPort  int
	LinkAddr string
}

// Go starts the app
func (m *Main) Go() {
	srvConn, err := net.Dial("tcp", m.LinkAddr)
	if err != nil {
		log.Printf("failed to connect to caves server: %s", err)
		return
	}

	cliConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", m.SdkPort))
	if err != nil {
		log.Printf("failed to connect to the sdk: %s", err)
		return
	}

	// TODO(brett19): Should probably use the API package to do this...
	cliConn.Write([]byte(`{"type":"hello"}`))

	go func() {
		log.Printf("Starting server to client copying")
		io.Copy(srvConn, cliConn)
		cliConn.Close()
		srvConn.Close()
	}()

	log.Printf("Starting client to server copying")
	io.Copy(cliConn, srvConn)
	cliConn.Close()
	srvConn.Close()
}
