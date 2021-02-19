package linkmode

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/couchbaselabs/gocaves/cmd/api"
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

	pktBytes, _ := api.EncodeCommandPacket(&api.CmdHello{})
	pktBytes = append(pktBytes, byte(0))
	_, err = cliConn.Write(pktBytes)
	if err != nil {
		log.Printf("failed to write hello packet to the client: %s", err)
		return
	}

	go func() {
		log.Printf("Starting server to client copying")
		_, err := io.Copy(srvConn, cliConn)
		if err != nil {
			log.Printf("server to client copying errored: %v", err)
		}
		cliConn.Close()
		srvConn.Close()
	}()

	log.Printf("Starting client to server copying")
	_, err = io.Copy(cliConn, srvConn)
	if err != nil {
		log.Printf("client to server copying errored: %v", err)
	}
	cliConn.Close()
	srvConn.Close()
}
