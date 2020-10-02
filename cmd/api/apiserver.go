package api

import (
	"fmt"
	"log"
	"net"
)

// HandlerFunc provides an interface for handling and replying to
// messages received via the API server.
type HandlerFunc func(command interface{}) interface{}

// Server represents an instance of the API server.
type Server struct {
	listenPort int
	handler    HandlerFunc
}

// NewServerOptions provides options when creating an API server.
type NewServerOptions struct {
	ListenPort int
	Handler    HandlerFunc
}

// ListenPort returns the port this server is listening on.
func (s *Server) ListenPort() int {
	return s.listenPort
}

// NewServer creates a new API server.
func NewServer(opts NewServerOptions) (*Server, error) {
	srv := &Server{
		listenPort: opts.ListenPort,
		handler:    opts.Handler,
	}

	err := srv.start()
	if err != nil {
		return nil, err
	}

	return srv, nil
}

func (s *Server) start() error {
	lsnr, err := net.Listen("tcp", fmt.Sprintf(":%d", s.listenPort))
	if err != nil {
		return err
	}

	// Save the local listening address
	addr := lsnr.Addr()
	tcpAddr := addr.(*net.TCPAddr)
	s.listenPort = tcpAddr.Port

	log.Printf("starting listener for CAVES server on port %d", s.listenPort)

	go func() {
		for {
			conn, err := lsnr.Accept()
			if err != nil {
				log.Printf("accept failed: %s", err)
				break
			}

			client, err := newServerClient(conn, s.handler)
			if err != nil {
				log.Printf("client start failed: %s", err)
				continue
			}

			log.Printf("new api client connected: %p", client)
		}
	}()

	return nil
}
