package servers

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/couchbase/gocbcore/v9/memd"
)

// MemdServerHandlers provides all the handlers for the memd server
type MemdServerHandlers struct {
	NewClientHandler  func(*MemdClient)
	LostClientHandler func(*MemdClient)
	PacketHandler     func(*MemdClient, *memd.Packet)
}

// MemdServer represents an instance of the memd server.
type MemdServer struct {
	lock       sync.Mutex
	listenPort int
	localAddr  string
	listener   net.Listener
	handlers   MemdServerHandlers
	tlsConfig  *tls.Config

	clients []*MemdClient
}

// NewMemdServerOptions enables the specification of default options for a new memd server.
type NewMemdServerOptions struct {
	TLSConfig  *tls.Config
	Handlers   MemdServerHandlers
	ListenPort int
}

// NewMemdService instantiates a new instance of the memd server.
func NewMemdService(opts NewMemdServerOptions) (*MemdServer, error) {
	svc := &MemdServer{
		handlers:   opts.Handlers,
		tlsConfig:  opts.TLSConfig,
		listenPort: opts.ListenPort,
	}

	err := svc.start()
	if err != nil {
		return nil, err
	}

	return svc, nil
}

// ListenPort returns the port this server is listening on.
func (s *MemdServer) ListenPort() int {
	return s.listenPort
}

func (s *MemdServer) start() error {
	listenAddr := fmt.Sprintf(":%d", s.listenPort)

	var lsnr net.Listener
	var err error
	if s.tlsConfig != nil {
		lsnr, err = tls.Listen("tcp", listenAddr, s.tlsConfig)
	} else {
		lsnr, err = net.Listen("tcp", listenAddr)
	}
	if err != nil {
		if s.tlsConfig != nil {
			log.Printf("failed to start listening for kv (memd) TLS server: %s", err)
		} else {
			log.Printf("failed to start listening for kv (memd) server: %s", err)
		}
		return err
	}

	// Save the local listening address
	addr := lsnr.Addr()
	tcpAddr := addr.(*net.TCPAddr)
	s.listenPort = tcpAddr.Port
	s.localAddr = addr.String()

	if s.tlsConfig != nil {
		log.Printf("starting listener for kv (memd) TLS server on port %d", s.listenPort)
	} else {
		log.Printf("starting listener for kv (memd) server on port %d", s.listenPort)
	}

	go func() {
		for {
			conn, err := lsnr.Accept()
			if err != nil {
				log.Printf("accept failed: %s", err)
				break
			}

			client, err := newMemdClient(s, conn)
			if err != nil {
				log.Printf("failed to create memd client: %s", err)
				break
			}

			s.lock.Lock()

			s.clients = append(s.clients, client)

			s.lock.Unlock()

			s.handlers.NewClientHandler(client)
		}
	}()

	return err
}

// GetAllClients returns a list of all clients which are connected.
func (s *MemdServer) GetAllClients() []*MemdClient {
	s.lock.Lock()
	defer s.lock.Unlock()

	allClients := append([]*MemdClient{}, s.clients...)

	return allClients
}

func (s *MemdServer) handleClientRequest(client *MemdClient, pak *memd.Packet) {
	s.handlers.PacketHandler(client, pak)
}

func (s *MemdServer) handleClientDisconnect(client *MemdClient) {
	s.handlers.LostClientHandler(client)

	s.lock.Lock()

	var newClients []*MemdClient
	for _, foundClient := range s.clients {
		if foundClient != client {
			newClients = append(newClients, foundClient)
		}
	}
	s.clients = newClients

	s.lock.Unlock()
}

// Close causes this memd server to be forcefully stopped and all clients dropped.
func (s *MemdServer) Close() error {
	err := s.listener.Close()
	if err != nil {
		log.Printf("failed to close memd listener: %s", err)
	}

	var lastClient *MemdClient
	for {
		s.lock.Lock()
		if len(s.clients) == 0 {
			s.lock.Unlock()
			break
		}

		nextClient := s.clients[0]
		s.lock.Unlock()

		if nextClient == lastClient {
			log.Printf("the same client appeared twice during closing")
			break
		}
		lastClient = nextClient

		err := nextClient.Close()
		if err != nil {
			log.Printf("failed to close memd client: %s", err)
		}
	}

	return nil
}
