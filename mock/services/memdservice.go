package services

import (
	"log"
	"net"
	"sync"

	"github.com/couchbase/gocbcore/v9/memd"
)

// MemdServiceHandlers provides all the handlers for this service
type MemdServiceHandlers struct {
	NewClientHandler  func(*MemdClient)
	LostClientHandler func(*MemdClient)
	PacketHandler     func(*MemdClient, *memd.Packet)
}

// MemdService represents an instance of the memd service.
type MemdService struct {
	lock       sync.Mutex
	listenPort int
	localAddr  string
	listener   net.Listener
	handlers   MemdServiceHandlers

	clients []*MemdClient
}

// NewMemdServiceOptions enables the specification of default options for a new memd service.
type NewMemdServiceOptions struct {
	Handlers MemdServiceHandlers
}

// NewMemdService instantiates a new instance of the memd service.
func NewMemdService(opts NewMemdServiceOptions) (*MemdService, error) {
	svc := &MemdService{
		handlers: opts.Handlers,
	}

	err := svc.start()
	if err != nil {
		return nil, err
	}

	return svc, nil
}

// ListenPort returns the port this service is listening on.
func (s *MemdService) ListenPort() int {
	return s.listenPort
}

func (s *MemdService) start() error {
	lsnr, err := net.Listen("tcp", ":0")
	if err != nil {
		return err
	}

	// Save the local listening address
	addr := lsnr.Addr()
	tcpAddr := addr.(*net.TCPAddr)
	s.listenPort = tcpAddr.Port
	s.localAddr = addr.String()

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

	log.Printf("memd service started on %+v", s)

	return err
}

func (s *MemdService) handleClientRequest(client *MemdClient, pak *memd.Packet) {
	s.handlers.PacketHandler(client, pak)
}

func (s *MemdService) handleClientDisconnect(client *MemdClient) {
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

// Close causes this memd service to be forcefully stopped and all clients dropped.
func (s *MemdService) Close() error {
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
