package mock

import (
	"log"
	"net"
	"sync"

	"github.com/couchbaselabs/gocaves/memd"
)

// KvService represents an instance of the kv service.
type KvService struct {
	clusterNode *ClusterNode

	lock       sync.Mutex
	listenPort int
	localAddr  string
	listener   net.Listener

	clients []*KvClient
}

// NewKvServiceOptions enables the specification of default options for a new kv service.
type NewKvServiceOptions struct {
}

// NewKvService instantiates a new instance of the kv service.
func NewKvService(parent *ClusterNode, opts NewKvServiceOptions) (*KvService, error) {
	svc := &KvService{
		clusterNode: parent,
	}

	err := svc.start()
	if err != nil {
		return nil, err
	}

	return svc, nil
}

func (s *KvService) start() error {
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

			client, err := NewKvClient(s, conn)
			if err != nil {
				log.Printf("failed to create kv client: %s", err)
				break
			}

			s.lock.Lock()

			s.clients = append(s.clients, client)

			s.lock.Unlock()
		}
	}()

	return err
}

func (s *KvService) handleClientRequest(client *KvClient, pak *memd.Packet) {

}

func (s *KvService) handleClientDisconnect(client *KvClient) {
	s.lock.Lock()

	var newClients []*KvClient
	for _, foundClient := range s.clients {
		if foundClient != client {
			newClients = append(newClients, foundClient)
		}
	}
	s.clients = newClients

	s.lock.Unlock()
}

// Close causes this kv service to be forcefully stopped and all clients dropped.
func (s *KvService) Close() error {
	s.lock.Lock()

	clients := s.clients
	s.clients = nil

	s.lock.Unlock()

	for _, client := range clients {
		err := client.Close()
		if err != nil {
			log.Printf("faile to close kv client: %s", err)
		}
	}

	return s.listener.Close()
}
