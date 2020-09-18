package mock

import (
	"fmt"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock/servers"
)

// KvRequest represents an incoming packet from one of our clients.  Note that
// kv is not always request/reply oriented, but this naming is more clear.
type KvRequest struct {
	Source *KvClient
	Packet memd.Packet
}

// KvClient represents all the state about a connected kv client.
type KvClient struct {
	client  *servers.MemdClient
	service *KvService

	SelectedBucket string
}

// Source returns the KvService which owns this client.
func (c *KvClient) Source() *KvService {
	return c.service
}

// WritePacket tries to write data to the underlying connection.
func (c *KvClient) WritePacket(pak *memd.Packet) error {
	return c.client.WritePacket(pak)
}

// Close attempts to close the connection.
func (c *KvClient) Close() error {
	return c.client.Close()
}

// KvService represents an instance of the kv service.
type KvService struct {
	clusterNode *ClusterNode
	server      *servers.MemdServer
}

// newKvServiceOptions enables the specification of default options for a new kv service.
type newKvServiceOptions struct {
}

// newKvService instantiates a new instance of the kv service.
func newKvService(parent *ClusterNode, opts newKvServiceOptions) (*KvService, error) {
	svc := &KvService{
		clusterNode: parent,
	}

	srv, err := servers.NewMemdService(servers.NewMemdServerOptions{
		Handlers: servers.MemdServerHandlers{
			NewClientHandler:  svc.handleNewMemdClient,
			LostClientHandler: svc.handleLostMemdClient,
			PacketHandler:     svc.handleMemdPacket,
		},
	})
	if err != nil {
		return nil, err
	}
	svc.server = srv

	return svc, nil
}

// Node returns the ClusterNode which owns this service.
func (s *KvService) Node() *ClusterNode {
	return s.clusterNode
}

// Address returns the host/port address of this service.
func (s *KvService) Address() string {
	return fmt.Sprintf("%s:%d", s.Hostname(), s.ListenPort())
}

// Hostname returns the hostname where this service can be accessed.
func (s *KvService) Hostname() string {
	return "127.0.0.1"
}

// ListenPort returns the port this service is listening on.
func (s *KvService) ListenPort() int {
	return s.server.ListenPort()
}

// GetAllClients returns a list of all the clients connected to this service.
func (s *KvService) GetAllClients() []*KvClient {
	allClients := s.server.GetAllClients()

	var allKvClients []*KvClient
	for _, client := range allClients {
		kvCli := s.getKvClient(client)
		allKvClients = append(allKvClients, kvCli)
	}

	return allKvClients
}

// Close will shut down this service once it is no longer needed.
func (s *KvService) Close() error {
	// TODO(brett19): Implement this...
	return nil
}

func (s *KvService) getKvClient(cli *servers.MemdClient) *KvClient {
	var kvCli *KvClient
	cli.GetContext(&kvCli)
	return kvCli
}

func (s *KvService) handleNewMemdClient(cli *servers.MemdClient) {
	kvCli := s.getKvClient(cli)
	kvCli.client = cli
	kvCli.service = s
}

func (s *KvService) handleLostMemdClient(cli *servers.MemdClient) {
	kvCli := s.getKvClient(cli)
	kvCli.client = nil
}

func (s *KvService) handleMemdPacket(cli *servers.MemdClient, pak *memd.Packet) {
	kvCli := s.getKvClient(cli)
	if kvCli.client == nil {
		return
	}

}
