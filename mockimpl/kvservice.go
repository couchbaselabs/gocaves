package mockimpl

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mockimpl/servers"
)

// KvClient represents all the state about a connected kv client.
type KvClient struct {
	client  *servers.MemdClient
	service *KvService

	authenticatedUserName string
	selectedBucketName    string
}

// SetAuthenticatedUserName sets the name of the user who is authenticated.
func (c *KvClient) SetAuthenticatedUserName(userName string) {
	c.authenticatedUserName = userName
}

// AuthenticatedUserName gets the name of the user who is authenticated.
func (c *KvClient) AuthenticatedUserName() string {
	return c.authenticatedUserName
}

// SetSelectedBucketName sets the currently selected bucket's name.
func (c *KvClient) SetSelectedBucketName(bucketName string) {
	c.selectedBucketName = bucketName
}

// SelectedBucketName returns the currently selected bucket's name.
func (c *KvClient) SelectedBucketName() string {
	return c.selectedBucketName
}

// SelectedBucket returns the currently selected bucket.
func (c *KvClient) SelectedBucket() *Bucket {
	return c.service.clusterNode.cluster.GetBucket(c.selectedBucketName)
}

// Source returns the KvService which owns this client.
func (c *KvClient) Source() *KvService {
	return c.service
}

// WritePacket tries to write data to the underlying connection.
func (c *KvClient) WritePacket(pak *memd.Packet) error {
	if !c.service.clusterNode.cluster.handleKvPacketOut(c, pak) {
		return nil
	}
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
	return s.server.Close()
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

	s.clusterNode.cluster.handleKvPacketIn(kvCli, pak)
}
