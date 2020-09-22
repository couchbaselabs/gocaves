package mockimpl

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mockimpl/servers"
	"github.com/couchbaselabs/gocaves/scramserver"
)

// kvClient represents all the state about a connected kv client.
type kvClient struct {
	client  *servers.MemdClient
	service *kvService

	authenticatedUserName string
	selectedBucketName    string
	scramServer           scramserver.ScramServer
}

// ScramServer returns a SCRAM server object specific to this user.
func (c *kvClient) ScramServer() *scramserver.ScramServer {
	return &c.scramServer
}

// SetAuthenticatedUserName sets the name of the user who is authenticated.
func (c *kvClient) SetAuthenticatedUserName(userName string) {
	c.authenticatedUserName = userName
}

// AuthenticatedUserName gets the name of the user who is authenticated.
func (c *kvClient) AuthenticatedUserName() string {
	return c.authenticatedUserName
}

// SetSelectedBucketName sets the currently selected bucket's name.
func (c *kvClient) SetSelectedBucketName(bucketName string) {
	c.selectedBucketName = bucketName
}

// SelectedBucketName returns the currently selected bucket's name.
func (c *kvClient) SelectedBucketName() string {
	return c.selectedBucketName
}

// SelectedBucket returns the currently selected bucket.
func (c *kvClient) SelectedBucket() mock.Bucket {
	return c.service.clusterNode.cluster.GetBucket(c.selectedBucketName)
}

// Source returns the KvService which owns this client.
func (c *kvClient) Source() mock.KvService {
	return c.service
}

// WritePacket tries to write data to the underlying connection.
func (c *kvClient) WritePacket(pak *memd.Packet) error {
	if !c.service.clusterNode.cluster.handleKvPacketOut(c, pak) {
		return nil
	}
	return c.client.WritePacket(pak)
}

// Close attempts to close the connection.
func (c *kvClient) Close() error {
	return c.client.Close()
}

// kvService represents an instance of the kv service.
type kvService struct {
	clusterNode *clusterNodeInst
	server      *servers.MemdServer
}

// newKvServiceOptions enables the specification of default options for a new kv service.
type newKvServiceOptions struct {
}

// newKvService instantiates a new instance of the kv service.
func newKvService(parent *clusterNodeInst, opts newKvServiceOptions) (*kvService, error) {
	svc := &kvService{
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
func (s *kvService) Node() mock.ClusterNode {
	return s.clusterNode
}

// Hostname returns the hostname where this service can be accessed.
func (s *kvService) Hostname() string {
	return "127.0.0.1"
}

// ListenPort returns the port this service is listening on.
func (s *kvService) ListenPort() int {
	return s.server.ListenPort()
}

// GetAllClients returns a list of all the clients connected to this service.
func (s *kvService) GetAllClients() []mock.KvClient {
	allClients := s.server.GetAllClients()

	var allKvClients []mock.KvClient
	for _, client := range allClients {
		kvCli := s.getKvClient(client)
		allKvClients = append(allKvClients, kvCli)
	}

	return allKvClients
}

// Close will shut down this service once it is no longer needed.
func (s *kvService) Close() error {
	return s.server.Close()
}

func (s *kvService) getKvClient(cli *servers.MemdClient) *kvClient {
	var kvCli *kvClient
	cli.GetContext(&kvCli)
	return kvCli
}

func (s *kvService) handleNewMemdClient(cli *servers.MemdClient) {
	kvCli := s.getKvClient(cli)
	kvCli.client = cli
	kvCli.service = s
}

func (s *kvService) handleLostMemdClient(cli *servers.MemdClient) {
	kvCli := s.getKvClient(cli)
	kvCli.client = nil
}

func (s *kvService) handleMemdPacket(cli *servers.MemdClient, pak *memd.Packet) {
	kvCli := s.getKvClient(cli)
	if kvCli.client == nil {
		return
	}

	s.clusterNode.cluster.handleKvPacketIn(kvCli, pak)
}
