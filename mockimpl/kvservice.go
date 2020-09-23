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
	isTLS   bool

	authenticatedUserName string
	selectedBucketName    string
	scramServer           scramserver.ScramServer
}

// IsTLS returns whether this client is connected via TLS
// TODO(brett19): Make this return the TLS config for cert-auth.
func (c *kvClient) IsTLS() bool {
	return c.isTLS
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
	tlsServer   *servers.MemdServer
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

	if parent.cluster.IsFeatureEnabled(mock.ClusterFeatureTLS) {
		tlsSrv, err := servers.NewMemdService(servers.NewMemdServerOptions{
			Handlers: servers.MemdServerHandlers{
				NewClientHandler:  svc.handleNewTLSMemdClient,
				LostClientHandler: svc.handleLostMemdClient,
				PacketHandler:     svc.handleMemdPacket,
			},
			TLSConfig: parent.cluster.tlsConfig,
		})
		if err != nil {
			return nil, err
		}
		svc.tlsServer = tlsSrv
	}

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
	if s.server == nil {
		return -1
	}
	return s.server.ListenPort()
}

// ListenPortTLS returns the TLS port this service is listening on.
func (s *kvService) ListenPortTLS() int {
	if s.tlsServer == nil {
		return -1
	}
	return s.tlsServer.ListenPort()
}

// GetAllClients returns a list of all the clients connected to this service.
func (s *kvService) GetAllClients() []mock.KvClient {
	var allKvClients []mock.KvClient

	if s.server != nil {
		allClients := s.server.GetAllClients()
		for _, client := range allClients {
			kvCli := s.getKvClient(client)
			allKvClients = append(allKvClients, kvCli)
		}
	}

	if s.tlsServer != nil {
		allClients := s.tlsServer.GetAllClients()
		for _, client := range allClients {
			kvCli := s.getKvClient(client)
			allKvClients = append(allKvClients, kvCli)
		}
	}

	return allKvClients
}

// Close will shut down this service once it is no longer needed.
func (s *kvService) Close() error {
	var errOut error
	if s.server != nil {
		errOut = s.server.Close()
	}
	if s.tlsServer != nil {
		errOut = s.tlsServer.Close()
	}
	return errOut
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
	kvCli.isTLS = false
}

func (s *kvService) handleNewTLSMemdClient(cli *servers.MemdClient) {
	kvCli := s.getKvClient(cli)
	kvCli.client = cli
	kvCli.service = s
	kvCli.isTLS = true
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
