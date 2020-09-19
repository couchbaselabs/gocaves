package mock

import "github.com/couchbaselabs/gocaves/mock/servers"

// MgmtRequest represents a single request received by the management service.
type MgmtRequest struct {
	Source  *MgmtService
	Request servers.HTTPRequest
}

// MgmtService represents a management service running somewhere in the cluster.
type MgmtService struct {
	clusterNode *ClusterNode
	server      *servers.HTTPServer
}

type newMgmtServiceOptions struct {
}

func newMgmtService(parent *ClusterNode, opts newMgmtServiceOptions) (*MgmtService, error) {
	svc := &MgmtService{
		clusterNode: parent,
	}

	srv, err := servers.NewHTTPServer(servers.NewHTTPServiceOptions{
		Name: "mgmt",
		Handlers: servers.HTTPServerHandlers{
			NewRequestHandler: svc.handleNewRequest,
		},
	})
	if err != nil {
		return nil, err
	}
	svc.server = srv

	return svc, nil
}

// Node returns the node which owns this service.
func (s *MgmtService) Node() *ClusterNode {
	return s.clusterNode
}

// Hostname returns the hostname where this service can be accessed.
func (s *MgmtService) Hostname() string {
	return "127.0.0.1"
}

// ListenPort returns the port this service is listening on.
func (s *MgmtService) ListenPort() int {
	return s.server.ListenPort()
}

func (s *MgmtService) handleNewRequest(req *servers.HTTPRequest) *servers.HTTPResponse {
	return s.clusterNode.cluster.handleMgmtRequest(s, req)
}

// Close will shut down this service once it is no longer needed.
func (s *MgmtService) Close() error {
	return s.server.Close()
}
