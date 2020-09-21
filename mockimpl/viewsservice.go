package mockimpl

import "github.com/couchbaselabs/gocaves/mockimpl/servers"

// ViewRequest represents a single request received by the view service.
type ViewRequest struct {
	Source  *ViewService
	Request servers.HTTPRequest
}

// ViewService represents a views service running somewhere in the cluster.
type ViewService struct {
	clusterNode *ClusterNode
	server      *servers.HTTPServer
}

type newViewServiceOptions struct {
}

func newViewService(parent *ClusterNode, opts newViewServiceOptions) (*ViewService, error) {
	svc := &ViewService{
		clusterNode: parent,
	}

	srv, err := servers.NewHTTPServer(servers.NewHTTPServiceOptions{
		Name: "view",
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
func (s *ViewService) Node() *ClusterNode {
	return s.clusterNode
}

// Hostname returns the hostname where this service can be accessed.
func (s *ViewService) Hostname() string {
	return "127.0.0.1"
}

// ListenPort returns the port this service is listening on.
func (s *ViewService) ListenPort() int {
	return s.server.ListenPort()
}

func (s *ViewService) handleNewRequest(req *servers.HTTPRequest) *servers.HTTPResponse {
	return s.clusterNode.cluster.handleViewRequest(s, req)
}

// Close will shut down this service once it is no longer needed.
func (s *ViewService) Close() error {
	return s.server.Close()
}
