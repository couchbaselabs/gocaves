package mockimpl

import (
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mockimpl/servers"
)

// viewService represents a views service running somewhere in the cluster.
type viewService struct {
	clusterNode *clusterNodeInst
	server      *servers.HTTPServer
}

type newViewServiceOptions struct {
}

func newViewService(parent *clusterNodeInst, opts newViewServiceOptions) (*viewService, error) {
	svc := &viewService{
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
func (s *viewService) Node() mock.ClusterNode {
	return s.clusterNode
}

// Hostname returns the hostname where this service can be accessed.
func (s *viewService) Hostname() string {
	return "127.0.0.1"
}

// ListenPort returns the port this service is listening on.
func (s *viewService) ListenPort() int {
	return s.server.ListenPort()
}

func (s *viewService) handleNewRequest(req *mock.HTTPRequest) *mock.HTTPResponse {
	return s.clusterNode.cluster.handleViewRequest(s, req)
}

// Close will shut down this service once it is no longer needed.
func (s *viewService) Close() error {
	return s.server.Close()
}
