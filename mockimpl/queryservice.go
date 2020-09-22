package mockimpl

import (
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mockimpl/servers"
)

// queryService represents a Querys service running somewhere in the cluster.
type queryService struct {
	clusterNode *clusterNodeInst
	server      *servers.HTTPServer
}

type newQueryServiceOptions struct {
}

func newQueryService(parent *clusterNodeInst, opts newQueryServiceOptions) (*queryService, error) {
	svc := &queryService{
		clusterNode: parent,
	}

	srv, err := servers.NewHTTPServer(servers.NewHTTPServiceOptions{
		Name: "query",
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
func (s *queryService) Node() mock.ClusterNode {
	return s.clusterNode
}

// Hostname returns the hostname where this service can be accessed.
func (s *queryService) Hostname() string {
	return "127.0.0.1"
}

// ListenPort returns the port this service is listening on.
func (s *queryService) ListenPort() int {
	return s.server.ListenPort()
}

func (s *queryService) handleNewRequest(req *mock.HTTPRequest) *mock.HTTPResponse {
	return s.clusterNode.cluster.handleQueryRequest(s, req)
}

// Close will shut down this service once it is no longer needed.
func (s *queryService) Close() error {
	return s.server.Close()
}
