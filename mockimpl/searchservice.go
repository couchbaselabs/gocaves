package mockimpl

import (
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mockimpl/servers"
)

// searchService represents a views service running somewhere in the cluster.
type searchService struct {
	clusterNode *clusterNodeInst
	server      *servers.HTTPServer
}

type newSearchServiceOptions struct {
}

func newSearchService(parent *clusterNodeInst, opts newSearchServiceOptions) (*searchService, error) {
	svc := &searchService{
		clusterNode: parent,
	}

	srv, err := servers.NewHTTPServer(servers.NewHTTPServiceOptions{
		Name: "search",
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
func (s *searchService) Node() mock.ClusterNode {
	return s.clusterNode
}

// Hostname returns the hostname where this service can be accessed.
func (s *searchService) Hostname() string {
	return "127.0.0.1"
}

// ListenPort returns the port this service is listening on.
func (s *searchService) ListenPort() int {
	return s.server.ListenPort()
}

func (s *searchService) handleNewRequest(req *mock.HTTPRequest) *mock.HTTPResponse {
	return s.clusterNode.cluster.handleSearchRequest(s, req)
}

// Close will shut down this service once it is no longer needed.
func (s *searchService) Close() error {
	return s.server.Close()
}
