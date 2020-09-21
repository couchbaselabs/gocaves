package mockimpl

import "github.com/couchbaselabs/gocaves/mockimpl/servers"

// SearchRequest represents a single request received by the view service.
type SearchRequest struct {
	Source  *SearchService
	Request servers.HTTPRequest
}

// SearchService represents a views service running somewhere in the cluster.
type SearchService struct {
	clusterNode *ClusterNode
	server      *servers.HTTPServer
}

type newSearchServiceOptions struct {
}

func newSearchService(parent *ClusterNode, opts newSearchServiceOptions) (*SearchService, error) {
	svc := &SearchService{
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
func (s *SearchService) Node() *ClusterNode {
	return s.clusterNode
}

// Hostname returns the hostname where this service can be accessed.
func (s *SearchService) Hostname() string {
	return "127.0.0.1"
}

// ListenPort returns the port this service is listening on.
func (s *SearchService) ListenPort() int {
	return s.server.ListenPort()
}

func (s *SearchService) handleNewRequest(req *servers.HTTPRequest) *servers.HTTPResponse {
	return s.clusterNode.cluster.handleSearchRequest(s, req)
}

// Close will shut down this service once it is no longer needed.
func (s *SearchService) Close() error {
	return s.server.Close()
}
