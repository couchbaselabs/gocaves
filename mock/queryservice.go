package mock

import "github.com/couchbaselabs/gocaves/mock/servers"

// QueryRequest represents a single request received by the Query service.
type QueryRequest struct {
	Source  *QueryService
	Request servers.HTTPRequest
}

// QueryService represents a Querys service running somewhere in the cluster.
type QueryService struct {
	clusterNode *ClusterNode
	server      *servers.HTTPServer
}

type newQueryServiceOptions struct {
}

func newQueryService(parent *ClusterNode, opts newQueryServiceOptions) (*QueryService, error) {
	svc := &QueryService{
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
func (s *QueryService) Node() *ClusterNode {
	return s.clusterNode
}

// Hostname returns the hostname where this service can be accessed.
func (s *QueryService) Hostname() string {
	return "127.0.0.1"
}

// ListenPort returns the port this service is listening on.
func (s *QueryService) ListenPort() int {
	return s.server.ListenPort()
}

func (s *QueryService) handleNewRequest(req *servers.HTTPRequest) *servers.HTTPResponse {
	return s.clusterNode.cluster.handleQueryRequest(s, req)
}

// Close will shut down this service once it is no longer needed.
func (s *QueryService) Close() error {
	return s.server.Close()
}
