package mockimpl

import (
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mockimpl/servers"
)

// analyticsService represents a analytics service running somewhere in the cluster.
type analyticsService struct {
	clusterNode *clusterNodeInst
	server      *servers.HTTPServer
}

type newAnalyticsServiceOptions struct {
}

func newAnalyticsService(parent *clusterNodeInst, opts newAnalyticsServiceOptions) (*analyticsService, error) {
	svc := &analyticsService{
		clusterNode: parent,
	}

	srv, err := servers.NewHTTPServer(servers.NewHTTPServiceOptions{
		Name: "analytics",
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
func (s *analyticsService) Node() mock.ClusterNode {
	return s.clusterNode
}

// Hostname returns the hostname where this service can be accessed.
func (s *analyticsService) Hostname() string {
	return "127.0.0.1"
}

// ListenPort returns the port this service is listening on.
func (s *analyticsService) ListenPort() int {
	return s.server.ListenPort()
}

func (s *analyticsService) handleNewRequest(req *mock.HTTPRequest) *mock.HTTPResponse {
	return s.clusterNode.cluster.handleAnalyticsRequest(s, req)
}

// Close will shut down this service once it is no longer needed.
func (s *analyticsService) Close() error {
	return s.server.Close()
}
