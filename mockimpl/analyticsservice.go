package mockimpl

import "github.com/couchbaselabs/gocaves/mockimpl/servers"

// AnalyticsRequest represents a single request received by the analytics service.
type AnalyticsRequest struct {
	Source  *MgmtService
	Request servers.HTTPRequest
}

// AnalyticsService represents a analytics service running somewhere in the cluster.
type AnalyticsService struct {
	clusterNode *ClusterNode
	server      *servers.HTTPServer
}

type newAnalyticsServiceOptions struct {
}

func newAnalyticsService(parent *ClusterNode, opts newAnalyticsServiceOptions) (*AnalyticsService, error) {
	svc := &AnalyticsService{
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
func (s *AnalyticsService) Node() *ClusterNode {
	return s.clusterNode
}

// Hostname returns the hostname where this service can be accessed.
func (s *AnalyticsService) Hostname() string {
	return "127.0.0.1"
}

// ListenPort returns the port this service is listening on.
func (s *AnalyticsService) ListenPort() int {
	return s.server.ListenPort()
}

func (s *AnalyticsService) handleNewRequest(req *servers.HTTPRequest) *servers.HTTPResponse {
	return s.clusterNode.cluster.handleAnalyticsRequest(s, req)
}

// Close will shut down this service once it is no longer needed.
func (s *AnalyticsService) Close() error {
	return s.server.Close()
}
