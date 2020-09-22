package mockimpl

import (
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mockimpl/servers"
)

// mgmtService represents a management service running somewhere in the cluster.
type mgmtService struct {
	clusterNode *clusterNodeInst
	server      *servers.HTTPServer
}

type newMgmtServiceOptions struct {
}

func newMgmtService(parent *clusterNodeInst, opts newMgmtServiceOptions) (*mgmtService, error) {
	svc := &mgmtService{
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
func (s *mgmtService) Node() mock.ClusterNode {
	return s.clusterNode
}

// Hostname returns the hostname where this service can be accessed.
func (s *mgmtService) Hostname() string {
	return "127.0.0.1"
}

// ListenPort returns the port this service is listening on.
func (s *mgmtService) ListenPort() int {
	return s.server.ListenPort()
}

func (s *mgmtService) handleNewRequest(req *mock.HTTPRequest) *mock.HTTPResponse {
	return s.clusterNode.cluster.handleMgmtRequest(s, req)
}

// Close will shut down this service once it is no longer needed.
func (s *mgmtService) Close() error {
	return s.server.Close()
}
