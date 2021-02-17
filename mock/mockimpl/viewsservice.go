package mockimpl

import (
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
	"github.com/couchbaselabs/gocaves/mock/mockimpl/servers"
)

// viewService represents a views service running somewhere in the cluster.
type viewService struct {
	clusterNode *clusterNodeInst
	server      *servers.HTTPServer
	tlsServer   *servers.HTTPServer
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

	if parent.HasFeature(mock.ClusterNodeFeatureTLS) {
		tlsSrv, err := servers.NewHTTPServer(servers.NewHTTPServiceOptions{
			Name: "view",
			Handlers: servers.HTTPServerHandlers{
				NewRequestHandler: svc.handleNewRequest,
			},
			TLSConfig: parent.cluster.tlsConfig,
		})
		if err != nil {
			return nil, err
		}
		svc.tlsServer = tlsSrv
	}

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
	if s.server == nil {
		return -1
	}
	return s.server.ListenPort()
}

// ListenPortTLS returns the TLS port this service is listening on.
func (s *viewService) ListenPortTLS() int {
	if s.tlsServer == nil {
		return -1
	}
	return s.tlsServer.ListenPort()
}

func (s *viewService) handleNewRequest(req *mock.HTTPRequest) *mock.HTTPResponse {
	return s.clusterNode.cluster.handleViewRequest(s, req)
}

// Close will shut down this service once it is no longer needed.
func (s *viewService) Close() error {
	var errOut error
	if s.server != nil {
		errOut = s.server.Close()
	}
	if s.tlsServer != nil {
		errOut = s.tlsServer.Close()
	}
	return errOut
}

// CheckAuthenticated verifies that the currently authenticated user has the specified permissions.
func (s *viewService) CheckAuthenticated(permission mockauth.Permission, bucket, scope, collection string,
	req *mock.HTTPRequest) bool {
	return checkHTTPAuthenticated(permission, bucket, scope, collection, req, s.Node().Cluster().Users())
}
