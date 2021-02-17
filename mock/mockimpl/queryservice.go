package mockimpl

import (
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
	"github.com/couchbaselabs/gocaves/mock/mockimpl/servers"
)

// queryService represents a Querys service running somewhere in the cluster.
type queryService struct {
	clusterNode *clusterNodeInst
	server      *servers.HTTPServer
	tlsServer   *servers.HTTPServer
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

	if parent.HasFeature(mock.ClusterNodeFeatureTLS) {
		tlsSrv, err := servers.NewHTTPServer(servers.NewHTTPServiceOptions{
			Name: "query",
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
func (s *queryService) Node() mock.ClusterNode {
	return s.clusterNode
}

// Hostname returns the hostname where this service can be accessed.
func (s *queryService) Hostname() string {
	return "127.0.0.1"
}

// ListenPort returns the port this service is listening on.
func (s *queryService) ListenPort() int {
	if s.server == nil {
		return -1
	}
	return s.server.ListenPort()
}

// ListenPortTLS returns the TLS port this service is listening on.
func (s *queryService) ListenPortTLS() int {
	if s.tlsServer == nil {
		return -1
	}
	return s.tlsServer.ListenPort()
}

func (s *queryService) handleNewRequest(req *mock.HTTPRequest) *mock.HTTPResponse {
	return s.clusterNode.cluster.handleQueryRequest(s, req)
}

// Close will shut down this service once it is no longer needed.
func (s *queryService) Close() error {
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
func (s *queryService) CheckAuthenticated(permission mockauth.Permission, bucket, scope, collection string,
	req *mock.HTTPRequest) bool {
	return checkHTTPAuthenticated(permission, bucket, scope, collection, req, s.Node().Cluster().Users())
}
