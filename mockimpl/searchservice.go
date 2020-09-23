package mockimpl

import (
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mockimpl/servers"
)

// searchService represents a views service running somewhere in the cluster.
type searchService struct {
	clusterNode *clusterNodeInst
	server      *servers.HTTPServer
	tlsServer   *servers.HTTPServer
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

	if parent.cluster.IsFeatureEnabled(mock.ClusterFeatureTLS) {
		tlsSrv, err := servers.NewHTTPServer(servers.NewHTTPServiceOptions{
			Name: "search",
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
func (s *searchService) Node() mock.ClusterNode {
	return s.clusterNode
}

// Hostname returns the hostname where this service can be accessed.
func (s *searchService) Hostname() string {
	return "127.0.0.1"
}

// ListenPort returns the port this service is listening on.
func (s *searchService) ListenPort() int {
	if s.server == nil {
		return -1
	}
	return s.server.ListenPort()
}

// ListenPortTLS returns the TLS port this service is listening on.
func (s *searchService) ListenPortTLS() int {
	if s.tlsServer == nil {
		return -1
	}
	return s.tlsServer.ListenPort()
}

func (s *searchService) handleNewRequest(req *mock.HTTPRequest) *mock.HTTPResponse {
	return s.clusterNode.cluster.handleSearchRequest(s, req)
}

// Close will shut down this service once it is no longer needed.
func (s *searchService) Close() error {
	var errOut error
	if s.server != nil {
		errOut = s.server.Close()
	}
	if s.tlsServer != nil {
		errOut = s.tlsServer.Close()
	}
	return errOut
}
