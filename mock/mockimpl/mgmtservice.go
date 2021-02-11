package mockimpl

import (
	"encoding/base64"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
	"github.com/couchbaselabs/gocaves/mock/mockimpl/servers"
	"strings"
)

// mgmtService represents a management service running somewhere in the cluster.
type mgmtService struct {
	clusterNode *clusterNodeInst
	server      *servers.HTTPServer
	tlsServer   *servers.HTTPServer
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

	if parent.HasFeature(mock.ClusterNodeFeatureTLS) {
		tlsSrv, err := servers.NewHTTPServer(servers.NewHTTPServiceOptions{
			Name: "mgmt",
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
func (s *mgmtService) Node() mock.ClusterNode {
	return s.clusterNode
}

// Hostname returns the hostname where this service can be accessed.
func (s *mgmtService) Hostname() string {
	return "127.0.0.1"
}

// ListenPort returns the port this service is listening on.
func (s *mgmtService) ListenPort() int {
	if s.server == nil {
		return -1
	}
	return s.server.ListenPort()
}

// ListenPortTLS returns the TLS port this service is listening on.
func (s *mgmtService) ListenPortTLS() int {
	if s.tlsServer == nil {
		return -1
	}
	return s.tlsServer.ListenPort()
}

func (s *mgmtService) handleNewRequest(req *mock.HTTPRequest) *mock.HTTPResponse {
	return s.clusterNode.cluster.handleMgmtRequest(s, req)
}

// Close will shut down this service once it is no longer needed.
func (s *mgmtService) Close() error {
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
func (s *mgmtService) CheckAuthenticated(permission mockauth.Permission, bucket, scope, collection string,
	req *mock.HTTPRequest) bool {
	authHeader := req.Header.Get("Authorization")
	if authHeader == "" {
		return false
	}

	split := strings.SplitN(authHeader, " ", 2)
	if len(split) != 2 || split[0] != "Basic" {
		return false
	}

	p, err := base64.StdEncoding.DecodeString(split[1])
	if err != nil {
		return false
	}

	userpassword := strings.SplitN(string(p), ":", 2)
	if len(userpassword) != 2 {
		return false
	}

	user := s.Node().Cluster().Users().GetUser(userpassword[0])
	if user == nil {
		return false
	}

	if user.Password != userpassword[1] {
		return false
	}

	return user.HasPermission(permission, bucket, scope, collection)
}
