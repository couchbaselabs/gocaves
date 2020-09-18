package mock

import "github.com/couchbaselabs/gocaves/mock/servers"

// MgmtRequest represents a single request received by the management service.
type MgmtRequest struct {
	Source  *MgmtService
	Request servers.HTTPRequest
}

// MgmtService represents a management service running somewhere in the cluster.
type MgmtService struct {
	clusterNode *ClusterNode
	server      *servers.HTTPServer
}

type newMgmtServiceOptions struct {
}

func newMgmtService(parent *ClusterNode, opts newMgmtServiceOptions) (*MgmtService, error) {
	svc := &MgmtService{
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

func (s *MgmtService) handleNewRequest(*servers.HTTPRequest) *servers.HTTPResponse {
	return nil
}

// Close will shut down this service once it is no longer needed.
func (s *MgmtService) Close() error {
	// TODO(brett19): Implement this...
	return nil
}
