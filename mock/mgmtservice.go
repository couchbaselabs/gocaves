package mock

// MgmtService represents an instance of the management service.
type MgmtService struct {
	clusterNode *ClusterNode

	httpSvc *HTTPService
}

// NewMgmtServiceOptions enables the specification of default options for a new mgmt service.
type NewMgmtServiceOptions struct {
}

// NewMgmtService creates a new MgmtService instance
func NewMgmtService(parent *ClusterNode, opts NewMgmtServiceOptions) (*MgmtService, error) {
	svc := &MgmtService{
		clusterNode: parent,
		httpSvc: &HTTPService{
			serviceName: "mgmt",
		},
	}

	err := svc.httpSvc.Start()
	if err != nil {
		return nil, err
	}

	return svc, nil
}
