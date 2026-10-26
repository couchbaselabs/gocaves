package mockimpl

import (
	"github.com/couchbaselabs/gocaves/mock"
)

func clusterFeatureListContains(list []mock.ClusterNodeFeature, feature mock.ClusterNodeFeature) bool {
	// An empty list acts like a completely full list
	if len(list) == 0 {
		return true
	}

	// Check if we actually have the service in the list
	for _, listFeat := range list {
		if listFeat == feature {
			return true
		}
	}
	return false
}

func serviceTypeListContains(list []mock.ServiceType, service mock.ServiceType) bool {
	// An empty list acts like a completely full list
	if len(list) == 0 {
		return true
	}

	// Check if we actually have the service in the list
	for _, listSvc := range list {
		if listSvc == service {
			return true
		}
	}
	return false
}

// checkHTTPAuthenticated reports whether the request carries the credentials
// of a user. The management and views services check the permission each
// request needs, as ns_server's menelaus_web and capi_frontend do, before
// its handler runs, and no handler of the other services needs one.
func checkHTTPAuthenticated(req *mock.HTTPRequest, users mock.UserManager) bool {
	return req.AuthenticatedUser(users) != nil
}
