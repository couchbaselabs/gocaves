package mockimpl

import (
	"encoding/base64"
	"strings"

	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
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

func checkHTTPAuthenticated(permission mockauth.Permission, bucket, scope, collection string,
	req *mock.HTTPRequest, users mock.UserManager) bool {
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

	user := users.GetUser(userpassword[0])
	if user == nil {
		return false
	}

	if user.Password != userpassword[1] {
		return false
	}

	return user.HasPermission(permission, bucket, scope, collection)
}
