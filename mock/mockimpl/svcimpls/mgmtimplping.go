package svcimpls

import (
	"bytes"
	"github.com/couchbaselabs/gocaves/mock"
)

func (x *mgmtImpl) handlePing(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	// TODO(chvck): double check that http ping handlers don't need auth

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte(`{"couchdb":"Welcome","version":"v4.5.1-237-g63b3e06","couchbase":"7.0.0-4342-enterprise"}`)),
	}
}
