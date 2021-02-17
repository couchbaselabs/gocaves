package svcimpls

import (
	"bytes"
	"github.com/couchbaselabs/gocaves/mock"
)

type queryImplPing struct {
}

func (x *queryImplPing) Register(h *hookHelper) {
	h.RegisterQueryHandler("GET", "/admin/ping", x.handlePing)
}

func (x *queryImplPing) handlePing(source mock.QueryService, req *mock.HTTPRequest) *mock.HTTPResponse {
	// TODO(chvck): double check that http ping handlers don't need auth

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte(`{}`)),
	}
}
