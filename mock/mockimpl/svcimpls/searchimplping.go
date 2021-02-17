package svcimpls

import (
	"bytes"
	"github.com/couchbaselabs/gocaves/mock"
)

type searchImplPing struct {
}

func (x *searchImplPing) Register(h *hookHelper) {
	h.RegisterSearchHandler("GET", "/api/ping", x.handlePing)
}

func (x *searchImplPing) handlePing(source mock.SearchService, req *mock.HTTPRequest) *mock.HTTPResponse {
	// TODO(chvck): double check that http ping handlers don't need auth

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte{}),
	}
}
