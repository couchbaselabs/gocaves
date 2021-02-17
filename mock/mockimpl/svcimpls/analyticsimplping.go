package svcimpls

import (
	"bytes"
	"github.com/couchbaselabs/gocaves/mock"
)

type analyticsImplPing struct {
}

func (x *analyticsImplPing) Register(h *hookHelper) {
	h.RegisterAnalyticsHandler("GET", "/admin/ping", x.handlePing)
}

func (x *analyticsImplPing) handlePing(source mock.AnalyticsService, req *mock.HTTPRequest) *mock.HTTPResponse {
	// TODO(chvck): double check that http ping handlers don't need auth

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte(`{"status":"ok"}`)),
	}
}
