package svcimpls

import (
	"bytes"
	"fmt"
	"github.com/couchbaselabs/gocaves/mock"
	"net/http"
)

func (x *mgmtImpl) handlePing(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	// TODO(chvck): double check that http ping handlers don't need auth

	headers := http.Header{}
	headers.Add("Location", fmt.Sprintf("http://%s:%d/ui/index.html", source.Hostname(), source.ListenPort()))
	return &mock.HTTPResponse{
		Header:     headers,
		StatusCode: 301,
		Body:       bytes.NewReader([]byte(`<!DOCTYPE HTML PUBLIC "-//IETF//DTD HTML 2.0//EN"><html><head><title>301 MovedPermanently</title></head><body><h1>Moved Permanently</h1><p>The document has moved <a href="http://172.23.111.134:8091/ui/index.html>here</a>.</p></body></html>`)),
	}
}

func (x *mgmtImpl) handleIndex(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	// TODO(chvck): double check that http ping handlers don't need auth
	// TODO(chvck): this obviously isn't right but is ok for ping.
	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte{}),
	}
}
