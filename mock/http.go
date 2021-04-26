package mock

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
)

// HTTPRequest encapsulates an HTTP request.
type HTTPRequest struct {
	IsTLS   bool
	Method  string
	URL     *url.URL
	Header  http.Header
	Body    io.Reader
	Form    url.Values
	Context context.Context
	Flusher http.Flusher
}

// PeekBody will return the full body and swap the reader with a
// new one which will allow other users to continue to use it.
func (r *HTTPRequest) PeekBody() []byte {
	data, _ := ioutil.ReadAll(r.Body)
	r.Body = bytes.NewReader(data)
	return data
}

// HTTPResponse encapsulates an HTTP response.
type HTTPResponse struct {
	StatusCode int
	Header     http.Header
	Body       io.Reader
	Streaming  bool
}

// PeekBody will return the full body and swap the reader with a
// new one which will allow other users to continue to use it.
func (r *HTTPResponse) PeekBody() []byte {
	data, _ := ioutil.ReadAll(r.Body)
	r.Body = bytes.NewReader(data)
	return data
}
