package mock

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/couchbaselabs/gocaves/mock/mockauth"
)

// HTTPRequest encapsulates an HTTP request.
type HTTPRequest struct {
	IsTLS   bool
	Method  string
	Target  string // the request target as sent
	URL     *url.URL
	Header  http.Header
	Body    io.Reader
	Form    url.Values // on the management ports, filled once the body is read
	Context context.Context
	Flusher http.Flusher
}

// PeekBody will return the full body and swap the reader with a
// new one which will allow other users to continue to use it.
func (r *HTTPRequest) PeekBody() []byte {
	data, _ := io.ReadAll(r.Body)
	r.Body = bytes.NewReader(data)
	return data
}

// AuthenticatedUser returns the user whose credentials the request's
// Authorization header carries, or nil. It reads them as ns_server's
// menelaus_auth does: after "Basic ", base64 of the name and the password,
// split at the first colon, the password empty when there is none.
func (r *HTTPRequest) AuthenticatedUser(users UserManager) *mockauth.User {
	value, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Basic ")
	if !ok {
		return nil
	}
	// Erlang's base64:decode/1 skips whitespace.
	value = strings.Map(func(r rune) rune {
		if r == ' ' || r == '\t' || r == '\r' || r == '\n' {
			return -1
		}
		return r
	}, value)
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil || len(decoded) == 0 {
		return nil
	}
	username, password, _ := strings.Cut(string(decoded), ":")
	return users.Authenticate(username, password)
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
	data, _ := io.ReadAll(r.Body)
	r.Body = bytes.NewReader(data)
	return data
}
