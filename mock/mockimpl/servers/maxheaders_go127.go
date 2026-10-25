//go:build go1.27

package servers

import "net/http"

// allowHeaderValues lets srv read up to n header values in a request;
// from Go 1.27 net/http reads 500 unless told otherwise.
func allowHeaderValues(srv *http.Server, n int) {
	srv.MaxHeaderValueCount = n
}
