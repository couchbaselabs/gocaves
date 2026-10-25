//go:build !go1.27

package servers

import "net/http"

// allowHeaderValues lets srv read up to n header values in a request;
// before Go 1.27 net/http has no such limit.
func allowHeaderValues(*http.Server, int) {}
