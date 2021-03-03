package mockmr

import "errors"

// This is a list of errors we support
var (
	ErrNotFound = errors.New(`{"error":"not_found","reason":"missing"}`)
)
