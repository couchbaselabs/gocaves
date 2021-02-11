package mockauth

import "errors"

// This is a list of errors we support
var (
	ErrUserExists = errors.New("user already exists")
)
