package crudproc

import "errors"

// This is a list of errors we support
var (
	ErrNotSupported = errors.New("not supported")
	ErrNotMyVbucket = errors.New("not my vbucket")
	ErrInternal     = errors.New("internal error")
	ErrDocExists    = errors.New("doc exists")
	ErrDocNotFound  = errors.New("doc not found")
	ErrCasMismatch  = errors.New("cas mismatch")
	ErrLocked       = errors.New("locked")
	ErrNotLocked    = errors.New("not locked")
)
