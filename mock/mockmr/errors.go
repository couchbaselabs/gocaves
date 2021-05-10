package mockmr

import "errors"

// This is a list of errors we support
var (
	ErrNotFound = errors.New(`{"error":"not_found","reason":"missing"}`)

	ErrInvalidParameters = errors.New("invalid parameters")
)

type InvalidParametersError struct {
	Message string
}

func (ipe *InvalidParametersError) Error() string {
	return ipe.Message
}

func (ipe *InvalidParametersError) Unwrap() error {
	return ErrInvalidParameters
}
