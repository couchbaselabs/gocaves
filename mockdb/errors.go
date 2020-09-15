package mockdb

import (
	"errors"
)

// We provide a number of errors to compare with here
var (
	ErrDocExists   = errors.New("document already exists")
	ErrDocNotFound = errors.New("document not found")
)
