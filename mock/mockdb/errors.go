package mockdb

import "errors"

// ErrDocExists is thrown when document was expected not to exist, but did.
var ErrDocExists = errors.New("document already exists")

// ErrDocNotFound is thrown when a document was expected to exist but did not.
var ErrDocNotFound = errors.New("document not found")
