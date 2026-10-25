package kvproc

import "errors"

// This is a list of errors we support
var (
	ErrNotSupported        = errors.New("not supported")
	ErrNotMyVbucket        = errors.New("not my vbucket")
	ErrInternal            = errors.New("internal error")
	ErrDocExists           = errors.New("doc exists")
	ErrDocNotFound         = errors.New("doc not found")
	ErrNotStored           = errors.New("not stored")
	ErrDeltaBadval         = errors.New("counter value is not a number")
	ErrValueTooBig         = errors.New("doc value too big")
	ErrCasMismatch         = errors.New("cas mismatch")
	ErrLocked              = errors.New("locked")
	ErrNotLocked           = errors.New("not locked")
	ErrInvalidArgument     = errors.New("invalid packet")
	ErrSdToManyTries       = errors.New("subdocument too many attempts")
	ErrSdNotJSON           = errors.New("subdocument not json")
	ErrSdPathInvalid       = errors.New("subdocument path invalid")
	ErrSdPathTooBig        = errors.New("subdocument path too big")
	ErrSdDocTooDeep        = errors.New("subdocument document too deep")
	ErrSdValueTooDeep      = errors.New("subdocument value too deep")
	ErrSdUnknownVattr      = errors.New("unknown virtual attribute")
	ErrSdUnknownVattrMacro = errors.New("unknown virtual attribute macro")
	ErrSdPathMismatch      = errors.New("subdocument path mismatch")
	ErrSdPathNotFound      = errors.New("subdocument path not found")
	ErrSdPathExists        = errors.New("subdocument path exists")
	ErrSdCantInsert        = errors.New("subdocument cant insert")
	ErrSdDeltaInvalid      = errors.New("subdocument counter delta invalid")
	ErrSdNumRange          = errors.New("subdocument number out of range")
	ErrUnknownXattrMacro   = errors.New("unknown xattr macro")
)

// SubdocMutateError is the failure of one spec of a multi-mutation, which
// fails the whole request. Index is the position of that spec in the request.
type SubdocMutateError struct {
	Index int
	Err   error
}

func (e SubdocMutateError) Error() string {
	return e.Err.Error()
}
