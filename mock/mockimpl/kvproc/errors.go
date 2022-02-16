package kvproc

import "errors"

// This is a list of errors we support
var (
	ErrNotSupported           = errors.New("not supported")
	ErrNotMyVbucket           = errors.New("not my vbucket")
	ErrInternal               = errors.New("internal error")
	ErrDocExists              = errors.New("doc exists")
	ErrDocNotFound            = errors.New("doc not found")
	ErrValueTooBig            = errors.New("doc value too big")
	ErrCasMismatch            = errors.New("cas mismatch")
	ErrLocked                 = errors.New("locked")
	ErrNotLocked              = errors.New("not locked")
	ErrInvalidArgument        = errors.New("invalid packet")
	ErrSdToManyTries          = errors.New("subdocument too many attempts")
	ErrSdNotJSON              = errors.New("subdocument not json")
	ErrSdPathInvalid          = errors.New("subdocument path invalid")
	ErrSdPathMismatch         = errors.New("subdocument path mismatch")
	ErrSdPathNotFound         = errors.New("subdocument path not found")
	ErrSdPathExists           = errors.New("subdocument path exists")
	ErrSdCantInsert           = errors.New("subdocument cant insert")
	ErrSdBadCombo             = errors.New("subdocument invalid combo")
	ErrSdInvalidFlagCombo     = errors.New("invalid xattr flag combination")
	ErrUnknownXattrMacro      = errors.New("unknown xattr macro")
	ErrSdInvalidXattr         = errors.New("there is something wrong with the syntax of the provided XATTR")
	ErrSdCannotModifyVattr    = errors.New("xattr cannot modify virtual attribute")
	ErrSdXattrInvalidKeyCombo = errors.New("invalid xattr key combination")
)

type SubdocMutateError struct {
	Err error
}

func (e SubdocMutateError) Error() string {
	return e.Err.Error()
}
