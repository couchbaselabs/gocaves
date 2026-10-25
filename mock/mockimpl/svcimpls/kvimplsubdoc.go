package svcimpls

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

const (
	subdocMultiMaxPaths          = 16
	subdocPathMaxLength          = 1024
	subdocMaxXattrLength         = 16
	subdocExtrasDocFlags         = 0xe0
	statusXattrEinval            = memd.StatusCode(0x87)
	statusXattrInvalidOrder      = memd.StatusCode(0xd4)
	statusXattrUnknownVattrMacro = memd.StatusCode(0xd5)

	subdocLookupDocFlags = memd.SubdocDocFlagAccessDeleted | memd.SubdocDocFlagReviveDocument
	subdocCreateDocFlags = memd.SubdocDocFlagMkDoc | memd.SubdocDocFlagAddDoc | memd.SubdocDocFlagCreateAsDeleted |
		subdocLookupDocFlags
)

// subdocSpecTraits are the traits kv_engine gives an opcode in a multi-path
// spec (daemon/subdocument_traits.h@v7.0.5).
type subdocSpecTraits struct {
	mutator        bool
	wholeDoc       bool
	validFlags     memd.SubdocFlag
	mandatoryFlags memd.SubdocFlag
	validDocFlags  memd.SubdocDocFlag
	hasValue       bool
	allowEmptyPath bool
}

var subdocSpecs = map[memd.SubDocOpType]subdocSpecTraits{
	memd.SubDocOpGetDoc: {wholeDoc: true, validDocFlags: subdocLookupDocFlags, allowEmptyPath: true},
	memd.SubDocOpSetDoc: {mutator: true, wholeDoc: true, validDocFlags: memd.SubdocDocFlagMkDoc | memd.SubdocDocFlagAddDoc,
		hasValue: true, allowEmptyPath: true},
	memd.SubDocOpDeleteDoc: {mutator: true, wholeDoc: true, validDocFlags: subdocLookupDocFlags, allowEmptyPath: true},
	memd.SubDocOpGet:       {validFlags: memd.SubdocFlagXattrPath, validDocFlags: subdocLookupDocFlags},
	memd.SubDocOpExists:    {validFlags: memd.SubdocFlagXattrPath, validDocFlags: subdocLookupDocFlags},
	memd.SubDocOpGetCount: {validFlags: memd.SubdocFlagXattrPath, validDocFlags: subdocLookupDocFlags,
		allowEmptyPath: true},
	memd.SubDocOpDictAdd: {mutator: true,
		validFlags:    memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
		validDocFlags: subdocCreateDocFlags, hasValue: true},
	memd.SubDocOpDictSet: {mutator: true,
		validFlags:    memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
		validDocFlags: subdocCreateDocFlags, hasValue: true},
	memd.SubDocOpDelete: {mutator: true, validFlags: memd.SubdocFlagXattrPath, validDocFlags: subdocLookupDocFlags},
	memd.SubDocOpReplace: {mutator: true, validFlags: memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
		validDocFlags: subdocLookupDocFlags | memd.SubdocDocFlagAddDoc, hasValue: true},
	memd.SubDocOpArrayPushLast: {mutator: true,
		validFlags:    memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
		validDocFlags: subdocCreateDocFlags, hasValue: true, allowEmptyPath: true},
	memd.SubDocOpArrayPushFirst: {mutator: true,
		validFlags:    memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
		validDocFlags: subdocCreateDocFlags, hasValue: true, allowEmptyPath: true},
	memd.SubDocOpArrayInsert: {mutator: true, validFlags: memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
		validDocFlags: subdocCreateDocFlags &^ memd.SubdocDocFlagMkDoc, hasValue: true},
	memd.SubDocOpArrayAddUnique: {mutator: true, validFlags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath,
		validDocFlags: subdocCreateDocFlags, hasValue: true, allowEmptyPath: true},
	memd.SubDocOpCounter: {mutator: true, validFlags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath,
		validDocFlags: subdocCreateDocFlags, hasValue: true},
	memd.SubDocOpReplaceBodyWithXattr: {mutator: true, validFlags: memd.SubdocFlagXattrPath,
		mandatoryFlags: memd.SubdocFlagXattrPath, validDocFlags: subdocLookupDocFlags},
}

// Key lengths kv_engine allows (KEY_MAX_LENGTH, MaxCollectionsKeyLen and
// MaxCollectionsLogicalKeyLen).
const (
	keyMaxLength                = 250
	maxCollectionsKeyLen        = 251
	maxCollectionsLogicalKeyLen = 246
)

// keyCollectionID returns the bytes of the collection ID memd decoded from
// the front of pak's key, or nil if it decoded none. memd decodes a request
// from one buffer and slices the key past the ID, so the ID lies between the
// extras and pak.Key.
func keyCollectionID(pak *memd.Packet) []byte {
	rest := pak.Extras[len(pak.Extras):cap(pak.Extras)]
	idLen := len(rest) - len(pak.Key) - len(pak.Value)
	if idLen <= 0 {
		return nil
	}
	return rest[:idLen]
}

// wireKeyLength returns the length of the key of pak as sent.
func wireKeyLength(pak *memd.Packet) int {
	return len(keyCollectionID(pak)) + len(pak.Key)
}

// checkDocumentKey returns the error context kv_engine's
// is_document_key_valid (daemon/mcbp_validators.cc@v7.0.5) refuses the key
// of pak with, or "" if it accepts it. memd has already refused a collection
// ID without a stop byte.
func checkDocumentKey(source mock.KvClient, pak *memd.Packet) string {
	if !source.HasFeature(memd.FeatureCollections) {
		return ""
	}
	if wireKeyLength(pak) <= 1 {
		return "Key length must be >= 2"
	}
	if id := keyCollectionID(pak); len(id) > 1 && id[len(id)-1] == 0 {
		return "Key contains non-canonical leb128"
	}
	if len(pak.Key) == 0 {
		return "No logical key found"
	}
	if pak.CollectionID >= 1 && pak.CollectionID <= 7 {
		return fmt.Sprintf("Invalid collection-id:%d", pak.CollectionID)
	}
	maxLen := maxCollectionsLogicalKeyLen
	if pak.CollectionID == 0 {
		maxLen = keyMaxLength
	}
	if len(pak.Key) > maxLen {
		return fmt.Sprintf("Logical key exceeds %d", maxLen)
	}
	return ""
}

// checkSubdocMulti returns the status and error context kv_engine refuses a
// SUBDOC_MULTI_LOOKUP or SUBDOC_MULTI_MUTATION with, or success:
// validate_basic_header_fields, then subdoc_multi_validator
// (daemon/subdocument_validators.cc@v7.0.5). It checks every spec, in order,
// before any spec runs.
func checkSubdocMulti(source mock.KvClient, pak *memd.Packet, mutator bool) (memd.StatusCode, string) {
	if status, context := checkHeader(source, pak); status != memd.StatusSuccess {
		return status, context
	}
	if context := checkDocumentKey(source, pak); context != "" {
		return memd.StatusInvalidArgs, context
	}

	var docFlags memd.SubdocDocFlag
	switch len(pak.Extras) {
	case 0, 4:
	case 1, 5:
		docFlags = memd.SubdocDocFlag(pak.Extras[len(pak.Extras)-1])
	default:
		return memd.StatusInvalidArgs, "Request extras invalid"
	}
	if docFlags&subdocExtrasDocFlags != 0 || (!mutator && len(pak.Extras) > 1) {
		return memd.StatusInvalidArgs, "Request extras invalid"
	}
	if docFlags&memd.SubdocDocFlagAddDoc != 0 && pak.Cas != 0 {
		return memd.StatusInvalidArgs, "Request with add flag must have CAS 0"
	}
	if docFlags&memd.SubdocDocFlagAddDoc != 0 && docFlags&memd.SubdocDocFlagMkDoc != 0 {
		return memd.StatusInvalidArgs, "Request must not contain both add and mkdoc flags"
	}
	if docFlags&memd.SubdocDocFlagReviveDocument != 0 {
		if docFlags&memd.SubdocDocFlagAccessDeleted == 0 {
			return memd.StatusInvalidArgs, "ReviveDocument can't be used without AccessDeleted"
		}
		if docFlags&memd.SubdocDocFlagCreateAsDeleted != 0 {
			return memd.StatusInvalidArgs, "ReviveDocument can't be used with CreateAsDeleted"
		}
	}

	// kv_engine's buckets, and the daemon when no bucket is selected, have
	// xattrs enabled, so only the connection decides.
	xattrEnabled := source.HasFeature(memd.FeatureXattr)

	headerLen := 4
	if mutator {
		headerLen = 8
	}
	specs := pak.Value
	xattrsAllowed := true
	bodyAllowed := true
	xattrKey := ""
	paths := 0
	for ; paths < subdocMultiMaxPaths && len(specs) > 0; paths++ {
		if !bodyAllowed {
			return memd.StatusSubDocBadCombo, "Request contains an invalid combination of commands"
		}

		if len(specs) < headerLen {
			if mutator {
				return memd.StatusInvalidArgs, "Multi mutation spec truncated"
			}
			return memd.StatusInvalidArgs, "Multi lookup spec truncated"
		}
		op := memd.SubDocOpType(specs[0])
		flags := memd.SubdocFlag(specs[1])
		pathLen := int(binary.BigEndian.Uint16(specs[2:]))
		valueLen := 0
		if mutator {
			valueLen = int(binary.BigEndian.Uint32(specs[4:]))
		}
		if headerLen+pathLen+valueLen > len(specs) {
			if mutator {
				return memd.StatusInvalidArgs, "Multi mutation path and value truncated"
			}
			return memd.StatusInvalidArgs, "Multi lookup path truncated"
		}
		path := string(specs[headerLen : headerLen+pathLen])
		value := string(specs[headerLen+pathLen : headerLen+pathLen+valueLen])
		specs = specs[headerLen+pathLen+valueLen:]
		isXattr := flags&memd.SubdocFlagXattrPath != 0

		traits, ok := subdocSpecs[op]
		if !ok {
			return memd.StatusSubDocBadCombo, "Subdoc and MCBP command must not both be invalid"
		}
		if traits.mutator != mutator {
			return memd.StatusSubDocBadCombo, "Mutation opcodes only permitted for mutation commands"
		}
		if flags&^traits.validFlags != 0 {
			return memd.StatusInvalidArgs, "Request flags invalid"
		}
		if flags&traits.mandatoryFlags != traits.mandatoryFlags {
			return memd.StatusInvalidArgs, "Request missing mandatory flags"
		}
		if docFlags&^traits.validDocFlags != 0 {
			return memd.StatusInvalidArgs, "Request document flags invalid"
		}
		if pathLen > subdocPathMaxLength {
			return memd.StatusInvalidArgs, "Request path length exceeds maximum"
		}
		if traits.wholeDoc && pathLen > 0 {
			return memd.StatusInvalidArgs, "Request must not include path for wholedoc operations"
		}

		if !isXattr {
			if flags&memd.SubdocFlagExpandMacros != 0 {
				return memd.StatusSubDocXattrInvalidFlagCombo, "EXPAND_MACROS flag requires XATTR flag to be set"
			}
		} else {
			if !xattrEnabled {
				return memd.StatusNotSupported, "Connection not XATTR enabled"
			}
			keyLen, ok := xattrKeyLength(path)
			if !ok {
				return statusXattrEinval, "Request XATTR key invalid"
			}
			if path[0] == '$' {
				if mutator {
					return memd.StatusSubDocXattrCannotModifyVAttr, ""
				}
			} else if xattrKey == "" {
				xattrKey = path[:keyLen]
			} else if xattrKey != path[:keyLen] {
				return memd.StatusSubDocXattrInvalidKeyCombo, ""
			}
			if flags&memd.SubdocFlagExpandMacros != 0 && !isXattrMacro(value) {
				return memd.StatusSubDocXattrUnknownMacro, ""
			}
		}

		if !isXattr && !traits.allowEmptyPath && pathLen == 0 {
			return memd.StatusInvalidArgs, "Request must include path"
		}
		if traits.hasValue && valueLen == 0 {
			return memd.StatusInvalidArgs, "Request must include value"
		}
		if !traits.hasValue && valueLen != 0 {
			return memd.StatusInvalidArgs, "Request must not include value"
		}

		isolated := op == memd.SubDocOpDeleteDoc
		if xattrsAllowed {
			xattrsAllowed = isXattr
		} else if isXattr {
			return statusXattrInvalidOrder, ""
		} else if isolated {
			return memd.StatusSubDocBadCombo, "Request contains an invalid combination of commands"
		}
		if isolated {
			bodyAllowed = false
		}
	}

	if paths == 0 {
		return memd.StatusSubDocBadCombo, "Request must contain at least one path"
	}
	if len(specs) != 0 {
		return memd.StatusSubDocBadCombo, "Request must contain at most 16 paths"
	}
	return memd.StatusSuccess, ""
}

// isXattrMacro reports whether kv_engine's validate_macro accepts value.
func isXattrMacro(value string) bool {
	switch value {
	case `"${Mutation.CAS}"`, `"${Mutation.seqno}"`, `"${Mutation.value_crc32c}"`:
		return true
	}
	return strings.HasPrefix(value, `"${$document`) && strings.HasSuffix(value, `}"`)
}

// xattrKeyLength returns the length of the xattr key path starts with, and
// whether kv_engine's is_valid_xattr_key (xattr/key_validator.cc@v7.0.5)
// accepts it.
func xattrKeyLength(path string) (int, bool) {
	dot := len(path)
	system := false
	for offset := 0; offset < len(path); {
		width, ok := utf8CharWidth(path[offset:])
		if !ok {
			return 0, false
		}
		if width == 1 {
			c := path[offset]
			if offset == 0 && (isCPunct(c) || c < 0x20 || c == 0x7f) {
				if c != '_' && c != '$' {
					return 0, false
				}
				system = true
			}
			if (c == '.' || c == '[') && dot == len(path) {
				dot = offset
			}
			if c == 0 {
				return 0, false
			}
		}
		offset += width
	}
	if dot == 0 || dot >= subdocMaxXattrLength || (system && dot == 1) {
		return 0, false
	}
	return dot, true
}

// utf8CharWidth is kv_engine's get_utf8_char_width, which accepts some byte
// sequences that are not valid UTF-8.
func utf8CharWidth(s string) (int, bool) {
	switch {
	case s[0] < 0x80:
		return 1, true
	case len(s) < 2:
		return 0, false
	case s[0]&0xe0 == 0xc0:
		return 2, s[1]&0xc0 == 0x80
	case len(s) < 3:
		return 0, false
	case s[0]&0xf0 == 0xe0:
		return 3, s[1]&0xc0 == 0x80 && s[2]&0xc0 == 0x80
	case len(s) < 4:
		return 0, false
	case s[0]&0xf8 == 0xf0:
		return 4, s[1]&0xc0 == 0x80 && s[2]&0xc0 == 0x80 && s[3]&0xc0 == 0x80
	}
	return 0, false
}

// isCPunct is ispunct in the C locale.
func isCPunct(c byte) bool {
	return c > 0x20 && c < 0x7f && !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z')
}
