// Derived from subjson, Copyright 2015 Couchbase, Inc., licensed under the
// Apache License, Version 2.0.

package kvproc

import (
	"math"
	"strings"
)

// SubDocPathComponent represents one part of a sub-document path.
type SubDocPathComponent struct {
	Path       string
	ArrayIndex int
}

// subdocMaxPathComponents is subjson's Limits::MAX_COMPONENTS, which counts
// the root of the document as a component.
const subdocMaxPathComponents = 32

// subdocPathEscapes are the characters subjson allows after a '\' in a key
// (allowed_json_escapes in subdoc/path.cc@v7.0.5), other than 'u', which it
// rejects.
const subdocPathEscapes = `"/\\bfnrt`

// ParseSubDocPath splits a sub-document path into components as subjson's
// Path::parse (subdoc/path.cc@v7.0.5) does. It returns ErrSdPathTooBig for a
// path of more components than subjson allows, and ErrSdPathInvalid for any
// other path subjson rejects.
func ParseSubDocPath(path string) ([]SubDocPathComponent, error) {
	parts := make([]SubDocPathComponent, 0)
	for i := 0; i < len(path); {
		if path[i] == '[' {
			end := strings.IndexByte(path[i+1:], ']')
			if end < 0 {
				return nil, ErrSdPathInvalid
			}
			index, ok := parseSubDocIndex(path[i+1 : i+1+end])
			if !ok {
				return nil, ErrSdPathInvalid
			}
			if len(parts)+1 == subdocMaxPathComponents {
				return nil, ErrSdPathTooBig
			}
			parts = append(parts, SubDocPathComponent{ArrayIndex: index})
			i += end + 2
			if i < len(path) {
				switch path[i] {
				case '[':
				case '.':
					i++
				default:
					return nil, ErrSdPathInvalid
				}
			}
			continue
		}

		key, consumed, err := parseSubDocKey(path[i:])
		if err != nil {
			return nil, err
		}
		if len(parts)+1 == subdocMaxPathComponents {
			return nil, ErrSdPathTooBig
		}
		if key == "" {
			return nil, ErrSdPathInvalid
		}
		parts = append(parts, SubDocPathComponent{Path: key})
		i += consumed
	}
	return parts, nil
}

// parseSubDocKey parses the key path starts with as subjson's parse_string
// and add_str_component do, and returns it and the number of bytes it
// consumed, including a '.' that ends it. Inside backticks '.', '[' and ']'
// are part of the key, and a doubled backtick stands for one. Escapes are
// kept: subjson compares the key with a document key whose \u escapes alone
// are decoded. The key is empty when nothing precedes the '.' or '[' that
// ends it, which the caller rejects after checking the number of
// components, in subjson's order.
func parseSubDocKey(path string) (string, int, error) {
	inBackticks := false
	inEscape := false
	backticks := 0
	end := len(path)
	consumed := len(path)
	for i := 0; i < len(path); i++ {
		c := path[i]
		if inEscape {
			if strings.IndexByte(subdocPathEscapes, c) < 0 {
				return "", 0, ErrSdPathInvalid
			}
			inEscape = false
		} else if c == '\\' {
			inEscape = true
		} else if c == '"' || c < 0x1f {
			return "", 0, ErrSdPathInvalid
		}

		if c == '`' {
			backticks++
			inBackticks = !inBackticks
		}
		if inBackticks {
			continue
		}

		if c == ']' {
			return "", 0, ErrSdPathInvalid
		}
		if c == '[' || c == '.' {
			end = i
			consumed = i
			if c == '.' {
				consumed++
			}
			break
		}
	}
	if inBackticks || inEscape {
		return "", 0, ErrSdPathInvalid
	}

	raw := path[:end]
	if len(raw) > 1 && raw[0] == '`' && raw[len(raw)-1] == '`' {
		raw = raw[1 : len(raw)-1]
		backticks -= 2
	}
	if backticks != 0 {
		var unquoted strings.Builder
		for i := 0; i < len(raw); i++ {
			if raw[i] != '`' {
				unquoted.WriteByte(raw[i])
			} else if i+1 < len(raw) && raw[i+1] == '`' {
				unquoted.WriteByte('`')
				i++
			}
		}
		raw = unquoted.String()
	}
	return raw, consumed, nil
}

// parseSubDocIndex parses an array index as subjson's add_num_component
// (subdoc/path.cc@v7.0.5) does: decimal digits, an empty index meaning 0, or
// -1 for the last element. subjson accumulates the digits in a size_t,
// rejecting only a step that wraps below the previous value, and stores the
// result as a long: 2^64-1 becomes -1, the last element, and any other value
// above 2^63-1 an index no element has.
func parseSubDocIndex(s string) (int, bool) {
	if strings.HasPrefix(s, "-") {
		return -1, s == "-1"
	}
	var value uint64
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		next := value*10 + uint64(s[i]-'0')
		if next < value {
			return 0, false
		}
		value = next
	}
	switch index := int64(value); {
	case index == -1:
		return -1, true
	case index < 0:
		return math.MaxInt, true
	default:
		return int(index), true
	}
}
