package mock

import "strings"

// QSParam is a key and value of a query string or form body.
type QSParam struct {
	Key, Value string
}

// ParseQS splits a query string as mochiweb_util:parse_qs does: pairs are
// separated by "&" or ";", a key ends at "=", "&" or ";", and keys and
// values are decoded by MochiwebUnquote. A duplicated key is kept.
func ParseQS(s string) []QSParam {
	var params []QSParam
	for s != "" {
		end := strings.IndexAny(s, "=&;")
		if end < 0 {
			end = len(s)
		}
		key := s[:end]
		s = strings.TrimPrefix(s[end:], "=")
		end = strings.IndexAny(s, "&;")
		if end < 0 {
			end = len(s)
		}
		value := s[:end]
		s = s[end:]
		if s != "" {
			s = s[1:]
		}
		params = append(params, QSParam{MochiwebUnquote(key), MochiwebUnquote(value)})
	}
	return params
}

// ParsePost returns the parameters of a form body as
// mochiweb_request:parse_post does: only for a Content-Type that starts
// with application/x-www-form-urlencoded, compared case-sensitively.
func ParsePost(contentType string, body []byte) []QSParam {
	if !strings.HasPrefix(contentType, "application/x-www-form-urlencoded") {
		return nil
	}
	return ParseQS(string(body))
}
