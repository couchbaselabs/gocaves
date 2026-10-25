package mock

import "strconv"

// MochiwebUnquote decodes a query string key or value, or a path token, as
// ns_server's mochiweb_util:unquote does: "+" is a space, "%" followed by
// two hex digits is that byte, and any other "%" is kept.
func MochiwebUnquote(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '+':
			out = append(out, ' ')
		case c == '%' && i+2 < len(s) && isHexDigit(s[i+1]) && isHexDigit(s[i+2]):
			v, _ := strconv.ParseUint(s[i+1:i+3], 16, 8)
			out = append(out, byte(v))
			i += 2
		default:
			out = append(out, c)
		}
	}
	return string(out)
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}
