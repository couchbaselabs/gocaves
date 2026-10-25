package svcimpls

import (
	"strconv"
	"strings"
)

// erlFormat formats like io_lib:format in ns_server 7.0 (OTP 22) for a
// format whose only directive is ~p, with each argument a string as
// mochiweb hands it over: a list of bytes.
func erlFormat(format string, args ...string) string {
	var out strings.Builder
	parts := strings.Split(format, "~p")
	for i, part := range parts {
		out.WriteString(part)
		if i < len(args) {
			text := out.String()
			col := len(text) - strings.LastIndexByte(text, '\n') - 1
			out.WriteString(erlPrettyBytes(args[i], col))
		}
	}
	return out.String()
}

// erlPrettyBytes prints a list of bytes as ~p does, starting at column col.
// A list io_lib:printable_latin1_list accepts is printed as a string;
// any other is printed as a list of integers, wrapped at the column where
// io_lib_pretty wraps it.
func erlPrettyBytes(s string, col int) string {
	if s == "" {
		return "[]"
	}
	if erlPrintableLatin1(s) {
		var out strings.Builder
		out.WriteByte('"')
		for i := 0; i < len(s); i++ {
			switch c := s[i]; c {
			case '"':
				out.WriteString(`\"`)
			case '\\':
				out.WriteString(`\\`)
			case '\n':
				out.WriteString(`\n`)
			case '\r':
				out.WriteString(`\r`)
			case '\t':
				out.WriteString(`\t`)
			case '\v':
				out.WriteString(`\v`)
			case '\b':
				out.WriteString(`\b`)
			case '\f':
				out.WriteString(`\f`)
			case 0x1b:
				out.WriteString(`\e`)
			default:
				out.WriteByte(c)
			}
		}
		out.WriteByte('"')
		return out.String()
	}

	elems := make([]string, len(s))
	flatLen := 1
	for i := 0; i < len(s); i++ {
		elems[i] = strconv.Itoa(int(s[i]))
		flatLen += len(elems[i]) + 1
	}
	const lineLength = 78
	if col+flatLen <= lineLength {
		return "[" + strings.Join(elems, ",") + "]"
	}

	var out strings.Builder
	out.WriteByte('[')
	pos := col + 1
	for i, elem := range elems {
		if i == len(elems)-1 {
			elem += "]"
		} else {
			elem += ","
		}
		if i > 0 && pos+len(elem) > lineLength {
			out.WriteByte('\n')
			out.WriteString(strings.Repeat(" ", col+1))
			pos = col + 1
		}
		out.WriteString(elem)
		pos += len(elem)
	}
	return out.String()
}

func erlPrintableLatin1(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 0x20 && c <= 0x7e, c >= 0xa0:
		case c == '\n', c == '\r', c == '\t', c == '\v', c == '\b', c == '\f', c == 0x1b:
		default:
			return false
		}
	}
	return true
}

// erlJSONString encodes a binary as a JSON string as ns_server's encoder
// does: control characters are escaped, with upper-case hex digits, every
// other byte is copied, so a byte that is not UTF-8 is sent as it is.
func erlJSONString(s string) string {
	var out strings.Builder
	out.WriteByte('"')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '"':
			out.WriteString(`\"`)
		case '\\':
			out.WriteString(`\\`)
		case '\n':
			out.WriteString(`\n`)
		case '\r':
			out.WriteString(`\r`)
		case '\t':
			out.WriteString(`\t`)
		case '\b':
			out.WriteString(`\b`)
		case '\f':
			out.WriteString(`\f`)
		default:
			if c < 0x20 {
				const hexDigits = "0123456789ABCDEF"
				out.WriteString(`\u00`)
				out.WriteByte(hexDigits[c>>4])
				out.WriteByte(hexDigits[c&0xf])
			} else {
				out.WriteByte(c)
			}
		}
	}
	out.WriteByte('"')
	return out.String()
}
