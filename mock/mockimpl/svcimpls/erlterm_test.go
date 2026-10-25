package svcimpls

import (
	"strings"
	"testing"
)

// TestErlFormat compares erlFormat with io_lib:format of OTP 22.3, the
// runtime of Couchbase Server 7.0.5.
func TestErlFormat(t *testing.T) {
	tests := []struct {
		format string
		args   []string
		want   string
	}{
		{"Invalid value ~p : ~p", []string{"a b", "X"}, "Invalid value \"a b\" : \"X\""},
		{"Invalid value ~p : ~p", []string{"a\"b\\c\n\t\r\v\b\f\x1b", "X"},
			"Invalid value \"a\\\"b\\\\c\\n\\t\\r\\v\\b\\f\\e\" : \"X\""},
		{"Invalid value ~p : ~p", []string{"\xc3\xa9\xa0\xff", "X"}, "Invalid value \"\xc3\xa9\xa0\xff\" : \"X\""},
		{"Invalid value ~p : ~p", []string{"a\x7fb", "X"}, "Invalid value [97,127,98] : \"X\""},
		{"Invalid value ~p : ~p", []string{"\xc2\x80", "X"}, "Invalid value [194,128] : \"X\""},
		{"Invalid value ~p : ~p", []string{strings.Repeat("a", 90) + "\x01", "X"},
			"Invalid value [97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,\n" +
				"               97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,\n" +
				"               97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,\n" +
				"               97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,97,\n" +
				"               97,97,97,97,97,97,1] : \"X\""},
		{"Collection with name ~p in scope ~p is not found", []string{strings.Repeat("x\x01", 30), strings.Repeat("\x01", 40)},
			"Collection with name [120,1,120,1,120,1,120,1,120,1,120,1,120,1,120,1,120,1,\n" +
				"                      120,1,120,1,120,1,120,1,120,1,120,1,120,1,120,1,120,1,\n" +
				"                      120,1,120,1,120,1,120,1,120,1,120,1,120,1,120,1,120,1,\n" +
				"                      120,1,120,1,120,1] in scope [1,1,1,1,1,1,1,1,1,1,1,1,1,\n" +
				"                                                   1,1,1,1,1,1,1,1,1,1,1,1,1,\n" +
				"                                                   1,1,1,1,1,1,1,1,1,1,1,1,1,\n" +
				"                                                   1] is not found"},
		{"Scope with name ~p is not found", []string{""}, "Scope with name [] is not found"},
	}
	for _, tt := range tests {
		if got := erlFormat(tt.format, tt.args...); got != tt.want {
			t.Errorf("erlFormat(%q, %q):\n got %q\nwant %q", tt.format, tt.args, got, tt.want)
		}
	}
}

// TestErlJSONString compares erlJSONString with the strings ns_server 7.0.5
// sends for keys holding those bytes.
func TestErlJSONString(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"a\x01b", `"a\u0001b"`},
		{"a\x1f\x1bb", `"a\u001F\u001Bb"`},
		{"a\x7f/<\xffb", "\"a\x7f/<\xffb\""},
		{"a\t\n\r\b\f\"\\b", `"a\t\n\r\b\f\"\\b"`},
		{"k\xe2\x80\xa8", "\"k\xe2\x80\xa8\""},
	}
	for _, tt := range tests {
		if got := erlJSONString(tt.in); got != tt.want {
			t.Errorf("erlJSONString(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
