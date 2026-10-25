package mock

import (
	"reflect"
	"testing"
)

// TestParsePost compares ParsePost with the parameters ns_server 7.0.5
// reads from a form body.
func TestParsePost(t *testing.T) {
	form := "application/x-www-form-urlencoded"
	tests := []struct {
		contentType, body string
		want              []QSParam
	}{
		{form, "name=a&maxTTL=1", []QSParam{{"name", "a"}, {"maxTTL", "1"}}},
		{form + "; charset=UTF-8", "name=x;bad=2", []QSParam{{"name", "x"}, {"bad", "2"}}},
		{form, "&name=y&&foo&", []QSParam{{"", ""}, {"name", "y"}, {"", ""}, {"foo", ""}}},
		{form, "a+b%01=q%zz%&name=t4=x", []QSParam{{"a b\x01", "q%zz%"}, {"name", "t4=x"}}},
		{form, "name=a&name=b", []QSParam{{"name", "a"}, {"name", "b"}}},
		{"Application/X-WWW-Form-Urlencoded", "name=a", nil},
		{"application/json", `{"name":"a"}`, nil},
	}
	for _, tt := range tests {
		if got := ParsePost(tt.contentType, []byte(tt.body)); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("ParsePost(%q, %q) = %q, want %q", tt.contentType, tt.body, got, tt.want)
		}
	}
}
