package mock

import "testing"

// TestMochiwebUnquote compares MochiwebUnquote with the names ns_server
// 7.0.5 decodes from a request body or path.
func TestMochiwebUnquote(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"a+b", "a b"},
		{"%41%4a", "AJ"},
		{"q%zz4", "q%zz4"},
		{"t%2", "t%2"},
		{"t6%", "t6%"},
		{"na%6De", "name"},
	}
	for _, tt := range tests {
		if got := MochiwebUnquote(tt.in); got != tt.want {
			t.Errorf("MochiwebUnquote(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
