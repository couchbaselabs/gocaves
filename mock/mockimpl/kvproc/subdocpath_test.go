package kvproc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func testOnePath(t *testing.T, path string, output []SubDocPathComponent) {
	paths, err := ParseSubDocPath(path)
	if err != nil {
		t.Fatalf("failed to parse path `%s`: %s", path, err)
	}

	assert.Equal(t, output, paths)
}

func TestSubdocParsing(t *testing.T) {
	testOnePath(t, "a", []SubDocPathComponent{
		{"a", 0},
	})

	testOnePath(t, "[4]", []SubDocPathComponent{
		{"", 4},
	})

	testOnePath(t, "a.b.c", []SubDocPathComponent{
		{"a", 0},
		{"b", 0},
		{"c", 0},
	})

	testOnePath(t, "a[4]", []SubDocPathComponent{
		{"a", 0},
		{"", 4},
	})

	testOnePath(t, "a.d[4].e", []SubDocPathComponent{
		{"a", 0},
		{"d", 0},
		{"", 4},
		{"e", 0},
	})
}
