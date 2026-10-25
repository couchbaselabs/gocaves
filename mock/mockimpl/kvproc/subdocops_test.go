package kvproc

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// runSubdocOp runs one operation of the subjson port on doc and returns the
// new document, or the result of a lookup, and the error.
func runSubdocOp(doc []byte, path string, value []byte, op byte, mkdirP bool) ([]byte, error) {
	comps, err := ParseSubDocPath(path)
	if err != nil {
		return nil, err
	}
	o := newSubdocOperation(doc, comps, value, mkdirP)
	switch op % 10 {
	case 0:
		return o.get()
	case 1:
		return o.containerSize()
	case 2:
		return o.storeDict(dictAdd)
	case 3:
		return o.storeDict(dictUpsert)
	case 4:
		return o.storeDict(dictReplace)
	case 5:
		return o.remove()
	case 6:
		return o.prepend()
	case 7:
		return o.appendValue(op%20 >= 10)
	case 8:
		return o.insert()
	}
	out, _, err := o.counter()
	return out, err
}

func TestSubdocOperations(t *testing.T) {
	tests := []struct {
		name   string
		doc    string
		path   string
		value  string
		op     byte
		mkdirP bool
		want   string
		err    error
	}{
		{name: "get keeps text", doc: `{"a": [ 1 ,2 ] }`, path: "a", want: `[ 1 ,2 ]`},
		{name: "get last element", doc: `[1, 2 ]`, path: "[-1]", want: `2`},
		{name: "get last of nested", doc: `[[1],[2, 3]]`, path: "[-1][-1]", want: `3`},
		{name: "get last of empty", doc: `{"a":[]}`, path: "a[-1]", err: ErrSdPathNotFound},
		{name: "last of a scalar root", doc: `5`, path: "[-1]", want: ``},
		{name: "string root", doc: `"a"`, path: "a", err: ErrSdNotJSON},
		{name: "decoded key", doc: `{"\u00e9":1}`, path: "é", want: `1`},
		{name: "count object", doc: `{"a":{"b":1,"c":2}}`, path: "a", op: 1, want: `2`},
		{name: "count scalar", doc: `{"a":1}`, path: "a", op: 1, err: ErrSdPathMismatch},
		{name: "add to object", doc: `{"a":1 }`, path: "b", value: `2`, op: 2, want: `{"a":1 ,"b":2}`},
		{name: "add to empty object", doc: `{ }`, path: "b", value: `2`, op: 2, want: `{ "b":2}`},
		{name: "add existing", doc: `{"a":1}`, path: "a", value: `2`, op: 2, err: ErrSdPathExists},
		{name: "add at index", doc: `{"a":[1]}`, path: "a[0]", value: `2`, op: 2, err: ErrSdPathInvalid},
		{name: "add with create path", doc: `{"a":1}`, path: "b.c.d", value: `2`, op: 2, mkdirP: true, want: `{"a":1,"b":{"c":{"d":2}}}`},
		{name: "create path under an index", doc: `{}`, path: "a[0].b", value: `2`, op: 3, mkdirP: true, err: ErrSdPathNotFound},
		{name: "replace array element", doc: `[1,2]`, path: "[1]", value: `9`, op: 4, want: `[1,9]`},
		{name: "replace root", doc: `[1]`, path: "", value: `9`, op: 4, err: ErrSdCantInsert},
		{name: "remove middle", doc: `[1, 2, 3]`, path: "[1]", op: 5, want: `[1,  3]`},
		{name: "remove last", doc: `{"a":1, "b":2}`, path: "b", op: 5, want: `{"a":1}`},
		{name: "remove only", doc: `{"a":1}`, path: "a", op: 5, want: `{}`},
		{name: "prepend", doc: `{"a":[ 1]}`, path: "a", value: `0`, op: 6, want: `{"a":[ 0,1]}`},
		{name: "prepend to empty", doc: `{"a":[ ]}`, path: "a", value: `0`, op: 6, want: `{"a":[0]}`},
		{name: "append to root", doc: "[1 ] \n", path: "", value: `2`, op: 7, want: `[1,2]`},
		{name: "append to object root", doc: `{}`, path: "", value: `2`, op: 7, err: ErrSdPathMismatch},
		{name: "append numbers without comma", doc: `[0]`, path: "", value: `1 2`, op: 7, want: `[0,1 2]`},
		{name: "append trailing comma", doc: `[0]`, path: "", value: `1,`, op: 7, err: ErrSdCantInsert},
		{name: "unique exists", doc: `{"l":[1,"a"]}`, path: "l", value: `"a"`, op: 17, err: ErrSdPathExists},
		{name: "unique with container", doc: `{"l":[{},1]}`, path: "l", value: `1`, op: 17, err: ErrSdPathMismatch},
		{name: "unique container value", doc: `{"l":[]}`, path: "l", value: `[]`, op: 17, err: ErrSdCantInsert},
		{name: "insert at end", doc: `[1]`, path: "[1]", value: `2`, op: 8, want: `[1,2]`},
		{name: "insert past end", doc: `[1]`, path: "[2]", value: `2`, op: 8, err: ErrSdPathNotFound},
		{name: "insert negative", doc: `[1]`, path: "[-1]", value: `2`, op: 8, err: ErrSdPathInvalid},
		{name: "counter", doc: `{"n":-5}`, path: "n", value: `2`, op: 9, want: `{"n":-3}`},
		{name: "counter float", doc: `{"n":1.0}`, path: "n", value: `2`, op: 9, err: ErrSdPathMismatch},
		{name: "counter overflow", doc: `{"n":9223372036854775807}`, path: "n", value: `1`, op: 9, err: ErrSdCantInsert},
		{name: "counter out of range", doc: `{"n":9223372036854775808}`, path: "n", value: `1`, op: 9, err: ErrSdNumRange},
		{name: "counter delta", doc: `{"n":1}`, path: "n", value: `-9223372036854775808`, op: 9, err: ErrSdDeltaInvalid},
		{name: "counter missing", doc: `{}`, path: "n", value: `2`, op: 9, want: `{"n":2}`},
		{name: "deep document", doc: strings.Repeat("[", 33) + strings.Repeat("]", 33), path: "[0]", err: ErrSdDocTooDeep},
		{name: "deep value", doc: `{}`, path: "a", value: strings.Repeat("[", 32) + strings.Repeat("]", 32), op: 3, err: ErrSdValueTooDeep},
		{name: "unterminated root", doc: ` `, path: "a", value: `0`, op: 3, mkdirP: true, err: ErrSdNotJSON},
		{name: "unterminated string", doc: `{"x":"}`, path: "x", value: `0`, op: 7, mkdirP: true, err: ErrSdPathMismatch},
		{name: "unterminated array", doc: `{"x":[`, path: "x", value: `0`, op: 7, mkdirP: true, err: ErrSdNotJSON},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := runSubdocOp([]byte(test.doc), test.path, []byte(test.value), test.op, test.mkdirP)
			if test.err != nil {
				assert.ErrorIs(t, err, test.err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, test.want, string(got))
		})
	}
}

func TestDecodeUescapes(t *testing.T) {
	for in, want := range map[string]string{
		`a\u0041`:       "aA",
		`\u00e9`:        "é",
		`\ud83d\ude00x`: "😀x",
		`a\"b`:          `a\"b`,
		`a\\u0041`:      `a\A`,
		`a\u004`:        "a",
		`a\u00g1b`:      "a",
		`a\u0000b`:      "a",
		`a\ud83dx`:      "ax",
		`a\ud83d\u0041`: "a",
		`a\ude00b`:      "a",
	} {
		assert.Equal(t, want, string(decodeUescapes([]byte(in))), in)
	}
}

func TestParseSubDocIndex(t *testing.T) {
	for in, want := range map[string]int{
		"":                     0,
		"007":                  7,
		"-1":                   -1,
		"18446744073709551615": -1,
		"9223372036854775808":  math.MaxInt,
		"30000000000000000000": math.MaxInt,
	} {
		got, ok := parseSubDocIndex(in)
		assert.True(t, ok, in)
		assert.Equal(t, want, got, in)
	}
	for _, in := range []string{"-2", "+1", " 1", "1a", "18446744073709551616"} {
		_, ok := parseSubDocIndex(in)
		assert.False(t, ok, in)
	}
}

// jsonsl accepts what its lexer can tokenise, which is not quite JSON.
func TestJSONLexer(t *testing.T) {
	lex := func(doc string) jsonError {
		j := newJSONLexer()
		j.feed([]byte(doc), 0)
		return j.err
	}
	for _, doc := range []string{`[1 2]`, `[-01]`, `{"a":1}`, `["\u12"]`, "[\"\x14\"]", `[1`, ` `} {
		assert.Equal(t, jsonOK, lex(doc), doc)
	}
	for _, doc := range []string{`""`, `["a" 1]`, `[1,]`, `{"a" 1}`, `[01]`, `[tru]`, `[1.]`, `["\x"]`, "[\"\x01\"]", `[]]`} {
		assert.Equal(t, jsonErrSyntax, lex(doc), doc)
	}
	assert.Equal(t, jsonErrLevelsExceeded, lex(strings.Repeat("[", 33)))
}

var subdocFuzzSeeds = []struct {
	doc, path, value string
}{
	{`{"a":[1,{"b":2}],"\u00e9":3}`, "a[1].b", "4"},
	{`[1, 2 ]`, "[-1]", "0"},
	{`{"x":"}`, "x", "0"},
	{` `, "0", "0"},
	{`5`, "[-1]", "1"},
	{`{"a":{}}`, "a.b.c", `[1, 2]`},
}

func FuzzParseSubDocPath(f *testing.F) {
	for _, seed := range subdocFuzzSeeds {
		f.Add(seed.path)
	}
	f.Fuzz(func(t *testing.T, path string) {
		comps, err := ParseSubDocPath(path)
		if err == nil && len(comps) >= subdocMaxPathComponents {
			t.Fatalf("%q parsed into %d components", path, len(comps))
		}
		if err != nil && !errors.Is(err, ErrSdPathInvalid) && !errors.Is(err, ErrSdPathTooBig) {
			t.Fatalf("unexpected error %v", err)
		}
	})
}

func FuzzSubdocMatch(f *testing.F) {
	for _, seed := range subdocFuzzSeeds {
		f.Add([]byte(seed.doc), seed.path)
	}
	f.Fuzz(func(t *testing.T, doc []byte, path string) {
		comps, err := ParseSubDocPath(path)
		if err != nil {
			return
		}
		var m subdocMatch
		execMatch(doc, jprPath(comps), &m)
		for _, loc := range []subdocLoc{m.locDeepest, m.locKey} {
			if loc.at < 0 || loc.length < 0 || loc.end() > len(doc) {
				t.Fatalf("span %+v outside a %d-byte document", loc, len(doc))
			}
		}
	})
}

func FuzzSubdocOperation(f *testing.F) {
	for i, seed := range subdocFuzzSeeds {
		f.Add([]byte(seed.doc), seed.path, []byte(seed.value), byte(i), i%2 == 0)
	}
	f.Fuzz(func(t *testing.T, doc []byte, path string, value []byte, op byte, mkdirP bool) {
		got, err := runSubdocOp(doc, path, value, op, mkdirP)
		if err == nil && got == nil && op%10 > 1 {
			t.Fatalf("operation %d returned no document", op)
		}
	})
}

func TestCheckUTF8JSON(t *testing.T) {
	for _, doc := range []string{
		`{"a":1}`, ` 1 `, `"abc"`, `null`, `[1,{"b":[]}]`, `1e5`,
		"{\"a\":\"\xc0\x80\"}", "{\"a\":\"\xed\xa0\x80\"}", "{\"a\":\"\xf4\x90\x80\x80\"}",
		strings.Repeat("[", 10001) + strings.Repeat("]", 10001),
	} {
		assert.True(t, CheckUTF8JSON([]byte(doc)), doc)
	}
	for _, doc := range []string{
		``, ` `, `{"a":1}x`, `1 2`, `-01`, `[1,]`, `{"a"}`, `"\u12"`, "\"\x01\"",
		"{\"a\":\"\xff\"}", "{\"a\":\"\xc0\"}", "\"\xf8\x80\x80\x80\x80\"", "[1]]",
	} {
		assert.False(t, CheckUTF8JSON([]byte(doc)), doc)
	}
}
