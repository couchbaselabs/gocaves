// Ported from jsonsl, as included in subjson (contrib/jsonsl).
//
// Copyright (C) 2012-2015 Mark Nunberg.
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package kvproc

// This file ports the streaming JSON lexer subjson parses documents and
// values with (contrib/jsonsl/jsonsl.c in subjson@v7.0.5), so that
// sub-document operations accept, reject and locate exactly what kv_engine
// does. It reads only as far as its callbacks need.

// jsonsl state types.
const (
	jsonStringy  = 0xffff00
	jsonTString  = '"' | jsonStringy
	jsonTHkey    = '#' | jsonStringy
	jsonTObject  = '{'
	jsonTList    = '['
	jsonTSpecial = '^'
)

// jsonsl special flags.
const (
	jsonSpecialSigned   = 1 << 0
	jsonSpecialUnsigned = 1 << 1
	jsonSpecialTrue     = 1 << 2
	jsonSpecialFalse    = 1 << 3
	jsonSpecialNull     = 1 << 4
	jsonSpecialFloat    = 1 << 5
	jsonSpecialExponent = 1 << 6
	jsonSpecialZero     = 1<<9 | jsonSpecialUnsigned
	jsonSpecialDash     = 1 << 10
	jsonSpecialNumeric  = jsonSpecialSigned | jsonSpecialUnsigned
)

// jsonError is a jsonsl error; only whether a document is too deep matters
// to subjson.
type jsonError int

const (
	jsonOK jsonError = iota
	jsonErrLevelsExceeded
	jsonErrSyntax
)

// jsonParserDepth is subjson's Limits::PARSER_DEPTH.
const jsonParserDepth = subdocMaxPathComponents + 1

type jsonState struct {
	typ            uint32
	specialFlags   uint32
	posBegin       int
	posCur         int
	level          int
	nelem          uint64
	ignoreCallback bool
	nescapes       int
	// mres is the match result subjson keeps in the state.
	mres int
}

const (
	jsonActionPush = '+'
	jsonActionPop  = '-'
)

// jsonCallback is called with the state pushed or popped and the position of
// the character being read.
type jsonCallback func(j *jsonLexer, action byte, st *jsonState, at int)

type jsonLexer struct {
	stack     [jsonParserDepth]jsonState
	level     int
	pos       int
	canInsert bool
	expecting byte
	tokLast   byte
	inEscape  bool
	stopped   bool
	err       jsonError

	// base is the position in the document of the bytes fed.
	base             int
	maxCallbackLevel int
	callHkey         bool
	push, pop, any   jsonCallback
}

func newJSONLexer() *jsonLexer {
	j := &jsonLexer{}
	j.reset()
	return j
}

func (j *jsonLexer) reset() {
	j.tokLast = 0
	j.canInsert = true
	j.pos = 0
	j.level = 0
	j.stopped = false
	j.inEscape = false
	j.expecting = 0
	j.err = jsonOK
	for i := range j.stack {
		j.stack[i] = jsonState{level: i}
	}
}

func (j *jsonLexer) fail(err jsonError) bool {
	j.err = err
	return false
}

// callback runs the callback for st as jsonsl's DO_CALLBACK does, and
// reports whether parsing goes on.
func (j *jsonLexer) callback(action byte, st *jsonState) bool {
	if st.typ == jsonTHkey && !j.callHkey {
		return true
	}
	if j.maxCallbackLevel <= st.level || st.ignoreCallback {
		return true
	}
	at := j.base + j.pos
	switch {
	case action == jsonActionPush && j.push != nil:
		j.push(j, action, st, at)
	case action == jsonActionPop && j.pop != nil:
		j.pop(j, action, st, at)
	case j.any != nil:
		j.any(j, action, st, at)
	}
	return !j.stopped
}

func (j *jsonLexer) pushState() (*jsonState, bool) {
	if j.level >= jsonParserDepth-1 {
		return nil, j.fail(jsonErrLevelsExceeded)
	}
	ignore := j.stack[j.level].ignoreCallback
	j.level++
	st := &j.stack[j.level]
	st.ignoreCallback = ignore
	st.posBegin = j.pos
	return st, true
}

func (j *jsonLexer) callbackAndPop(st *jsonState) bool {
	st.posCur = j.pos
	if !j.callback(jsonActionPop, st) {
		return false
	}
	st.nescapes = 0
	j.level--
	j.stack[j.level].posCur = j.pos
	return true
}

// feed parses buf, which starts at position base of the document, from the
// state reset leaves. Positions in states are relative to buf.
func (j *jsonLexer) feed(buf []byte, base int) {
	j.base = base
	pos := j.pos
	for pos < len(buf) {
		// Skip the body of a string and digits of a number, as jsonsl's
		// str_fastparse and num_fastparse do.
		switch st := &j.stack[j.level]; {
		case st.typ&jsonStringy != 0 && !j.inEscape:
			for pos < len(buf) && !jsonStringNoPassthrough[buf[pos]] {
				pos++
			}
		case st.typ == jsonTSpecial && (st.specialFlags == jsonSpecialUnsigned || st.specialFlags == jsonSpecialSigned):
			for pos < len(buf) && isJSONDigit(buf[pos]) {
				pos++
			}
		}
		if pos == len(buf) {
			break
		}
		j.pos = pos
		if !j.step(buf[pos]) {
			return
		}
		pos = j.pos + 1
	}
	j.pos = pos
}

func (j *jsonLexer) step(c byte) bool {
	st := &j.stack[j.level]
	switch {
	case st.typ&jsonStringy != 0:
		if j.inEscape {
			j.inEscape = false
			if !jsonAllowedEscapes[c] {
				return j.fail(jsonErrSyntax)
			}
			return true
		}
		if !jsonStringNoPassthrough[c] {
			return true
		}
		if c == '"' {
			return j.quote(st)
		}
		if c == '\\' {
			return j.escape(st)
		}
		return j.fail(jsonErrSyntax)
	case st.typ == jsonTSpecial:
		return j.special(c, st)
	case isJSONWhitespace(c):
		return true
	case jsonSpecialTable[c] != 0:
		return j.specialBegin(c, st)
	}
	if c == '"' {
		return j.quote(st)
	}
	if c == '\\' {
		return j.escape(st)
	}
	return j.structural(c)
}

func (j *jsonLexer) quote(st *jsonState) bool {
	j.canInsert = false
	switch st.typ {
	case jsonTString, jsonTHkey:
		return j.callbackAndPop(st)
	case jsonTObject:
		st.nelem++
		if (st.nelem-1)%2 != 0 {
			if j.tokLast != ':' {
				return j.fail(jsonErrSyntax)
			}
			j.expecting = ','
			j.tokLast = 0
			child, ok := j.pushState()
			if !ok {
				return false
			}
			child.typ = jsonTString
			return j.callback(jsonActionPush, child)
		}
		if j.expecting != '"' {
			return j.fail(jsonErrSyntax)
		}
		j.tokLast = 0
		j.expecting = ':'
		child, ok := j.pushState()
		if !ok {
			return false
		}
		child.typ = jsonTHkey
		return j.callback(jsonActionPush, child)
	case jsonTList:
		st.nelem++
		child, ok := j.pushState()
		if !ok {
			return false
		}
		child.typ = jsonTString
		j.expecting = ','
		j.tokLast = 0
		return j.callback(jsonActionPush, child)
	default:
		return j.fail(jsonErrSyntax)
	}
}

func (j *jsonLexer) escape(st *jsonState) bool {
	if st.typ&jsonStringy == 0 {
		return j.fail(jsonErrSyntax)
	}
	st.nescapes++
	j.inEscape = true
	return true
}

func (j *jsonLexer) structural(c byte) bool {
	st := &j.stack[j.level]
	switch c {
	case ':':
		if j.expecting != c {
			return j.fail(jsonErrSyntax)
		}
		j.tokLast = ':'
		j.canInsert = true
		j.expecting = '"'
		return true
	case ',':
		if j.expecting != c {
			return j.fail(jsonErrSyntax)
		}
		if st.typ != jsonTObject {
			j.canInsert = true
		}
		j.tokLast = ','
		j.expecting = '"'
		return true
	case '{', '[':
		if !j.canInsert {
			return j.fail(jsonErrSyntax)
		}
		if st.nelem%2 == 0 && st.typ == jsonTObject {
			return j.fail(jsonErrSyntax)
		}
		st.nelem++
		child, ok := j.pushState()
		if !ok {
			return false
		}
		child.typ = uint32(c)
		child.nelem = 0
		j.canInsert = true
		if c == '{' {
			j.expecting = '"'
		}
		if !j.callback(jsonActionPush, child) {
			return false
		}
		j.tokLast = 0
		return true
	case '}', ']':
		if j.tokLast == ',' {
			return j.fail(jsonErrSyntax)
		}
		j.canInsert = false
		j.level--
		j.expecting = ','
		j.tokLast = 0
		if c == ']' {
			if st.typ != jsonTList {
				return j.fail(jsonErrSyntax)
			}
		} else if st.typ != jsonTObject || (st.nelem != 0 && st.nelem%2 != 0) {
			return j.fail(jsonErrSyntax)
		}
		if !j.callback(jsonActionPop, st) {
			return false
		}
		j.stack[j.level].posCur = j.pos
		return true
	default:
		return j.specialBegin(c, st)
	}
}

func (j *jsonLexer) specialBegin(c byte, st *jsonState) bool {
	if st.typ == jsonTSpecial {
		return true
	}
	flags := jsonSpecialTable[c]
	if flags == 0 {
		return j.fail(jsonErrSyntax)
	}
	if st.nelem%2 == 0 && st.typ == jsonTObject {
		return j.fail(jsonErrSyntax)
	}
	st.nelem++
	if !j.canInsert {
		return j.fail(jsonErrSyntax)
	}
	child, ok := j.pushState()
	if !ok {
		return false
	}
	child.typ = jsonTSpecial
	child.specialFlags = flags
	child.nescapes = 1
	if flags == jsonSpecialUnsigned {
		child.nelem = uint64(c - '0')
		j.tokLast = '1'
	} else {
		j.tokLast = '-'
		child.nelem = 0
	}
	return j.callback(jsonActionPush, child)
}

func isJSONDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func (j *jsonLexer) special(c byte, st *jsonState) bool {
	normal := st.specialFlags == jsonSpecialUnsigned || st.specialFlags == jsonSpecialSigned
	switch {
	case normal:
		if isJSONDigit(c) {
			return true
		}
		return j.specialNumeric(c, st)
	case st.specialFlags == jsonSpecialDash:
		if !isJSONDigit(c) {
			return j.fail(jsonErrSyntax)
		}
		if c == '0' {
			st.specialFlags = jsonSpecialZero | jsonSpecialSigned
		} else {
			st.specialFlags = jsonSpecialSigned
		}
		return true
	case st.specialFlags == jsonSpecialZero:
		if isJSONDigit(c) {
			return j.fail(jsonErrSyntax)
		}
		st.specialFlags = jsonSpecialUnsigned
		return j.specialNumeric(c, st)
	case st.specialFlags&jsonSpecialNumeric != 0:
		return j.specialNumeric(c, st)
	}
	if !jsonSpecialEndings[c] {
		st.nescapes++
		var literal string
		switch st.specialFlags {
		case jsonSpecialTrue:
			literal = "true"
		case jsonSpecialFalse:
			literal = "false"
		case jsonSpecialNull:
			literal = "null"
		}
		if literal != "" {
			at := j.pos - st.posBegin
			if at >= len(literal) || literal[at] != c {
				return j.fail(jsonErrSyntax)
			}
		}
		return true
	}
	return j.specialPop(c, st)
}

func (j *jsonLexer) specialNumeric(c byte, st *jsonState) bool {
	switch {
	case isJSONDigit(c):
		j.tokLast = '1'
		return true
	case c == '.':
		if st.specialFlags&jsonSpecialFloat != 0 {
			return j.fail(jsonErrSyntax)
		}
		st.specialFlags |= jsonSpecialFloat
		j.tokLast = '.'
		return true
	case c == 'e' || c == 'E':
		if st.specialFlags&jsonSpecialExponent != 0 {
			return j.fail(jsonErrSyntax)
		}
		st.specialFlags |= jsonSpecialExponent
		j.tokLast = 'e'
		return true
	case c == '-' || c == '+':
		if j.tokLast != 'e' {
			return j.fail(jsonErrSyntax)
		}
		j.tokLast = '-'
		return true
	case jsonSpecialEndings[c]:
		return j.specialPop(c, st)
	}
	return j.fail(jsonErrSyntax)
}

func (j *jsonLexer) specialPop(c byte, st *jsonState) bool {
	switch flags := st.specialFlags; {
	case flags == jsonSpecialUnsigned || flags == jsonSpecialSigned:
	case flags == jsonSpecialZero || flags == jsonSpecialZero|jsonSpecialSigned:
		st.specialFlags = jsonSpecialUnsigned
	case flags == jsonSpecialDash:
		return j.fail(jsonErrSyntax)
	case flags&jsonSpecialNumeric != 0:
		if j.tokLast != '1' {
			return j.fail(jsonErrSyntax)
		}
	case flags == jsonSpecialTrue:
		if st.nescapes != 4 {
			return j.fail(jsonErrSyntax)
		}
		st.nelem = 1
	case flags == jsonSpecialFalse:
		if st.nescapes != 5 {
			return j.fail(jsonErrSyntax)
		}
	case flags == jsonSpecialNull:
		if st.nescapes != 4 {
			return j.fail(jsonErrSyntax)
		}
	}
	if !j.callbackAndPop(st) {
		return false
	}
	j.expecting = ','
	j.tokLast = 0
	if isJSONWhitespace(c) {
		return true
	}
	return j.structural(c)
}

func isJSONWhitespace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

var (
	jsonSpecialTable        [256]uint32
	jsonSpecialEndings      [256]bool
	jsonStringNoPassthrough [256]bool
	jsonAllowedEscapes      [256]bool
)

func init() {
	jsonSpecialTable['-'] = jsonSpecialDash
	jsonSpecialTable['0'] = jsonSpecialZero
	for c := '1'; c <= '9'; c++ {
		jsonSpecialTable[c] = jsonSpecialUnsigned
	}
	jsonSpecialTable['f'] = jsonSpecialFalse
	jsonSpecialTable['n'] = jsonSpecialNull
	jsonSpecialTable['t'] = jsonSpecialTrue
	for _, c := range "\t\n\r \",:[\\]{}" {
		jsonSpecialEndings[c] = true
	}
	// jsonsl passes 0x14 to 0x1f through in strings.
	for c := 0; c <= 0x13; c++ {
		jsonStringNoPassthrough[c] = true
	}
	jsonStringNoPassthrough['"'] = true
	jsonStringNoPassthrough['\\'] = true
	for _, c := range "\"/\\bfnrtu" {
		jsonAllowedEscapes[c] = true
	}
}
