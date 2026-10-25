// Derived from subjson, Copyright 2015 Couchbase, Inc., licensed under the
// Apache License, Version 2.0.
//
// matchContinue and pathMatch port jsonsl__match_continue and
// jsonsl_path_match from jsonsl, under the MIT notice in jsonsl.go.

package kvproc

import (
	"bytes"
	"strconv"
)

// This file ports how subjson finds a path in a document
// (subdoc/match.cc, subdoc/hkesc.h and subdoc/uescape.h in subjson@v7.0.5).

// jsonsl match results. JSONSL_MATCH_UNKNOWN has the value of NOMATCH.
const (
	jsonMatchComplete     = 1
	jsonMatchPossible     = 0
	jsonMatchNoMatch      = -1
	jsonMatchTypeMismatch = -2
)

// subdocLoc is a span of the document.
type subdocLoc struct {
	at, length int
}

func (l subdocLoc) end() int {
	return l.at + l.length
}

// subdocMatch is subjson's Match.
type subdocMatch struct {
	typ                  uint32
	status               jsonError
	matchres             int
	sflags               uint32
	matchLevel           int
	position             int
	numSiblings          int
	numChildren          uint64
	immediateParentFound bool
	getLast              bool
	followingSiblings    bool
	uniqueItemFound      bool
	locDeepest           subdocLoc
	locKey               subdocLoc
	ensureUnique         []byte
}

func (m *subdocMatch) isLast() bool {
	return m.numSiblings != 0 && m.position == m.numSiblings
}

// jprComp is a component of a path as jsonsl matches it; the first is the
// root.
type jprComp struct {
	root, numeric, neg bool
	key                string
	idx                int
}

func jprPath(comps []SubDocPathComponent) []jprComp {
	path := make([]jprComp, 0, len(comps)+1)
	path = append(path, jprComp{root: true})
	for _, comp := range comps {
		if comp.Path != "" {
			path = append(path, jprComp{key: comp.Path})
		} else {
			path = append(path, jprComp{numeric: true, idx: comp.ArrayIndex, neg: comp.ArrayIndex == -1})
		}
	}
	return path
}

type matchContext struct {
	doc   []byte
	path  []jprComp
	match *subdocMatch

	hkBegin, hkLen int
	hkEsc          bool
	hkValid        bool
	hkStr          []byte
	uniqueBegin    int
}

// key returns the key last read, with its \u escapes decoded as subjson's
// HashKey::get_hk does when the key has any escape.
func (ctx *matchContext) key() []byte {
	raw := ctx.doc[ctx.hkBegin : ctx.hkBegin+ctx.hkLen]
	if !ctx.hkEsc {
		return raw
	}
	if !ctx.hkValid {
		ctx.hkStr = decodeUescapes(raw)
		ctx.hkValid = true
	}
	return ctx.hkStr
}

func lastState(j *jsonLexer, st *jsonState) *jsonState {
	if st.level > 1 {
		return &j.stack[st.level-1]
	}
	return nil
}

func (ctx *matchContext) matchContinue(prlevel int, chtype uint32) int {
	if prlevel == len(ctx.path)-1 {
		return jsonMatchComplete
	}
	next := ctx.path[prlevel+1]
	switch chtype {
	case jsonTList:
		if next.numeric {
			return jsonMatchPossible
		}
	case jsonTObject:
		if !next.numeric {
			return jsonMatchPossible
		}
	}
	return jsonMatchTypeMismatch
}

// pathMatch is jsonsl_path_match.
func (ctx *matchContext) pathMatch(parent, child *jsonState, key []byte) int {
	if parent == nil {
		return ctx.matchContinue(0, child.typ)
	}
	comp := ctx.path[parent.level]
	if parent.typ == jsonTObject {
		if comp.key != string(key) {
			return jsonMatchNoMatch
		}
	} else if uint64(comp.idx) != parent.nelem-1 {
		return jsonMatchNoMatch
	}
	return ctx.matchContinue(parent.level, child.typ)
}

func (ctx *matchContext) push(j *jsonLexer, _ byte, st *jsonState, at int) {
	m := ctx.match
	if st.typ == jsonTHkey {
		ctx.hkBegin = at + 1
		ctx.hkValid = false
		return
	}
	parent := lastState(j, st)
	if parent != nil && parent.mres != jsonMatchPossible {
		return
	}
	var key []byte
	if parent != nil && parent.typ == jsonTObject {
		key = ctx.key()
	}
	st.mres = ctx.pathMatch(parent, st, key)
	switch st.mres {
	case jsonMatchComplete:
		m.matchres = jsonMatchComplete
		m.typ = st.typ
		m.locDeepest.at = at
		m.matchLevel = st.level
		if parent != nil && parent.typ == jsonTObject {
			m.locKey = subdocLoc{at: ctx.hkBegin - 1, length: ctx.hkLen + 2}
			m.position = int(parent.nelem/2) - 1
		} else if parent != nil && parent.typ == jsonTList {
			m.position = int(parent.nelem) - 1
		}
		if m.ensureUnique != nil {
			if st.typ != jsonTList {
				m.matchres = jsonMatchTypeMismatch
				j.stopped = true
				return
			}
			j.push = ctx.unique
			j.pop = ctx.unique
			j.maxCallbackLevel = st.level + 2
		}
	case jsonMatchNoMatch:
		st.ignoreCallback = true
	case jsonMatchPossible:
		m.matchLevel = st.level
		m.locDeepest.at = at
	case jsonMatchTypeMismatch:
		st.ignoreCallback = true
		m.matchres = jsonMatchTypeMismatch
	}
}

func (ctx *matchContext) pop(j *jsonLexer, _ byte, st *jsonState, _ int) {
	m := ctx.match
	if st.typ == jsonTHkey {
		ctx.hkLen = st.posCur - (st.posBegin + 1)
		ctx.hkEsc = st.nescapes > 0
		return
	}

	if st.mres == jsonMatchComplete {
		m.locDeepest.length = j.pos - st.posBegin
		m.immediateParentFound = true
		m.numChildren = st.nelem
		if st.typ != jsonTSpecial {
			m.locDeepest.length++
		} else {
			m.sflags = st.specialFlags
		}

		if m.getLast {
			if st.typ != jsonTList {
				m.matchres = jsonMatchTypeMismatch
				j.stopped = true
				return
			}
			if st.nelem == 0 {
				m.matchres = jsonMatchNoMatch
				j.stopped = true
				return
			}
			child := &j.stack[st.level+1]
			m.locDeepest = subdocLoc{at: j.base + child.posBegin, length: j.pos - child.posBegin}
			for m.locDeepest.length > 0 && isCSpace(ctx.doc[m.locDeepest.end()-1]) {
				m.locDeepest.length--
			}
			m.matchLevel = child.level
			m.sflags = child.specialFlags
			m.typ = child.typ
			m.numChildren = child.nelem
			m.numSiblings = int(st.nelem) - 1
			m.position = m.numSiblings
			m.locKey = subdocLoc{}
			j.stopped = true
			return
		}
		if m.followingSiblings {
			j.push = nil
			j.pop = nil
			j.any = ctx.nextSibling
		} else {
			j.stopped = true
		}
		return
	}

	if st.typ != jsonTObject && st.typ != jsonTList {
		return
	}
	if st.mres != jsonMatchPossible {
		return
	}
	m.locDeepest.length = j.pos - st.posBegin + 1
	if st.typ == jsonTObject {
		m.numSiblings = int(st.nelem / 2)
	} else {
		m.numSiblings = int(st.nelem)
	}
	m.typ = st.typ
	if st.level == len(ctx.path)-1 {
		m.immediateParentFound = true
	}
	j.stopped = true
}

func (ctx *matchContext) nextSibling(j *jsonLexer, action byte, st *jsonState, _ int) {
	m := ctx.match
	parent := st
	if action != jsonActionPop {
		parent = lastState(j, st)
	}
	if parent != nil && parent.mres == jsonMatchPossible {
		if parent.typ == jsonTObject {
			n := parent.nelem
			if action == jsonActionPush {
				n++
			}
			m.numSiblings = int(n / 2)
		} else if parent.typ == jsonTList {
			m.numSiblings = int(parent.nelem)
		}
		m.numSiblings--
	}
	j.stopped = true
}

func (ctx *matchContext) unique(j *jsonLexer, action byte, st *jsonState, at int) {
	m := ctx.match
	if action == jsonActionPush {
		ctx.uniqueBegin = at
		return
	}
	if st.mres == jsonMatchComplete {
		j.push = ctx.push
		j.pop = ctx.pop
		j.maxCallbackLevel = st.level + 1
		ctx.pop(j, action, st, at)
		return
	}

	slen := st.posCur - st.posBegin
	var equal bool
	switch st.typ {
	case jsonTString:
		slen++
		if len(m.ensureUnique) < 2 || slen != len(m.ensureUnique) {
			return
		}
		equal = bytes.Equal(ctx.doc[ctx.uniqueBegin+1:ctx.uniqueBegin+slen-1], m.ensureUnique[1:slen-1])
	case jsonTSpecial:
		if len(m.ensureUnique) != slen {
			return
		}
		equal = bytes.Equal(ctx.doc[ctx.uniqueBegin:ctx.uniqueBegin+slen], m.ensureUnique)
	default:
		m.matchres = jsonMatchTypeMismatch
		j.stopped = true
		return
	}
	if equal {
		m.uniqueItemFound = true
		j.stopped = true
	}
}

// isCSpace is isspace in the C locale.
func isCSpace(c byte) bool {
	return c == ' ' || (c >= '\t' && c <= '\r')
}

// execMatchSimple is Match::exec_match_simple on doc[start:start+length].
func execMatchSimple(doc []byte, start, length int, path []jprComp, m *subdocMatch) {
	ctx := &matchContext{doc: doc, path: path, match: m}
	j := newJSONLexer()
	j.callHkey = true
	j.push = ctx.push
	j.pop = ctx.pop
	j.maxCallbackLevel = len(path) + 1
	j.feed(doc[start:start+length], start)
	m.status = j.err
}

// execMatch is Match::exec_match.
func execMatch(doc []byte, path []jprComp, m *subdocMatch) {
	hasNeg := false
	for _, comp := range path {
		hasNeg = hasNeg || comp.neg
	}
	if !hasNeg {
		execMatchSimple(doc, 0, len(doc), path, m)
		return
	}

	curStart := 1
	levelOffset := 0
	lastStart, lastLen := 0, len(doc)
	for curStart < len(path) {
		ii := curStart
		isLastNeg := false
		for ; ii < len(path); ii++ {
			if path[ii].neg {
				isLastNeg = true
				break
			}
		}
		tmp := append([]jprComp{{root: true}}, path[curStart:ii]...)
		*m = subdocMatch{}
		m.getLast = isLastNeg
		execMatchSimple(doc, lastStart, lastLen, tmp, m)
		m.matchLevel += levelOffset
		if levelOffset != 0 {
			m.matchLevel--
		}
		levelOffset = m.matchLevel
		if m.status != jsonOK {
			return
		}
		if m.matchres != jsonMatchComplete {
			break
		}
		lastStart, lastLen = m.locDeepest.at, m.locDeepest.length
		curStart = ii + 1
	}
	m.immediateParentFound = m.matchLevel >= len(path)-1
}

// decodeUescapes decodes the \u escapes of a key as subjson's
// UescapeConverter does, leaving other escapes as they are. It treats any
// "\u" as an escape, even after a backslash, and stops at the first escape
// it cannot decode, returning what it decoded before it.
func decodeUescapes(in []byte) []byte {
	out := make([]byte, 0, len(in))
	var high uint16
	for i := 0; i < len(in); i++ {
		if in[i] != '\\' || i == len(in)-1 || in[i+1] != 'u' {
			out = append(out, in[i])
			continue
		}
		if len(in)-(i+2) < 4 {
			return out
		}
		v, err := strconv.ParseUint(string(in[i+2:i+6]), 16, 16)
		if err != nil {
			return out
		}
		r := uint16(v)
		switch {
		case r == 0:
			return out
		case high != 0:
			if r < 0xdc00 || r > 0xdfff {
				return out
			}
			out = appendUTF8(out, (uint32(high&0x3ff)<<10|uint32(r&0x3ff))+0x10000)
			high = 0
		case r < 0xd800 || r > 0xdfff:
			out = appendUTF8(out, uint32(r))
		case r < 0xdc00:
			high = r
		default:
			return out
		}
		i += 5
	}
	return out
}

// appendUTF8 is UescapeConverter::append_utf8.
func appendUTF8(out []byte, pt uint32) []byte {
	switch {
	case pt < 0x80:
		return append(out, byte(pt))
	case pt < 0x800:
		return append(out, byte(pt>>6|0xc0), byte(pt&0x3f|0x80))
	case pt < 0x10000:
		return append(out, byte(pt>>12|0xe0), byte(pt>>6&0x3f|0x80), byte(pt&0x3f|0x80))
	}
	return append(out, byte(pt>>18|0xf0), byte(pt>>12&0x3f|0x80), byte(pt>>6&0x3f|0x80), byte(pt&0x3f|0x80))
}
