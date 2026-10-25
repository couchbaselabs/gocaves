// Derived from subjson, Copyright 2015 Couchbase, Inc., licensed under the
// Apache License, Version 2.0.

package kvproc

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
)

// This file ports subjson's sub-document operations
// (subdoc/operations.cc in subjson@v7.0.5). Each edits the text of the
// document: the result is the document with spans replaced, so the rest of
// the document keeps its text.

// subdocOperation is one operation on a document, as subjson's Operation.
type subdocOperation struct {
	doc    []byte
	path   []jprComp
	value  []byte
	mkdirP bool
	match  subdocMatch
}

func newSubdocOperation(doc []byte, comps []SubDocPathComponent, value []byte, mkdirP bool) *subdocOperation {
	return &subdocOperation{doc: doc, path: jprPath(comps), value: value, mkdirP: mkdirP}
}

// matchCommon is Operation::do_match_common.
func (o *subdocOperation) matchCommon(followingSiblings bool) error {
	o.match.followingSiblings = followingSiblings
	execMatch(o.doc, o.path, &o.match)
	switch {
	case o.match.matchres == jsonMatchTypeMismatch:
		return ErrSdPathMismatch
	case o.match.status == jsonErrLevelsExceeded:
		return ErrSdDocTooDeep
	case o.match.status != jsonOK:
		return ErrSdNotJSON
	}
	return nil
}

// Validator modes of subjson's Validator::validate.
const (
	validateParentArray = iota
	validateParentDict
)

// validate is Operation::validate: the value must be what subjson accepts in
// an array or an object, no deeper than maxdepth.
func (o *subdocOperation) validate(parent int, maxdepth int, primitive, single bool) error {
	var pre, post string
	if parent == validateParentArray {
		pre, post = "[", "]"
	} else {
		pre, post = `{"k":`, "}"
	}
	maxdepth++

	rootcount := 0
	failure := jsonOK
	j := newJSONLexer()
	j.maxCallbackLevel = maxdepth + 2
	j.any = func(j *jsonLexer, action byte, st *jsonState, _ int) {
		if st.level-1 == maxdepth {
			failure = jsonErrLevelsExceeded
			j.stopped = true
			return
		}
		if st.level == 1 {
			rootcount++
			return
		}
		if st.level != 2 || action != jsonActionPush {
			return
		}
		if primitive && (st.typ == jsonTObject || st.typ == jsonTList) {
			failure = jsonErrSyntax
			j.stopped = true
		}
		if single {
			wrapper := &j.stack[1]
			if (wrapper.typ == jsonTList && wrapper.nelem > 1) || (wrapper.typ == jsonTObject && wrapper.nelem > 2) {
				failure = jsonErrSyntax
				j.stopped = true
			}
		}
	}

	buf := make([]byte, 0, len(pre)+len(o.value)+len(post))
	buf = append(append(append(buf, pre...), o.value...), post...)
	j.feed(buf, 0)
	if failure == jsonOK {
		failure = j.err
	}
	if failure == jsonOK && rootcount != 2 {
		failure = jsonErrSyntax
	}
	switch failure {
	case jsonOK:
		return nil
	case jsonErrLevelsExceeded:
		return ErrSdValueTooDeep
	}
	return ErrSdCantInsert
}

// maxDepthNewKey and maxDepthParent are Operation::get_maxdepth for a value
// stored under a new key and for one stored in the container the path names.
func (o *subdocOperation) maxDepthNewKey() int {
	return subdocMaxPathComponents + 1 - len(o.path)
}

func (o *subdocOperation) maxDepthParent() int {
	return subdocMaxPathComponents - len(o.path)
}

func join(parts ...[]byte) []byte {
	n := 0
	for _, part := range parts {
		n += len(part)
	}
	out := make([]byte, 0, n)
	for _, part := range parts {
		out = append(out, part...)
	}
	return out
}

// container returns the span of the container the match ended in. One that
// never closed has no length; that document is not JSON.
func (o *subdocOperation) container() (subdocLoc, error) {
	loc := o.match.locDeepest
	if loc.length < 2 {
		return loc, ErrSdNotJSON
	}
	return loc, nil
}

func (o *subdocOperation) matched() []byte {
	loc := o.match.locDeepest
	return o.doc[loc.at:loc.end()]
}

// get is the match of GET and EXISTS.
func (o *subdocOperation) get() ([]byte, error) {
	if err := o.matchCommon(false); err != nil {
		return nil, err
	}
	if o.match.matchres != jsonMatchComplete {
		return nil, ErrSdPathNotFound
	}
	return o.matched(), nil
}

// containerSize is Operation::do_container_size.
func (o *subdocOperation) containerSize() ([]byte, error) {
	if err := o.matchCommon(false); err != nil {
		return nil, err
	}
	if o.match.matchres != jsonMatchComplete {
		return nil, ErrSdPathNotFound
	}
	size := o.match.numChildren
	if o.match.typ == jsonTObject {
		size /= 2
	} else if o.match.typ != jsonTList {
		return nil, ErrSdPathMismatch
	}
	return []byte(strconv.FormatUint(size, 10)), nil
}

// dictOp is the operation subjson's do_store_dict runs for.
type dictOp int

const (
	dictAdd dictOp = iota
	dictUpsert
	dictReplace
)

// storeDict is op_exec for DICT_ADD, DICT_UPSERT and REPLACE.
func (o *subdocOperation) storeDict(op dictOp) ([]byte, error) {
	// subjson cannot store a value in place of the root.
	if len(o.path) == 1 {
		return nil, ErrSdCantInsert
	}
	if err := o.validate(validateParentDict, o.maxDepthNewKey(), false, false); err != nil {
		return nil, err
	}
	if err := o.matchCommon(false); err != nil {
		return nil, err
	}
	return o.doStoreDict(op)
}

// doStoreDict is Operation::do_store_dict.
func (o *subdocOperation) doStoreDict(op dictOp) ([]byte, error) {
	m := &o.match
	if op != dictReplace && o.path[len(o.path)-1].numeric {
		return nil, ErrSdPathInvalid
	}
	if m.matchres != jsonMatchComplete {
		if op == dictReplace {
			return nil, ErrSdPathNotFound
		}
		if !m.immediateParentFound {
			if o.mkdirP {
				return o.mkdirPath(false)
			}
			return nil, ErrSdPathNotFound
		}
		parent, err := o.container()
		if err != nil {
			return nil, err
		}
		sep := []byte(`"`)
		if m.numSiblings != 0 {
			sep = []byte(`,"`)
		}
		return join(o.doc[:parent.end()-1], sep, []byte(o.path[len(o.path)-1].key), []byte(`":`), o.value, o.doc[parent.end()-1:]), nil
	}
	if op == dictAdd {
		return nil, ErrSdPathExists
	}
	loc := m.locDeepest
	return join(o.doc[:loc.at], o.value, o.doc[loc.end():]), nil
}

// mkdirPath is Operation::do_mkdir_p: the missing components are added as
// object keys under the deepest container found, holding the value or an
// array holding it.
func (o *subdocOperation) mkdirPath(inArray bool) ([]byte, error) {
	m := &o.match
	parent, err := o.container()
	if err != nil {
		return nil, err
	}
	var head, tail []byte
	if m.numSiblings != 0 {
		head = append(head, ',')
	}
	if o.path[m.matchLevel].numeric {
		return nil, ErrSdPathNotFound
	}
	head = append(append(append(head, '"'), o.path[m.matchLevel].key...), `":`...)
	for _, comp := range o.path[m.matchLevel+1:] {
		if comp.numeric {
			return nil, ErrSdPathNotFound
		}
		head = append(append(append(head, `{"`...), comp.key...), `":`...)
		tail = append(tail, '}')
	}
	if inArray {
		head = append(head, '[')
		tail = append([]byte{']'}, tail...)
	}
	return join(o.doc[:parent.end()-1], head, o.value, tail, o.doc[parent.end()-1:]), nil
}

// remove is op_exec for REMOVE and Operation::do_remove.
func (o *subdocOperation) remove() ([]byte, error) {
	if len(o.path) == 1 {
		return nil, ErrSdCantInsert
	}
	if err := o.matchCommon(true); err != nil {
		return nil, err
	}
	m := &o.match
	if m.matchres != jsonMatchComplete {
		return nil, ErrSdPathNotFound
	}
	from := m.locDeepest.at
	if m.locKey.length != 0 {
		from = m.locKey.at
	}
	head := o.doc[:from]
	tail := o.doc[m.locDeepest.end():]
	if m.numSiblings != 0 {
		if m.isLast() {
			if i := bytes.LastIndexByte(head, ','); i >= 0 {
				head = head[:i]
			}
		} else if i := bytes.IndexByte(tail, ','); i >= 0 {
			tail = tail[i+1:]
		}
	}
	return join(head, tail), nil
}

// insertSingleton is Operation::insert_singleton_element.
func (o *subdocOperation) insertSingleton() ([]byte, error) {
	array, err := o.container()
	if err != nil {
		return nil, err
	}
	return join(o.doc[:array.at+1], o.value, o.doc[array.end()-1:]), nil
}

// prepend is op_exec for ARRAY_PREPEND and Operation::do_list_prepend.
func (o *subdocOperation) prepend() ([]byte, error) {
	if err := o.validate(validateParentArray, o.maxDepthParent(), false, false); err != nil {
		return nil, err
	}
	if len(o.path) == subdocMaxPathComponents {
		return nil, ErrSdPathTooBig
	}
	path := o.path
	o.path = append(append([]jprComp{}, path...), jprComp{numeric: true})
	err := o.matchCommon(false)
	o.path = path
	if err != nil {
		return nil, err
	}
	m := &o.match
	if m.matchres != jsonMatchComplete {
		switch {
		case m.immediateParentFound:
			return o.insertSingleton()
		case o.mkdirP:
			return o.mkdirPath(true)
		}
		return nil, ErrSdPathNotFound
	}
	first := m.locDeepest
	return join(o.doc[:first.at], o.value, []byte(","), o.doc[first.at:]), nil
}

// emptyAppend is Operation::do_empty_append, which appends to the root
// array without parsing the document.
func (o *subdocOperation) emptyAppend() ([]byte, error) {
	doc := o.doc
	end := len(doc) - 1
	for end > 0 && isCSpace(doc[end]) {
		end--
	}
	if end <= 0 || doc[end] != ']' {
		if end >= 0 && (doc[end] == ']' || doc[end] == '}') {
			return nil, ErrSdPathMismatch
		}
		return nil, ErrSdNotJSON
	}
	last := end - 1
	for last > 0 && isJSONWhitespace(doc[last]) {
		last--
	}
	switch {
	case doc[last] == '[' || doc[last] == ',':
		return join(doc[:last+1], o.value, doc[end:end+1]), nil
	case !isCSpace(doc[last]):
		return join(doc[:last+1], []byte(","), o.value, doc[end:end+1]), nil
	}
	return nil, ErrSdNotJSON
}

// appendValue is op_exec for ARRAY_APPEND and ARRAY_ADD_UNIQUE and
// Operation::do_list_append.
func (o *subdocOperation) appendValue(unique bool) ([]byte, error) {
	if err := o.validate(validateParentArray, o.maxDepthParent(), unique, unique); err != nil {
		return nil, err
	}
	if len(o.path) == 1 && !unique {
		return o.emptyAppend()
	}
	if unique {
		o.match.ensureUnique = o.value
	}
	if err := o.matchCommon(false); err != nil {
		return nil, err
	}
	m := &o.match
	if m.matchres != jsonMatchComplete {
		if o.mkdirP {
			return o.mkdirPath(true)
		}
		return nil, ErrSdPathNotFound
	}
	if m.typ != jsonTList {
		return nil, ErrSdPathMismatch
	}
	if unique && m.uniqueItemFound {
		return nil, ErrSdPathExists
	}
	if m.numChildren == 0 {
		return o.insertSingleton()
	}
	array, err := o.container()
	if err != nil {
		return nil, err
	}
	return join(o.doc[:array.end()-1], []byte(","), o.value, o.doc[array.end()-1:]), nil
}

// insert is op_exec for ARRAY_INSERT and Operation::do_insert.
func (o *subdocOperation) insert() ([]byte, error) {
	if err := o.validate(validateParentArray, o.maxDepthNewKey(), false, false); err != nil {
		return nil, err
	}
	last := o.path[len(o.path)-1]
	if !last.numeric || last.neg {
		return nil, ErrSdPathInvalid
	}
	if err := o.matchCommon(false); err != nil {
		return nil, err
	}
	m := &o.match
	switch {
	case m.matchres == jsonMatchComplete:
		element := m.locDeepest
		return join(o.doc[:element.at], o.value, []byte(","), o.doc[element.at:]), nil
	case !m.immediateParentFound:
		return nil, ErrSdPathNotFound
	case m.numSiblings == 0 && last.idx == 0:
		return o.insertSingleton()
	case last.idx == m.numSiblings:
		array, err := o.container()
		if err != nil {
			return nil, err
		}
		return join(o.doc[:array.end()-1], []byte(","), o.value, o.doc[array.end()-1:]), nil
	}
	return nil, ErrSdPathNotFound
}

// parseCounterDelta is subjson's parse_int64: a non-zero decimal integer,
// with an optional minus sign and no leading zero, whose magnitude is at
// most the largest int64.
func parseCounterDelta(value []byte) (int64, error) {
	digits := bytes.TrimPrefix(value, []byte("-"))
	if len(digits) == 0 || digits[0] == '0' {
		return 0, ErrSdDeltaInvalid
	}
	for _, c := range digits {
		if !isJSONDigit(c) {
			return 0, ErrSdDeltaInvalid
		}
	}
	magnitude, err := strconv.ParseInt(string(digits), 10, 64)
	if err != nil {
		return 0, ErrSdDeltaInvalid
	}
	if len(digits) != len(value) {
		return -magnitude, nil
	}
	return magnitude, nil
}

// counter is op_exec for COUNTER and Operation::do_arith_op. It returns the
// document and the counter's new value.
func (o *subdocOperation) counter() ([]byte, []byte, error) {
	delta, err := parseCounterDelta(o.value)
	if err != nil {
		return nil, nil, err
	}
	if err := o.matchCommon(o.mkdirP); err != nil {
		return nil, nil, err
	}
	m := &o.match
	if m.matchres != jsonMatchComplete {
		if !o.mkdirP && !m.immediateParentFound {
			return nil, nil, ErrSdPathNotFound
		}
		if m.typ != jsonTObject {
			return nil, nil, ErrSdPathNotFound
		}
		o.value = []byte(strconv.FormatInt(delta, 10))
		o.mkdirP = true
		doc, err := o.doStoreDict(dictAdd)
		if err != nil {
			return nil, nil, err
		}
		return doc, o.value, nil
	}

	if m.typ != jsonTSpecial || m.sflags&^jsonSpecialNumeric != 0 {
		return nil, nil, ErrSdPathMismatch
	}
	current, err := parseCLongLong(o.doc[m.locDeepest.at:])
	if err != nil {
		return nil, nil, err
	}
	if (delta >= 0 && current >= 0 && math.MaxInt64-delta < current) ||
		(delta < 0 && current < 0 && delta < math.MinInt64-current) {
		return nil, nil, ErrSdCantInsert
	}
	result := []byte(strconv.FormatInt(current+delta, 10))
	loc := m.locDeepest
	return join(o.doc[:loc.at], result, o.doc[loc.end():]), result, nil
}

// parseCLongLong reads the integer at the start of text as strtoll does,
// returning ErrSdNumRange where strtoll sets ERANGE.
func parseCLongLong(text []byte) (int64, error) {
	end := 0
	if end < len(text) && (text[end] == '-' || text[end] == '+') {
		end++
	}
	for end < len(text) && isJSONDigit(text[end]) {
		end++
	}
	value, err := strconv.ParseInt(string(text[:end]), 10, 64)
	if numErr, ok := err.(*strconv.NumError); ok && numErr.Err == strconv.ErrRange {
		return 0, ErrSdNumRange
	}
	return value, nil
}

// marshalJSON encodes v as kv_engine encodes JSON it builds: without
// escaping '<', '>' and '&'.
func marshalJSON(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}
