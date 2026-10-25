// Ported from src/JSON_checker.cc in couchbase/platform@v7.0.5, which is
// distributed under the JSON.org license below:
//
// Copyright (c) 2005 JSON.org
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// The Software shall be used for Good, not Evil.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package kvproc

import "github.com/couchbase/gocbcore/v10/memd"

// SetDatatypeJSONFromValue returns datatype with its JSON bit set if
// kv_engine's JSON checker accepts value and cleared otherwise, as kv_engine
// sets it from the content of every value it stores instead of trusting the
// client.
func SetDatatypeJSONFromValue(datatype uint8, value []byte) uint8 {
	if CheckUTF8JSON(value) {
		return datatype | uint8(memd.DatatypeFlagJSON)
	}
	return datatype &^ uint8(memd.DatatypeFlagJSON)
}

// JSON_checker character classes.
const (
	jcSpace = iota
	jcWhite
	jcLCurb
	jcRCurb
	jcLSqrb
	jcRSqrb
	jcColon
	jcComma
	jcQuote
	jcBacks
	jcSlash
	jcPlus
	jcMinus
	jcPoint
	jcZero
	jcDigit
	jcLowA
	jcLowB
	jcLowC
	jcLowD
	jcLowE
	jcLowF
	jcLowL
	jcLowN
	jcLowR
	jcLowS
	jcLowT
	jcLowU
	jcABCDF
	jcE
	jcEtc
)

// JSON_checker states.
const (
	jsGO = iota
	jsOK
	jsOB
	jsKE
	jsCO
	jsVA
	jsAR
	jsST
	jsES
	jsU1
	jsU2
	jsU3
	jsU4
	jsMI
	jsZE
	jsIN
	jsFR
	jsFS
	jsE1
	jsE2
	jsE3
	jsT1
	jsT2
	jsT3
	jsF1
	jsF2
	jsF3
	jsF4
	jsN1
	jsN2
	jsN3
)

const jcErr = -1

var jcASCIIClass = [128]int{
	jcErr, jcErr, jcErr, jcErr, jcErr, jcErr, jcErr, jcErr,
	jcErr, jcWhite, jcWhite, jcErr, jcErr, jcWhite, jcErr, jcErr,
	jcErr, jcErr, jcErr, jcErr, jcErr, jcErr, jcErr, jcErr,
	jcErr, jcErr, jcErr, jcErr, jcErr, jcErr, jcErr, jcErr,

	jcSpace, jcEtc, jcQuote, jcEtc, jcEtc, jcEtc, jcEtc, jcEtc,
	jcEtc, jcEtc, jcEtc, jcPlus, jcComma, jcMinus, jcPoint, jcSlash,
	jcZero, jcDigit, jcDigit, jcDigit, jcDigit, jcDigit, jcDigit, jcDigit,
	jcDigit, jcDigit, jcColon, jcEtc, jcEtc, jcEtc, jcEtc, jcEtc,

	jcEtc, jcABCDF, jcABCDF, jcABCDF, jcABCDF, jcE, jcABCDF, jcEtc,
	jcEtc, jcEtc, jcEtc, jcEtc, jcEtc, jcEtc, jcEtc, jcEtc,
	jcEtc, jcEtc, jcEtc, jcEtc, jcEtc, jcEtc, jcEtc, jcEtc,
	jcEtc, jcEtc, jcEtc, jcLSqrb, jcBacks, jcRSqrb, jcEtc, jcEtc,

	jcEtc, jcLowA, jcLowB, jcLowC, jcLowD, jcLowE, jcLowF, jcEtc,
	jcEtc, jcEtc, jcEtc, jcEtc, jcLowL, jcEtc, jcLowN, jcEtc,
	jcEtc, jcEtc, jcLowR, jcLowS, jcLowT, jcLowU, jcEtc, jcEtc,
	jcEtc, jcEtc, jcEtc, jcLCurb, jcEtc, jcRCurb, jcEtc, jcEtc,
}

const __ = jcErr

// jcTransitions is JSON_checker's state transition table; a negative entry
// other than __ is an action.
var jcTransitions = [31][31]int{
	/*start  GO*/ {jsGO, jsGO, -6, __, -5, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __},
	/*ok     OK*/ {jsOK, jsOK, __, -8, __, -7, __, -3, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __},
	/*object OB*/ {jsOB, jsOB, __, -9, __, __, __, __, jsST, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __},
	/*key    KE*/ {jsKE, jsKE, __, __, __, __, __, __, jsST, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __},
	/*colon  CO*/ {jsCO, jsCO, __, __, __, __, -2, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __},
	/*value  VA*/ {jsVA, jsVA, -6, __, -5, __, __, __, jsST, __, __, __, jsMI, __, jsZE, jsIN, __, __, __, __, __, jsF1, __, jsN1, __, __, jsT1, __, __, __, __},
	/*array  AR*/ {jsAR, jsAR, -6, __, -5, -7, __, __, jsST, __, __, __, jsMI, __, jsZE, jsIN, __, __, __, __, __, jsF1, __, jsN1, __, __, jsT1, __, __, __, __},
	/*string ST*/ {jsST, __, jsST, jsST, jsST, jsST, jsST, jsST, -4, jsES, jsST, jsST, jsST, jsST, jsST, jsST, jsST, jsST, jsST, jsST, jsST, jsST, jsST, jsST, jsST, jsST, jsST, jsST, jsST, jsST, jsST},
	/*escape ES*/ {__, __, __, __, __, __, __, __, jsST, jsST, jsST, __, __, __, __, __, __, jsST, __, __, __, jsST, __, jsST, jsST, __, jsST, jsU1, __, __, __},
	/*u1     U1*/ {__, __, __, __, __, __, __, __, __, __, __, __, __, __, jsU2, jsU2, jsU2, jsU2, jsU2, jsU2, jsU2, jsU2, __, __, __, __, __, __, jsU2, jsU2, __},
	/*u2     U2*/ {__, __, __, __, __, __, __, __, __, __, __, __, __, __, jsU3, jsU3, jsU3, jsU3, jsU3, jsU3, jsU3, jsU3, __, __, __, __, __, __, jsU3, jsU3, __},
	/*u3     U3*/ {__, __, __, __, __, __, __, __, __, __, __, __, __, __, jsU4, jsU4, jsU4, jsU4, jsU4, jsU4, jsU4, jsU4, __, __, __, __, __, __, jsU4, jsU4, __},
	/*u4     U4*/ {__, __, __, __, __, __, __, __, __, __, __, __, __, __, jsST, jsST, jsST, jsST, jsST, jsST, jsST, jsST, __, __, __, __, __, __, jsST, jsST, __},
	/*minus  MI*/ {__, __, __, __, __, __, __, __, __, __, __, __, __, __, jsZE, jsIN, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __},
	/*zero   ZE*/ {jsOK, jsOK, __, -8, __, -7, __, -3, __, __, __, __, __, jsFR, __, __, __, __, __, __, jsE1, __, __, __, __, __, __, __, __, jsE1, __},
	/*int    IN*/ {jsOK, jsOK, __, -8, __, -7, __, -3, __, __, __, __, __, jsFR, jsIN, jsIN, __, __, __, __, jsE1, __, __, __, __, __, __, __, __, jsE1, __},
	/*frac   FR*/ {__, __, __, __, __, __, __, __, __, __, __, __, __, __, jsFS, jsFS, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __},
	/*fracs  FS*/ {jsOK, jsOK, __, -8, __, -7, __, -3, __, __, __, __, __, __, jsFS, jsFS, __, __, __, __, jsE1, __, __, __, __, __, __, __, __, jsE1, __},
	/*e      E1*/ {__, __, __, __, __, __, __, __, __, __, __, jsE2, jsE2, __, jsE3, jsE3, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __},
	/*ex     E2*/ {__, __, __, __, __, __, __, __, __, __, __, __, __, __, jsE3, jsE3, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __},
	/*exp    E3*/ {jsOK, jsOK, __, -8, __, -7, __, -3, __, __, __, __, __, __, jsE3, jsE3, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __},
	/*tr     T1*/ {__, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, jsT2, __, __, __, __, __, __},
	/*tru    T2*/ {__, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, jsT3, __, __, __},
	/*true   T3*/ {__, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, jsOK, __, __, __, __, __, __, __, __, __, __},
	/*fa     F1*/ {__, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, jsF2, __, __, __, __, __, __, __, __, __, __, __, __, __, __},
	/*fal    F2*/ {__, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, jsF3, __, __, __, __, __, __, __, __},
	/*fals   F3*/ {__, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, jsF4, __, __, __, __, __},
	/*false  F4*/ {__, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, jsOK, __, __, __, __, __, __, __, __, __, __},
	/*nu     N1*/ {__, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, jsN2, __, __, __},
	/*nul    N2*/ {__, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, jsN3, __, __, __, __, __, __, __, __},
	/*null   N3*/ {__, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, jsOK, __, __, __, __, __, __, __, __},
}

// JSON_checker modes.
const (
	jmArray = iota
	jmDone
	jmKey
	jmObject
)

type jsonChecker struct {
	state int
	stack []byte
}

func (jc *jsonChecker) push(mode byte) {
	jc.stack = append(jc.stack, mode)
}

func (jc *jsonChecker) pop(mode byte) bool {
	if len(jc.stack) == 0 {
		return false
	}
	top := jc.stack[len(jc.stack)-1]
	jc.stack = jc.stack[:len(jc.stack)-1]
	return top == mode
}

// char is JSON_checker_char.
func (jc *jsonChecker) char(c byte) bool {
	class := jcEtc
	if c < 128 {
		class = jcASCIIClass[c]
		if class == jcErr {
			return false
		}
	}
	next := jcTransitions[jc.state][class]
	if next >= 0 {
		jc.state = next
		return true
	}
	switch next {
	case -9:
		if !jc.pop(jmKey) {
			return false
		}
		jc.state = jsOK
	case -8:
		if !jc.pop(jmObject) {
			return false
		}
		jc.state = jsOK
	case -7:
		if !jc.pop(jmArray) {
			return false
		}
		jc.state = jsOK
	case -6:
		jc.push(jmKey)
		jc.state = jsOB
	case -5:
		jc.push(jmArray)
		jc.state = jsAR
	case -4:
		switch jc.stack[len(jc.stack)-1] {
		case jmKey:
			jc.state = jsCO
		case jmArray, jmObject, jmDone:
			jc.state = jsOK
		default:
			return false
		}
	case -3:
		switch jc.stack[len(jc.stack)-1] {
		case jmObject:
			if !jc.pop(jmObject) {
				return false
			}
			jc.push(jmKey)
			jc.state = jsKE
		case jmArray:
			jc.state = jsVA
		default:
			return false
		}
	case -2:
		if !jc.pop(jmKey) {
			return false
		}
		jc.push(jmObject)
		jc.state = jsVA
	default:
		return false
	}
	return true
}

// CheckUTF8JSON reports whether kv_engine's checkUTF8JSON accepts data: a
// JSON value, at any depth, whose bytes above 0x7f follow UTF-8's lead and
// continuation byte counts. It does not reject overlong forms, surrogates or
// code points above U+10FFFF.
func CheckUTF8JSON(data []byte) bool {
	jc := jsonChecker{state: jsVA}
	jc.push(jmDone)
	expect := 0
	for _, c := range data {
		if !jc.char(c) {
			return false
		}
		if c <= 0x7f {
			if expect != 0 {
				return false
			}
			continue
		}
		if c&0xc0 == 0xc0 {
			if expect != 0 {
				return false
			}
			expect++
			if c&0x20 != 0 {
				expect++
			}
			if c&0x10 != 0 && expect == 2 {
				expect++
			}
			if expect == 3 && c&0x8 != 0 {
				return false
			}
			continue
		}
		if expect == 0 {
			return false
		}
		expect--
	}
	if jc.state != jsOK && !jc.char(' ') {
		return false
	}
	return jc.state == jsOK && jc.pop(jmDone)
}
