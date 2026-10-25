package svcimpls

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"unicode/utf8"

	"github.com/couchbaselabs/gocaves/mock"
	"github.com/rivo/uniseg"
)

// formParam is a key and value, of a form or of an error.
type formParam struct {
	key, value string
}

func formParams(params []mock.QSParam) []formParam {
	out := make([]formParam, len(params))
	for i, param := range params {
		out[i] = formParam{param.Key, param.Value}
	}
	return out
}

// parseQS splits a query string as mochiweb_util:parse_qs does.
func parseQS(s string) []formParam {
	return formParams(mock.ParseQS(s))
}

// parsePost returns the parameters of the form body of req as
// mochiweb_request:parse_post does.
func parsePost(req *mock.HTTPRequest) []formParam {
	return formParams(mock.ParsePost(req.Header.Get("Content-Type"), req.PeekBody()))
}

func paramValue(params []formParam, key string) (string, bool) {
	for _, param := range params {
		if param.key == key {
			return param.value, true
		}
	}
	return "", false
}

// formValidator follows the state ns_server 7.0's validator module keeps
// for a form: a key gets at most one error, and a validator skips a key
// that is missing or already has an error.
type formValidator struct {
	params  []formParam
	touched map[string]bool
	// errors holds the newest error first, the order validator:handle
	// reports them in.
	errors []formParam
}

func newFormValidator(params []formParam) *formValidator {
	return &formValidator{params: params, touched: map[string]bool{}}
}

func (v *formValidator) value(key string) (string, bool) {
	if _, failed := paramValue(v.errors, key); failed {
		return "", false
	}
	return paramValue(v.params, key)
}

func (v *formValidator) fail(key, msg string) {
	v.errors = append([]formParam{{key, msg}}, v.errors...)
}

// validate runs check on the value of key; a non-empty result is its error.
func (v *formValidator) validate(key string, check func(string) string) {
	v.touched[key] = true
	if value, ok := v.value(key); ok {
		if msg := check(value); msg != "" {
			v.fail(key, msg)
		}
	}
}

func (v *formValidator) required(key string) {
	v.touched[key] = true
	if _, ok := paramValue(v.params, key); !ok {
		v.fail(key, "The value must be supplied")
	}
}

// unsupported reports each key no validator looked at, in the order sent,
// before the other errors.
func (v *formValidator) unsupported() {
	var unsupported []formParam
	for _, param := range v.params {
		if !v.touched[param.key] {
			unsupported = append(unsupported, formParam{param.key, "Unsupported key"})
		}
	}
	v.errors = append(unsupported, v.errors...)
}

// response answers as validator:handle does: errors with 400, and, for a
// request with just_validate=1 in its query, the errors found with 200.
// It returns nil when the request is to be carried out.
func (v *formValidator) response(req *mock.HTTPRequest) *mock.HTTPResponse {
	justValidate, _ := paramValue(parseQS(req.URL.RawQuery), "just_validate")
	if justValidate != "1" && len(v.errors) == 0 {
		return nil
	}
	statusCode := 400
	if justValidate == "1" {
		statusCode = 200
	}
	return errorsResponse(statusCode, v.errors)
}

// errorsResponse answers {"errors":{...}} with errs in order.
func errorsResponse(statusCode int, errs []formParam) *mock.HTTPResponse {
	var body bytes.Buffer
	body.WriteString(`{"errors":{`)
	for i, e := range errs {
		if i > 0 {
			body.WriteByte(',')
		}
		body.WriteString(erlJSONString(e.key))
		body.WriteByte(':')
		body.WriteString(erlJSONString(e.value))
	}
	body.WriteString(`}}`)
	return &mock.HTTPResponse{
		StatusCode: statusCode,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       bytes.NewReader(body.Bytes()),
	}
}

// validateScopeName runs scope_validators of ns_server 7.0's
// menelaus_web_collections on name, without validator:unsupported.
func (v *formValidator) validateScopeName() {
	v.required("name")
	v.validate("name", func(name string) string {
		switch utf8State(name) {
		case utf8Incomplete:
			return erlFormat("Incomplete utf8 name ~p", name)
		case utf8IllFormed:
			return erlFormat("Ill-formed utf8 name ~p", name)
		}
		// string:length counts grapheme clusters.
		if n := uniseg.GraphemeClusterCount(name); n < 1 || n > 251 {
			return fmt.Sprintf("Name length (%d) must be in the range from 1 to 251, inclusive", n)
		}
		return ""
	})
	v.validate("name", func(name string) string {
		for i := 0; i < len(name); i++ {
			c := name[i]
			if !(c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c == '_' || c == '%' || c == '-') {
				return erlFormat("Invalid value ~p : ~p", name,
					"Can only contain characters A-Z, a-z, 0-9 and the following symbols _ - %")
			}
		}
		return ""
	})
	v.validate("name", func(name string) string {
		if name[0] == '_' || name[0] == '%' {
			return "First character must not be _ or %"
		}
		return ""
	})
}

// validateMaxTTL runs validator:integer(maxTTL, 0, ?MC_MAXINT), which
// parses the value with list_to_integer, of any size, and returns it.
// ns_server accepts -1 (no expiry) from 7.6 only.
func (v *formValidator) validateMaxTTL() int {
	var maxTTL int
	v.validate("maxTTL", func(value string) string {
		n, ok := new(big.Int).SetString(value, 10)
		if !ok {
			return "The value must be an integer"
		}
		if n.Sign() < 0 || n.Cmp(big.NewInt(math.MaxInt32)) > 0 {
			return fmt.Sprintf("The value must be in range from 0 to %d", math.MaxInt32)
		}
		maxTTL = int(n.Int64())
		return ""
	})
	return maxTTL
}

const (
	utf8Valid = iota
	utf8Incomplete
	utf8IllFormed
)

// utf8State classifies s as unicode:characters_to_binary of OTP 22 does:
// incomplete when it ends with a lead byte and fewer continuation bytes
// than the lead byte announces, whatever their values, and ill-formed at
// any other invalid byte.
func utf8State(s string) int {
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size <= 1 {
			rest := s[i:]
			need := 0
			switch lead := rest[0]; {
			case lead >= 0xc0 && lead <= 0xdf:
				need = 2
			case lead >= 0xe0 && lead <= 0xef:
				need = 3
			case lead >= 0xf0 && lead <= 0xf7:
				need = 4
			}
			if len(rest) >= need {
				return utf8IllFormed
			}
			for j := 1; j < len(rest); j++ {
				if rest[j] < 0x80 || rest[j] > 0xbf {
					return utf8IllFormed
				}
			}
			return utf8Incomplete
		}
		i += size
	}
	return utf8Valid
}
