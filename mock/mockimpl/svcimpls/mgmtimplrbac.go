package svcimpls

import (
	"bytes"
	"net/http"
	"sort"
	"strings"

	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
)

// mgmtImplRBAC serves the user and group endpoints of
// menelaus_web_rbac that mgmtImpl does not.
type mgmtImplRBAC struct {
	mgmt mgmtImpl
}

func (x *mgmtImplRBAC) Register(h *hookHelper) {
	h.RegisterMgmtHandler("GET", "/settings/rbac/users", x.mgmt.handleGetAllUsers)
	h.RegisterMgmtHandler("PUT", "/settings/rbac/users/*", x.mgmt.handleUpsertUser)
	h.RegisterMgmtHandler("DELETE", "/settings/rbac/users/*", x.mgmt.handleDropUser)
	h.RegisterMgmtHandler("GET", "/settings/rbac/groups", x.handleGetAllGroups)
	h.RegisterMgmtHandler("GET", "/settings/rbac/groups/*", x.handleGetGroup)
	h.RegisterMgmtHandler("PUT", "/settings/rbac/groups/*", x.handleUpsertGroup)
	h.RegisterMgmtHandler("DELETE", "/settings/rbac/groups/*", x.handleDropGroup)
	h.RegisterMgmtHandler("GET", "/whoami", x.mgmt.handleWhoami)
	h.RegisterMgmtHandler("PUT", "/settings/rbac/users", methodNotAllowed)
	h.RegisterMgmtHandler("DELETE", "/settings/rbac/users", methodNotAllowed)
	h.RegisterMgmtHandler("PUT", "/settings/rbac/groups", methodNotAllowed)
	h.RegisterMgmtHandler("DELETE", "/settings/rbac/groups", methodNotAllowed)
}

// methodNotAllowed is the answer of menelaus_web:get_action/4 to a PUT or
// DELETE of a path it has no route for.
func methodNotAllowed(mock.MgmtService, *mock.HTTPRequest) *mock.HTTPResponse {
	return &mock.HTTPResponse{
		StatusCode: 405,
		Header:     http.Header{"Content-Type": {"text/plain"}},
		Body:       bytes.NewReader([]byte("Method Not Allowed")),
	}
}

// ldapDNError is eldap:parse_dn/1 of OTP's eldap 1.2.8, which ns_server
// 7.0.5 runs: it returns the reason it refuses dn for, or "". A dn starting
// with "," matches no clause of parse_dn and crashes it, which crashed is
// true for.
func ldapDNError(dn string) (reason string, crashed bool) {
	if dn == "" {
		return "", false
	}
	if dn[0] == ',' {
		return "", true
	}
	s := dn
	for s != "" {
		if s[0] == ',' {
			s = s[1:]
			continue
		}
		var err string
		if s, err = ldapNameComponent(s); err != "" {
			return err, false
		}
	}
	return "", false
}

func ldapNameComponent(s string) (string, string) {
	for {
		rest, err := ldapAttributeTypeAndValue(s)
		if err != "" || rest == "" || rest[0] != '+' {
			return rest, err
		}
		s = rest[1:]
	}
}

func isDigit(c byte) bool { return '0' <= c && c <= '9' }

func isLetter(c byte) bool { return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' }

// ldapHexPair is eldap's guard for a hex pair, whose ";" and "," bind as
// Erlang's do.
func ldapHexPair(x, y byte) bool {
	return isDigit(x) || 'a' <= x && x <= 'f' || 'A' <= x && x <= 'F' && isDigit(y) ||
		'a' <= y && y <= 'f' || 'A' <= y && y <= 'F'
}

func isLDAPSpecial(c byte) bool { return strings.IndexByte(",=+<>#;", c) >= 0 }

func ldapAttributeTypeAndValue(s string) (string, string) {
	i := 0
	switch {
	case s != "" && isLetter(s[0]):
		i = 1
		for i < len(s) && (isLetter(s[i]) || isDigit(s[i]) || s[i] == '-') {
			i++
		}
	case s != "" && isDigit(s[0]):
		for i < len(s) && isDigit(s[i]) {
			if i+1 < len(s) && s[i+1] == '.' {
				i += 2
			} else {
				i++
			}
		}
	default:
		return s, "invalid_attribute_type"
	}
	if i == len(s) || s[i] != '=' {
		return s, "expecting_equal_sign"
	}
	return ldapAttributeValue(s[i+1:])
}

// ldapEscape returns the length of the escape sequence at the start of s,
// a backslash and what may follow it, or 0.
func ldapEscape(s string) int {
	switch {
	case len(s) < 2 || s[0] != '\\':
		return 0
	case isLDAPSpecial(s[1]) || s[1] == '\\' || s[1] == '"':
		return 2
	case len(s) >= 3 && ldapHexPair(s[1], s[2]):
		return 3
	}
	return 0
}

func ldapAttributeValue(s string) (string, string) {
	switch {
	case len(s) >= 3 && s[0] == '#' && ldapHexPair(s[1], s[2]):
		s = s[3:]
		for len(s) >= 2 && ldapHexPair(s[0], s[1]) {
			s = s[2:]
		}
		return s, ""
	case s != "" && s[0] == '"':
		s = s[1:]
		for {
			switch {
			case s != "" && s[0] == '"':
				return s[1:], ""
			case s != "" && s[0] != '\\':
				s = s[1:]
			case ldapEscape(s) > 0:
				s = s[ldapEscape(s):]
			default:
				return s, "expecting_double_quote_mark"
			}
		}
	}
	for s != "" {
		switch {
		case !isLDAPSpecial(s[0]) && s[0] != '\\' && s[0] != '"':
			s = s[1:]
		case ldapEscape(s) > 0:
			s = s[ldapEscape(s):]
		default:
			return s, ""
		}
	}
	return s, ""
}

func hasLDAPGroupRef(info mockauth.GroupInfo) bool {
	return info.LDAPGroupRef != nil && *info.LDAPGroupRef != ""
}

// handleUpsertGroup is menelaus_web_rbac:handle_put_group/2.
func (x *mgmtImplRBAC) handleUpsertGroup(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	users := source.Node().Cluster().Users()
	requester := requestUser(req)
	name := rbacPathTokens(req)[0]
	if msg := validateRBACID(name, "Group name"); msg != "" {
		return errorsResponse(400, []formParam{{"_", msg}})
	}

	// put_group_validators. eldap:parse_dn/1 crashes on a DN starting with
	// ",", which ns_server answers as an unexpected error.
	params := parsePost(req)
	ldapRef, hasLDAPRef := paramValue(params, "ldap_group_ref")
	if _, crashed := ldapDNError(ldapRef); hasLDAPRef && crashed {
		return rbacJSONResponse(500, []byte(`["Unexpected server error, request logged."]`))
	}
	v := newFormValidator(params)
	v.touched["description"] = true
	v.required("roles")
	var roles []string
	v.validate("roles", func(raw string) string {
		roles = parseRBACList(raw)
		if err := users.ValidateRoles(roles); err != nil {
			return err.Error()
		}
		return ""
	})
	v.validate("ldap_group_ref", func(ref string) string {
		if reason, _ := ldapDNError(ref); reason != "" {
			return "Should be valid LDAP distinguished name: " + reason
		}
		return ""
	})
	v.unsupported()
	if resp := v.response(req); resp != nil {
		return resp
	}

	res := mock.ClusterResources{Cluster: source.Node().Cluster()}
	affected := parsedRoles(roles)
	hadLDAPRef := false
	if group := users.GetGroup(name); group != nil {
		info := group.Info()
		affected = append(affected, info.Roles...)
		hadLDAPRef = hasLDAPGroupRef(info)
	}
	if hasSecurityRole(affected) && !requester.IsAllowed(res, securityWrite) {
		return forbiddenResponse(securityWrite)
	}
	if (hadLDAPRef || ldapRef != "") && !requester.IsAllowed(res, externalWrite) {
		return forbiddenResponse(externalWrite)
	}

	opts := mockauth.UpsertGroupOptions{Name: name, Roles: roles}
	if hasLDAPRef {
		opts.LDAPGroupRef = &ldapRef
	}
	if description, ok := paramValue(params, "description"); ok {
		opts.Description = &description
	}
	if err := users.UpsertGroup(opts); err != nil {
		return rbacJSONString(500, err.Error())
	}
	return rbacJSONString(200, "")
}

// handleGetGroup is menelaus_web_rbac:handle_get_group/2.
func (x *mgmtImplRBAC) handleGetGroup(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	users := source.Node().Cluster().Users()
	requester := requestUser(req)
	group := users.GetGroup(rbacPathTokens(req)[0])
	if group == nil {
		return rbacJSONString(404, "Unknown group.")
	}
	info := group.Info()
	if hasSecurityRole(info.Roles) && !requester.IsAllowed(mock.ClusterResources{Cluster: source.Node().Cluster()}, securityRead) {
		return forbiddenResponse(securityRead)
	}
	return rbacJSONResponse(200, marshalJSON(newJSONGroup(info)))
}

// handleGetAllGroups is menelaus_web_rbac:handle_get_all_groups/1: the
// groups, without those with security roles unless the requester may read
// them, nor those with an LDAP group unless it may read external users.
func (x *mgmtImplRBAC) handleGetAllGroups(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	users := source.Node().Cluster().Users()
	requester := requestUser(req)
	res := mock.ClusterResources{Cluster: source.Node().Cluster()}
	securityReadable := requester.IsAllowed(res, securityRead)
	externalReadable := requester.IsAllowed(res, externalRead)
	groups := []jsonGroup{}
	for _, group := range users.GetAllGroups() {
		info := group.Info()
		if !securityReadable && hasSecurityRole(info.Roles) || !externalReadable && hasLDAPGroupRef(info) {
			continue
		}
		groups = append(groups, newJSONGroup(info))
	}
	return rbacJSONResponse(200, marshalJSON(groups))
}

// handleDropGroup is menelaus_web_rbac:handle_delete_group/2.
func (x *mgmtImplRBAC) handleDropGroup(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	users := source.Node().Cluster().Users()
	requester := requestUser(req)
	name := rbacPathTokens(req)[0]
	res := mock.ClusterResources{Cluster: source.Node().Cluster()}
	if group := users.GetGroup(name); group != nil {
		info := group.Info()
		if hasSecurityRole(info.Roles) && !requester.IsAllowed(res, securityWrite) {
			return forbiddenResponse(securityWrite)
		}
		if hasLDAPGroupRef(info) && !requester.IsAllowed(res, externalWrite) {
			return forbiddenResponse(externalWrite)
		}
	}
	if err := users.DropGroup(name); err != nil {
		return rbacJSONString(404, err.Error())
	}
	return rbacJSONString(200, "")
}

// jsonGroup is menelaus_web_rbac:group_to_json/2.
type jsonGroup struct {
	ID           string     `json:"id"`
	Roles        []jsonRole `json:"roles"`
	LDAPGroupRef *string    `json:"ldap_group_ref,omitempty"`
	Description  *string    `json:"description,omitempty"`
}

func newJSONGroup(info mockauth.GroupInfo) jsonGroup {
	group := jsonGroup{ID: info.Name, Roles: []jsonRole{}, LDAPGroupRef: info.LDAPGroupRef, Description: info.Description}
	for _, role := range info.Roles {
		group.Roles = append(group.Roles, jsonRole{role: role})
	}
	return group
}

// readableBuckets returns, by name, the buckets whose settings the user of
// req may read, as menelaus_web_buckets:handle_bucket_list lists them.
func readableBuckets(cluster mock.Cluster, req *mock.HTTPRequest) []mock.Bucket {
	user := req.AuthenticatedUser(cluster.Users())
	if user == nil {
		return nil
	}
	var buckets []mock.Bucket
	for _, bucket := range cluster.GetAllBuckets() {
		permission := mockauth.RBACPermission{
			Object:    []mockauth.Vertex{mockauth.BucketVertex(mockauth.NameParam(bucket.Name())), {Name: "settings"}},
			Operation: "read",
		}
		if user.IsAllowed(mock.ClusterResources{Cluster: cluster}, permission) {
			buckets = append(buckets, bucket)
		}
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].Name() < buckets[j].Name() })
	return buckets
}
