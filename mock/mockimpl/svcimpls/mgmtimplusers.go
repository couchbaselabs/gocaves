package svcimpls

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
)

// The permissions of menelaus_web_rbac on users and groups of a domain.
var (
	securityRead  = mockauth.RBACPermission{Object: mockauth.PlainVertices("admin", "security", "admin"), Operation: "read"}
	securityWrite = mockauth.RBACPermission{Object: mockauth.PlainVertices("admin", "security", "admin"), Operation: "write"}
	externalRead  = mockauth.RBACPermission{Object: mockauth.PlainVertices("admin", "security", "external"), Operation: "read"}
	externalWrite = mockauth.RBACPermission{Object: mockauth.PlainVertices("admin", "security", "external"), Operation: "write"}
)

// domainPermission is menelaus_web_rbac:get_domain_access_permission/2.
func domainPermission(operation, domain string) mockauth.RBACPermission {
	return mockauth.RBACPermission{Object: mockauth.PlainVertices("admin", "security", domain), Operation: operation}
}

// marshalJSON encodes v as the JSON encoders of ns_server and kv_engine do,
// which leave "<", ">" and "&" as they are.
func marshalJSON(v interface{}) []byte {
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(v); err != nil {
		return nil
	}
	return bytes.TrimSuffix(body.Bytes(), []byte("\n"))
}

func rbacJSONResponse(statusCode int, body []byte) *mock.HTTPResponse {
	return &mock.HTTPResponse{
		StatusCode: statusCode,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       bytes.NewReader(body),
	}
}

func rbacJSONString(statusCode int, msg string) *mock.HTTPResponse {
	return rbacJSONResponse(statusCode, marshalJSON(msg))
}

func forbiddenResponse(permission mockauth.RBACPermission) *mock.HTTPResponse {
	return rbacJSONResponse(403, marshalJSON(struct {
		Message     string   `json:"message"`
		Permissions []string `json:"permissions"`
	}{"Forbidden. User needs the following permissions", []string{permission.String()}}))
}

// validateRBACID is menelaus_web_rbac:validate_id/2.
func validateRBACID(id, field string) string {
	switch {
	case id == "":
		return field + " must not be empty"
	case len(id) > 128:
		return field + " may not exceed 128 characters"
	case id[0] == '@':
		return field + " cannot start with '@'"
	}
	valid := utf8.ValidString(id)
	for i := 0; i < len(id) && valid; i++ {
		c := id[i]
		valid = c > 32 && c != 127 && !strings.ContainsRune(`()<>,;:\"/[]?={}`, rune(c))
	}
	if !valid {
		return field + ` must not contain spaces, control or any of ()<>,;:\"/[]?={} characters and must be valid utf8`
	}
	return ""
}

// validatePassword is menelaus_web_rbac:is_valid_password/2 with the
// default password policy, at least 6 characters.
func validatePassword(password string) string {
	switch {
	case utf8.RuneCountInString(password) < 6:
		return "The password must be at least 6 characters long."
	case !utf8.ValidString(password):
		return "The password must be valid utf8"
	}
	for _, c := range password {
		if c < 32 || c == 127 {
			return "The password must not contain control characters"
		}
	}
	return ""
}

func hasSecurityRole(roles []*mockauth.UserRole) bool {
	for _, role := range roles {
		if role.IsSecurityRole() {
			return true
		}
	}
	return false
}

// parseRBACList is string:tokens(Value, ",") with each token trimmed.
func parseRBACList(value string) []string {
	var items []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	return items
}

// rbacPathTokens are the tokens of the request path after
// /settings/rbac/<users or groups>, those the route matched.
func rbacPathTokens(req *mock.HTTPRequest) []string {
	tokens := mgmtPathTokens(req)
	if len(tokens) < 3 {
		return nil
	}
	return tokens[3:]
}

// userPath returns the domain and user a user route names: a route without
// a domain is for an external user.
func userPath(req *mock.HTTPRequest) (string, string) {
	tokens := rbacPathTokens(req)
	if len(tokens) == 1 {
		return "external", tokens[0]
	}
	return tokens[0], tokens[1]
}

func knownUserDomain(domain string) bool {
	return domain == "local" || domain == "external"
}

// handleUpsertUser is menelaus_web_rbac:handle_put_user/3.
func (x *mgmtImpl) handleUpsertUser(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	users := source.Node().Cluster().Users()
	requester := requestUser(req)
	res := mock.ClusterResources{Cluster: source.Node().Cluster()}
	domain, username := userPath(req)
	if msg := validateRBACID(username, "Username"); msg != "" {
		return errorsResponse(400, []formParam{{"_", msg}})
	}
	if !knownUserDomain(domain) {
		return rbacJSONString(404, "Unknown user domain.")
	}

	// put_user_validators
	params := parsePost(req)
	v := newFormValidator(params)
	v.touched["name"] = true
	var groups, roles []string
	v.validate("groups", func(raw string) string {
		groups = parseRBACList(raw)
		if missing := users.MissingGroups(groups); len(missing) > 0 {
			return (&mockauth.MissingGroupsError{Groups: missing}).Error()
		}
		return ""
	})
	v.validate("roles", func(raw string) string {
		roles = parseRBACList(raw)
		if err := users.ValidateRoles(roles); err != nil {
			return err.Error()
		}
		return ""
	})
	if domain == "local" {
		v.validate("password", validatePassword)
	}
	v.unsupported()
	if resp := v.response(req); resp != nil {
		return resp
	}

	// handle_put_user_validated checks the security roles the user has,
	// gets and would get from its groups, then the domain, then LDAP groups.
	affected := append([]*mockauth.UserRole{}, parsedRoles(roles)...)
	ldapGroups := false
	for _, name := range groups {
		// A group dropped since the groups were validated has no roles.
		group := users.GetGroup(name)
		if group == nil {
			continue
		}
		info := group.Info()
		affected = append(affected, info.Roles...)
		ldapGroups = ldapGroups || info.LDAPGroupRef != nil && *info.LDAPGroupRef != ""
	}
	if old := users.GetUserInDomain(username, domain); old != nil {
		affected = append(affected, old.Info().AllRoles...)
	}
	if hasSecurityRole(affected) && !requester.IsAllowed(res, securityWrite) {
		return forbiddenResponse(securityWrite)
	}
	if permission := domainPermission("write", domain); !requester.IsAllowed(res, permission) {
		return forbiddenResponse(permission)
	}
	if ldapGroups && !requester.IsAllowed(res, externalWrite) {
		return forbiddenResponse(externalWrite)
	}

	opts := mockauth.UpsertUserOptions{
		Username: username,
		Domain:   domain,
		Roles:    roles,
		Groups:   groups,
	}
	opts.Password, _ = paramValue(params, "password")
	opts.DisplayName, opts.DisplayNameGiven = paramValue(params, "name")
	err := users.UpsertUser(opts)
	if errors.Is(err, mockauth.ErrPasswordRequired) {
		return errorsResponse(400, []formParam{{"password", err.Error()}})
	}
	if err != nil {
		return rbacJSONString(500, err.Error())
	}
	return rbacJSONString(200, "")
}

// parsedRoles returns the roles that parse, for checking which are security
// roles.
func parsedRoles(raws []string) []*mockauth.UserRole {
	var roles []*mockauth.UserRole
	for _, raw := range raws {
		if role, ok := mockauth.ParseRole(raw); ok {
			roles = append(roles, role)
		}
	}
	return roles
}

// handleGetUser is menelaus_web_rbac:handle_get_user/3.
func (x *mgmtImpl) handleGetUser(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	users := source.Node().Cluster().Users()
	requester := requestUser(req)
	domain, username := userPath(req)
	if !knownUserDomain(domain) {
		return rbacJSONString(404, "Unknown user domain.")
	}
	user := users.GetUserInDomain(username, domain)
	if user == nil {
		return rbacJSONString(404, "Unknown user.")
	}
	info := user.Info()
	res := mock.ClusterResources{Cluster: source.Node().Cluster()}
	if hasSecurityRole(info.AllRoles) && !requester.IsAllowed(res, securityRead) {
		return forbiddenResponse(securityRead)
	}
	return rbacJSONResponse(200, marshalJSON(newJSONUser(info)))
}

// handleGetAllUsers is menelaus_web_rbac:handle_get_all_users/3: the users,
// of a domain when the route names one, without those with security roles
// unless the requester may read them, nor those of a domain the requester
// handleGetAllUsers is menelaus_web_rbac:handle_get_users/2 and /3: the
// users, of a domain when the path names one, that have a role allowing the
// permission given, leaving out those the requester may not read. With
// pageSize it answers a page of them, as handle_get_users_page/4 does.
func (x *mgmtImpl) handleGetAllUsers(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	users := source.Node().Cluster().Users()
	requester := requestUser(req)
	domain := ""
	if tokens := rbacPathTokens(req); len(tokens) == 1 {
		if domain = tokens[0]; !knownUserDomain(domain) {
			return rbacJSONString(404, "Unknown user domain.")
		}
	}

	query := parseQS(req.URL.RawQuery)
	_, paged := paramValue(query, "pageSize")
	v := newFormValidator(query)
	var page usersPage
	if paged {
		page = validateUsersPage(v, domain)
	}
	var permission *mockauth.RBACPermission
	v.validate("permission", func(raw string) string {
		parsed, ok := mockauth.ParsePermission(raw)
		if !ok {
			return "Malformed permission"
		}
		permission = &parsed
		return ""
	})
	if resp := v.response(req); resp != nil {
		return resp
	}

	// filter_by_roles keeps the users with one of the roles, as
	// produce_roles_by_permission lists them, that allow the permission:
	// of the user's own and its groups' roles for a page, and of its own
	// only otherwise, as handle_get_all_users/3 selects no groups.
	var allowing map[string]bool
	if permission != nil {
		roles, err := users.RolesByPermission(permission)
		if err != nil {
			return serverErrorResponse()
		}
		allowing = make(map[string]bool)
		for _, role := range roles {
			allowing[role.String()] = true
		}
	}
	res := mock.ClusterResources{Cluster: source.Node().Cluster()}
	securityReadable := requester.IsAllowed(res, securityRead)
	var infos []mockauth.UserInfo
	for _, user := range users.GetAllUsers() {
		info := user.Info()
		switch {
		case domain != "" && info.Domain != domain:
		case allowing != nil && !hasAnyRole(filteredRoles(info, paged), allowing):
		case !securityReadable && hasSecurityRole(info.AllRoles):
		case !requester.IsAllowed(res, domainPermission("read", info.Domain)):
		case paged && !page.matches(info):
		default:
			infos = append(infos, info)
		}
	}
	if paged {
		return page.response(req, infos, users, permission, domain == "")
	}
	jsonUsers := []jsonUser{}
	for _, info := range infos {
		jsonUsers = append(jsonUsers, newJSONUser(info))
	}
	return rbacJSONResponse(200, marshalJSON(jsonUsers))
}

func filteredRoles(info mockauth.UserInfo, withGroups bool) []*mockauth.UserRole {
	if withGroups {
		return info.AllRoles
	}
	var own []*mockauth.UserRole
	for _, role := range info.Roles {
		for _, origin := range role.Origins {
			if origin == "" {
				own = append(own, role.Role)
			}
		}
	}
	return own
}

func hasAnyRole(roles []*mockauth.UserRole, names map[string]bool) bool {
	for _, role := range roles {
		if names[role.String()] {
			return true
		}
	}
	return false
}

// usersPage is a page of users as handle_get_users_page/4 takes it.
type usersPage struct {
	size            int
	sortBy, order   string
	substr          string
	hasSubstr       bool
	startFrom       string
	startFromDomain string
	hasStart        bool
}

// validateUsersPage runs get_users_page_validators/2 on v, for a path that
// names domain, or all domains when it is empty.
func validateUsersPage(v *formValidator, domain string) usersPage {
	page := usersPage{sortBy: "id", order: "asc", startFromDomain: domain}
	v.validate("pageSize", func(value string) string {
		n, ok := new(big.Int).SetString(value, 10)
		if !ok {
			return "The value must be an integer"
		}
		if n.Cmp(big.NewInt(2)) < 0 || n.Cmp(big.NewInt(100)) > 0 {
			return "The value must be in range from 2 to 100"
		}
		page.size = int(n.Int64())
		return ""
	})
	page.startFrom, page.hasStart = paramValue(v.params, "startFrom")
	v.touched["startFrom"] = true
	oneOf := func(target *string, values ...string) func(string) string {
		return func(value string) string {
			for _, allowed := range values {
				if value == allowed {
					*target = value
					return ""
				}
			}
			return "The value must be one of the following: [" + strings.Join(values, ",") + "]"
		}
	}
	v.validate("sortBy", oneOf(&page.sortBy, "id", "name", "domain", "password_change_timestamp"))
	v.validate("order", oneOf(&page.order, "asc", "desc"))
	page.substr, page.hasSubstr = paramValue(v.params, "substr")
	v.touched["substr"] = true
	if page.hasStart {
		if domain == "" {
			v.required("startFromDomain")
			v.validate("startFromDomain", oneOf(&page.startFromDomain, "local", "external"))
		} else if _, ok := paramValue(v.params, "startFromDomain"); ok {
			v.fail("startFromDomain", "The value must not be supplied")
		}
	}
	return page
}

// matches is substr_filter/2: the substring in the id or the name, both in
// lower case.
func (p usersPage) matches(info mockauth.UserInfo) bool {
	if !p.hasSubstr {
		return true
	}
	substr := erlLower(p.substr)
	return strings.Contains(erlLower(info.Username), substr) ||
		info.DisplayName != nil && strings.Contains(erlLower(*info.DisplayName), substr)
}

// erlLower is string:to_lower/1 of a byte list: Latin-1 upper case letters
// become lower case.
func erlLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' || c >= 0xc0 && c <= 0xde && c != 0xd7 {
			b[i] = c + 0x20
		}
	}
	return string(b)
}

// pageEntry is a user as create_skews/4 orders it: its identity, its name
// and the time its password changed, undefined when absent.
type pageEntry struct {
	id, domain string
	name       *string
	changed    *int64
	info       mockauth.UserInfo
}

func newPageEntry(id, domain string, info *mockauth.UserInfo) pageEntry {
	entry := pageEntry{id: id, domain: domain}
	if info != nil {
		entry.info = *info
		entry.name = info.DisplayName
		if !info.PasswordChanged.IsZero() {
			ms := info.PasswordChanged.UnixMilli()
			entry.changed = &ms
		}
	}
	return entry
}

// compare orders two users as Erlang orders the terms create_skews/4
// compares, in ascending order: the identity {Id, Domain}, {Domain, Id},
// or the property and then the identity, an undefined property, an atom,
// after a time, an integer, and before a name, a list.
func (p usersPage) compare(a, b pageEntry) int {
	identity := func() int {
		if c := strings.Compare(a.id, b.id); c != 0 {
			return c
		}
		return strings.Compare(a.domain, b.domain)
	}
	switch p.sortBy {
	case "domain":
		if c := strings.Compare(a.domain, b.domain); c != 0 {
			return c
		}
		return strings.Compare(a.id, b.id)
	case "name":
		switch {
		case a.name == nil && b.name != nil:
			return -1
		case a.name != nil && b.name == nil:
			return 1
		case a.name != nil:
			if c := strings.Compare(*a.name, *b.name); c != 0 {
				return c
			}
		}
	case "password_change_timestamp":
		switch {
		case a.changed == nil && b.changed != nil:
			return 1
		case a.changed != nil && b.changed == nil:
			return -1
		case a.changed != nil && *a.changed != *b.changed:
			if *a.changed < *b.changed {
				return -1
			}
			return 1
		}
	}
	return identity()
}

// response answers the page of infos, the users that pass the filters, as
// handle_get_users_page/4 does: the users from startFrom on, in order, the
// number of those before it and of all, and links to the first, previous,
// next and last pages.
func (p usersPage) response(req *mock.HTTPRequest, infos []mockauth.UserInfo, users mock.UserManager,
	permission *mockauth.RBACPermission, allDomains bool) *mock.HTTPResponse {
	less := func(a, b pageEntry) bool {
		if p.order == "desc" {
			return p.compare(a, b) > 0
		}
		return p.compare(a, b) < 0
	}
	entries := make([]pageEntry, len(infos))
	for i := range infos {
		entries[i] = newPageEntry(infos[i].Username, infos[i].Domain, &infos[i])
	}
	sort.SliceStable(entries, func(i, j int) bool { return less(entries[i], entries[j]) })

	from := 0
	if p.hasStart {
		var startInfo *mockauth.UserInfo
		if user := users.GetUserInDomain(p.startFrom, p.startFromDomain); user != nil {
			info := user.Info()
			startInfo = &info
		}
		start := newPageEntry(p.startFrom, p.startFromDomain, startInfo)
		for from < len(entries) && less(entries[from], start) {
			from++
		}
	}
	end := min(from+p.size, len(entries))

	type link struct {
		name  string
		entry *pageEntry
	}
	var links []link
	if p.hasStart && from > 0 {
		links = append(links, link{"first", nil}, link{"prev", &entries[max(from-p.size, 0)]})
	}
	if end < len(entries) {
		last := entries[max(len(entries)-p.size, 0)]
		next := entries[end]
		if less(last, next) {
			next = last
		}
		links = append(links, link{"next", &next}, link{"last", &last})
	}

	var params []string
	if permission != nil {
		params = append(params, "permission="+erlQuotePlus(permission.String()))
	}
	if p.hasSubstr {
		params = append(params, "substr="+erlQuotePlus(p.substr))
	}
	params = append(params, fmt.Sprintf("pageSize=%d", p.size), "sortBy="+p.sortBy, "order="+p.order)
	path := req.Target
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	var body bytes.Buffer
	fmt.Fprintf(&body, `{"total":%d,"links":{`, len(entries))
	for i, l := range links {
		linkParams := params
		if l.entry != nil {
			if allDomains {
				linkParams = append(linkParams[:len(linkParams):len(linkParams)], "startFromDomain="+erlQuotePlus(l.entry.domain))
			}
			linkParams = append(linkParams[:len(linkParams):len(linkParams)], "startFrom="+erlQuotePlus(l.entry.id))
		}
		if i > 0 {
			body.WriteByte(',')
		}
		fmt.Fprintf(&body, `%s:%s`, marshalJSON(l.name), marshalJSON(path+"?"+strings.Join(linkParams, "&")))
	}
	fmt.Fprintf(&body, `},"skipped":%d,"users":[`, from)
	for i, entry := range entries[from:end] {
		if i > 0 {
			body.WriteByte(',')
		}
		body.Write(marshalJSON(newJSONUser(entry.info)))
	}
	body.WriteString("]}")
	return rbacJSONResponse(200, body.Bytes())
}

// erlQuotePlus is mochiweb_util:quote_plus/1.
func erlQuotePlus(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.IndexByte(".-~_", c) >= 0:
			b.WriteByte(c)
		case c == ' ':
			b.WriteByte('+')
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// handleWhoami is menelaus_web_rbac:handle_whoami/1, which requires no
// permission: the identity of the request, anonymous without credentials,
// with its roles and the user's name and password change time.
func (x *mgmtImpl) handleWhoami(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	if _, ok := req.Header["Authorization"]; !ok {
		return rbacJSONResponse(200, []byte(`{"roles":[],"id":"","domain":"anonymous"}`))
	}
	user := req.AuthenticatedUser(source.Node().Cluster().Users())
	if user == nil {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Header:     http.Header{"WWW-Authenticate": {`Basic realm="Couchbase Server Admin / REST"`}},
			Body:       bytes.NewReader(nil),
		}
	}
	info := user.Info()
	// The roles of menelaus_users:get_roles/1, in Erlang's order.
	roles := []jsonRole{}
	for i := len(info.Roles) - 1; i >= 0; i-- {
		roles = append(roles, jsonRole{role: info.Roles[i].Role})
	}
	var body bytes.Buffer
	fmt.Fprintf(&body, `{"roles":%s,"id":%s,"domain":%s`, marshalJSON(roles), marshalJSON(info.Username), marshalJSON(info.Domain))
	if info.DisplayName != nil {
		fmt.Fprintf(&body, `,"name":%s`, marshalJSON(*info.DisplayName))
	}
	if !info.PasswordChanged.IsZero() {
		fmt.Fprintf(&body, `,"password_change_date":%s`, marshalJSON(passwordChangeDate(info.PasswordChanged)))
	}
	body.WriteByte('}')
	return rbacJSONResponse(200, body.Bytes())
}

// handleDropUser is menelaus_web_rbac:handle_delete_user/3.
func (x *mgmtImpl) handleDropUser(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	users := source.Node().Cluster().Users()
	requester := requestUser(req)
	domain, username := userPath(req)
	if !knownUserDomain(domain) {
		return rbacJSONString(404, "Unknown user domain.")
	}
	res := mock.ClusterResources{Cluster: source.Node().Cluster()}
	if user := users.GetUserInDomain(username, domain); user != nil {
		if hasSecurityRole(user.Info().AllRoles) && !requester.IsAllowed(res, securityWrite) {
			return forbiddenResponse(securityWrite)
		}
	}
	if permission := domainPermission("write", domain); !requester.IsAllowed(res, permission) {
		return forbiddenResponse(permission)
	}
	if err := users.DropUserInDomain(username, domain); err != nil {
		return rbacJSONString(404, err.Error())
	}
	return rbacJSONString(200, "")
}

// handleGetRoles is menelaus_web_rbac:handle_get_roles/1: the roles that
// allow the permission given, or all, without the security roles unless the
// requester may read them.
func (x *mgmtImpl) handleGetRoles(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	users := source.Node().Cluster().Users()
	requester := requestUser(req)
	var permission *mockauth.RBACPermission
	if raw, ok := paramValue(parseQS(req.URL.RawQuery), "permission"); ok {
		parsed, ok := mockauth.ParsePermission(raw)
		if !ok {
			return errorsResponse(400, []formParam{{"permission", "Malformed permission"}})
		}
		permission = &parsed
	}
	descriptions := make(map[string]*mockauth.ClusterRole)
	for _, clusterRole := range users.GetAllClusterRoles() {
		descriptions[clusterRole.Role] = clusterRole
	}
	securityReadable := requester.IsAllowed(mock.ClusterResources{Cluster: source.Node().Cluster()}, securityRead)
	var body bytes.Buffer
	body.WriteByte('[')
	roles, err := users.RolesByPermission(permission)
	if err != nil {
		return serverErrorResponse()
	}
	for _, role := range roles {
		if !securityReadable && role.IsSecurityRole() {
			continue
		}
		if body.Len() > 1 {
			body.WriteByte(',')
		}
		fmt.Fprintf(&body, `{"role":%s`, marshalJSON(role.Name))
		for _, param := range role.Params() {
			fmt.Fprintf(&body, `,%s:%s`, marshalJSON(param.Name), marshalJSON(param.Value))
		}
		description := descriptions[role.Name]
		fmt.Fprintf(&body, `,"name":%s,"desc":%s}`, marshalJSON(description.Name), marshalJSON(description.Description))
	}
	body.WriteByte(']')
	return rbacJSONResponse(200, body.Bytes())
}

type jsonUserOrigin struct {
	Type string `json:"type"`
	Name string `json:"name,omitempty"`
}

// jsonRole is a role as menelaus_web_rbac:role_to_json/2 gives it: its name,
// its parameters by name, and where the user has it from.
type jsonRole struct {
	role    *mockauth.UserRole
	origins []jsonUserOrigin
}

func (r jsonRole) MarshalJSON() ([]byte, error) {
	var body bytes.Buffer
	fmt.Fprintf(&body, `{"role":%s`, marshalJSON(r.role.Name))
	for _, param := range r.role.Params() {
		fmt.Fprintf(&body, `,%q:%s`, param.Name, marshalJSON(param.Value))
	}
	if r.origins != nil {
		fmt.Fprintf(&body, `,"origins":%s`, marshalJSON(r.origins))
	}
	body.WriteByte('}')
	return body.Bytes(), nil
}

// passwordChangeDate is menelaus_web_rbac:format_password_change_time/1:
// calendar:now_to_local_time/1 drops the milliseconds the time is kept in,
// and menelaus_util:format_server_time/1 writes zero ones.
func passwordChangeDate(changed time.Time) string {
	return changed.Truncate(time.Second).Format("2006-01-02T15:04:05.000Z")
}

// jsonUser is menelaus_web_rbac:user_to_json/2.
type jsonUser struct {
	ID              string     `json:"id"`
	Domain          string     `json:"domain"`
	Roles           []jsonRole `json:"roles"`
	Groups          []string   `json:"groups"`
	ExternalGroups  []string   `json:"external_groups"`
	Name            *string    `json:"name,omitempty"`
	PasswordChanged string     `json:"password_change_date,omitempty"`
}

func newJSONUser(info mockauth.UserInfo) jsonUser {
	user := jsonUser{
		ID:             info.Username,
		Domain:         info.Domain,
		Roles:          []jsonRole{},
		Groups:         append([]string{}, info.Groups...),
		ExternalGroups: []string{},
		Name:           info.DisplayName,
	}
	if !info.PasswordChanged.IsZero() {
		user.PasswordChanged = passwordChangeDate(info.PasswordChanged)
	}
	for _, role := range info.Roles {
		origins := []jsonUserOrigin{}
		for _, origin := range role.Origins {
			if origin == "" {
				origins = append(origins, jsonUserOrigin{Type: "user"})
			} else {
				origins = append(origins, jsonUserOrigin{Type: "group", Name: origin})
			}
		}
		user.Roles = append(user.Roles, jsonRole{role: role.Role, origins: origins})
	}
	return user
}
