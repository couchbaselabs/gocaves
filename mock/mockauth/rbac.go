package mockauth

import (
	"errors"
	"strings"
)

// Resources reports which buckets, scopes and collections exist. A bucket
// without collections, a memcached bucket, has no scopes.
type Resources interface {
	HasBucket(bucket string) bool
	HasScope(bucket, scope string) bool
	HasCollection(bucket, scope, collection string) bool
}

// roleDefinition is a role of ns_server 7.0's menelaus_roles:roles/0: its
// parameters and its permission patterns, in order. The first pattern that
// matches an object gives the operations allowed on it.
type roleDefinition struct {
	params   []string
	patterns []rolePattern
}

// rolePattern is an object pattern, as dot-separated vertices with their
// parameters in brackets, and the operations it allows: "all", "none" or a
// comma-separated list.
type rolePattern struct {
	object     string
	operations string
}

var roleDefinitions = map[string]roleDefinition{
	"admin": {params: []string{}, patterns: []rolePattern{
		{"", "all"},
	}},
	"ro_admin": {params: []string{}, patterns: []rolePattern{
		{"bucket[any].password", "none"},
		{"bucket[any].data", "none"},
		{"bucket[any].fts", "none"},
		{"bucket[any].analytics", "none"},
		{"admin.security", "read"},
		{"admin", "none"},
		{"eventing", "none"},
		{"analytics", "none"},
		{"backup", "none"},
		{"", "read,list"},
	}},
	"security_admin_local": {params: []string{}, patterns: []rolePattern{
		{"admin.security.admin", "none"},
		{"admin.security.external", "none"},
		{"admin.security", "all"},
		{"admin.logs", "none"},
		{"bucket[any].data", "none"},
		{"bucket[any].views", "none"},
		{"bucket[any].n1ql", "none"},
		{"bucket[any].fts", "none"},
		{"bucket[any].analytics", "none"},
		{"bucket[any].password", "none"},
		{"bucket[any]", "read"},
		{"analytics", "none"},
		{"backup", "none"},
		{"eventing", "none"},
		{"settings.fts", "none"},
		{"", "read,list"},
	}},
	"security_admin_external": {params: []string{}, patterns: []rolePattern{
		{"admin.security.admin", "none"},
		{"admin.security.local", "none"},
		{"admin.security", "all"},
		{"admin.logs", "none"},
		{"bucket[any].data", "none"},
		{"bucket[any].views", "none"},
		{"bucket[any].n1ql", "none"},
		{"bucket[any].fts", "none"},
		{"bucket[any].analytics", "none"},
		{"bucket[any].password", "none"},
		{"bucket[any]", "read"},
		{"analytics", "none"},
		{"backup", "none"},
		{"eventing", "none"},
		{"settings.fts", "none"},
		{"", "read,list"},
	}},
	"cluster_admin": {params: []string{}, patterns: []rolePattern{
		{"admin.internal", "none"},
		{"admin.security", "none"},
		{"admin.diag", "read"},
		{"bucket[any].data", "none"},
		{"bucket[any].views", "none"},
		{"bucket[any].n1ql", "none"},
		{"bucket[any].fts", "none"},
		{"bucket[any].analytics", "none"},
		{"bucket[any].password", "none"},
		{"n1ql.curl", "none"},
		{"eventing", "none"},
		{"analytics", "none"},
		{"backup", "none"},
		{"", "all"},
	}},
	"eventing_admin": {params: []string{}, patterns: []rolePattern{
		{"admin", "none"},
		{"xdcr", "none"},
		{"bucket[any].xdcr", "none"},
		{"bucket[any].password", "none"},
		{"bucket[any]", "all"},
		{"n1ql", "all"},
		{"eventing", "all"},
		{"analytics", "all"},
		{"buckets", "all"},
		{"", "read"},
	}},
	"backup_admin": {params: []string{}, patterns: []rolePattern{
		{"admin", "none"},
		{"bucket[any].password", "none"},
		{"", "all"},
	}},
	"bucket_admin": {params: []string{"bucket_name"}, patterns: []rolePattern{
		{"bucket[bucket_name].xdcr", "read,execute"},
		{"bucket[bucket_name].data", "none"},
		{"bucket[bucket_name].views", "none"},
		{"bucket[bucket_name].n1ql", "none"},
		{"bucket[bucket_name].password", "none"},
		{"bucket[bucket_name].fts", "none"},
		{"bucket[any].analytics", "none"},
		{"bucket[bucket_name]", "all"},
		{"bucket[any].settings", "read"},
		{"bucket[any]", "none"},
		{"xdcr", "none"},
		{"admin", "none"},
		{"eventing", "none"},
		{"analytics", "none"},
		{"backup", "none"},
		{"", "read"},
	}},
	"scope_admin": {params: []string{"bucket_name", "scope_name"}, patterns: []rolePattern{
		{"collection[bucket_name,scope_name,any].collections", "all"},
	}},
	"bucket_full_access": {params: []string{"bucket_name"}, patterns: []rolePattern{
		{"bucket[bucket_name].data", "all"},
		{"bucket[bucket_name].views", "all"},
		{"bucket[bucket_name].n1ql.index", "all"},
		{"bucket[bucket_name].n1ql", "execute"},
		{"bucket[bucket_name]", "read,flush"},
		{"bucket[bucket_name].settings", "read"},
		{"pools", "read"},
	}},
	"views_admin": {params: []string{"bucket_name"}, patterns: []rolePattern{
		{"bucket[bucket_name].views", "all"},
		{"bucket[bucket_name].data", "read"},
		{"bucket[bucket_name].stats", "read"},
		{"bucket[any].settings", "read"},
		{"bucket[any]", "none"},
		{"bucket[bucket_name].n1ql", "execute"},
		{"xdcr", "none"},
		{"admin", "none"},
		{"eventing", "none"},
		{"analytics", "none"},
		{"backup", "none"},
		{"", "read"},
	}},
	"views_reader": {params: []string{"bucket_name"}, patterns: []rolePattern{
		{"bucket[bucket_name].views", "read"},
		{"bucket[bucket_name].data.docs", "read"},
		{"pools", "read"},
	}},
	"replication_admin": {params: []string{}, patterns: []rolePattern{
		{"bucket[any].xdcr", "all"},
		{"bucket[any].data", "read"},
		{"bucket[any].settings", "read"},
		{"bucket[any].stats", "read"},
		{"bucket[any].collections", "read"},
		{"bucket[any]", "none"},
		{"xdcr", "all"},
		{"admin", "none"},
		{"eventing", "none"},
		{"analytics", "none"},
		{"backup", "none"},
		{"", "read"},
	}},
	"data_reader": {params: []string{"bucket_name", "scope_name", "collection_name"}, patterns: []rolePattern{
		{"collection[bucket_name,scope_name,collection_name].data.docs", "read"},
		{"collection[bucket_name,scope_name,collection_name].data.meta", "read"},
		{"collection[bucket_name,scope_name,collection_name].data.xattr", "read"},
		{"bucket[bucket_name].settings", "read"},
		{"pools", "read"},
	}},
	"data_writer": {params: []string{"bucket_name", "scope_name", "collection_name"}, patterns: []rolePattern{
		{"collection[bucket_name,scope_name,collection_name].data.docs", "insert,upsert,delete"},
		{"collection[bucket_name,scope_name,collection_name].data.xattr", "write"},
		{"bucket[bucket_name].settings", "read"},
		{"pools", "read"},
	}},
	"data_dcp_reader": {params: []string{"bucket_name", "scope_name", "collection_name"}, patterns: []rolePattern{
		{"collection[bucket_name,scope_name,collection_name].data.docs", "read"},
		{"collection[bucket_name,scope_name,collection_name].data.meta", "read"},
		{"collection[bucket_name,scope_name,collection_name].data.dcpstream", "read"},
		{"collection[bucket_name,scope_name,collection_name].data.sxattr", "read"},
		{"collection[bucket_name,scope_name,collection_name].data.xattr", "read"},
		{"collection[bucket_name,scope_name,collection_name].collections", "read"},
		{"bucket[bucket_name].data.dcp", "read"},
		{"bucket[bucket_name].settings", "read"},
		{"admin.memcached.idle", "write"},
		{"pools", "read"},
	}},
	"data_backup": {params: []string{"bucket_name"}, patterns: []rolePattern{
		{"collection[bucket_name,any,any].collections", "all"},
		{"bucket[bucket_name].data", "all"},
		{"bucket[bucket_name].views", "read,write"},
		{"bucket[bucket_name].fts", "read,write,manage"},
		{"bucket[bucket_name].stats", "read"},
		{"bucket[bucket_name].settings", "read"},
		{"bucket[bucket_name].n1ql.index", "create,list,build"},
		{"bucket[bucket_name].n1ql.meta", "backup"},
		{"bucket[bucket_name].analytics", "manage,select"},
		{"analytics", "select,backup"},
		{"pools", "read"},
	}},
	"data_monitoring": {params: []string{"bucket_name", "scope_name", "collection_name"}, patterns: []rolePattern{
		{"collection[bucket_name,scope_name,collection_name].stats", "read"},
		{"collection[bucket_name,scope_name,collection_name].collections", "read"},
		{"bucket[bucket_name].settings", "read"},
		{"tasks", "read"},
		{"pools", "read"},
	}},
	"fts_admin": {params: []string{"bucket_name"}, patterns: []rolePattern{
		{"bucket[bucket_name].fts", "read,write,manage"},
		{"bucket[bucket_name].collections", "read"},
		{"bucket[bucket_name].data.docs", "read"},
		{"settings.fts", "read,write,manage"},
		{"ui", "read"},
		{"pools", "read"},
		{"bucket[bucket_name].settings", "read"},
	}},
	"fts_searcher": {params: []string{"bucket_name", "scope_name", "collection_name"}, patterns: []rolePattern{
		{"collection[bucket_name,scope_name,collection_name].fts", "read"},
		{"settings.fts", "read"},
		{"ui", "read"},
		{"pools", "read"},
		{"bucket[bucket_name].settings", "read"},
	}},
	"query_select": {params: []string{"bucket_name", "scope_name", "collection_name"}, patterns: []rolePattern{
		{"collection[bucket_name,scope_name,collection_name].n1ql.select", "execute"},
		{"collection[bucket_name,scope_name,collection_name].data.docs", "read"},
		{"bucket[bucket_name].settings", "read"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"query_update": {params: []string{"bucket_name", "scope_name", "collection_name"}, patterns: []rolePattern{
		{"collection[bucket_name,scope_name,collection_name].n1ql.update", "execute"},
		{"collection[bucket_name,scope_name,collection_name].data.docs", "upsert"},
		{"bucket[bucket_name].settings", "read"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"query_insert": {params: []string{"bucket_name", "scope_name", "collection_name"}, patterns: []rolePattern{
		{"collection[bucket_name,scope_name,collection_name].n1ql.insert", "execute"},
		{"collection[bucket_name,scope_name,collection_name].data.docs", "insert"},
		{"bucket[bucket_name].settings", "read"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"query_delete": {params: []string{"bucket_name", "scope_name", "collection_name"}, patterns: []rolePattern{
		{"collection[bucket_name,scope_name,collection_name].n1ql.delete", "execute"},
		{"collection[bucket_name,scope_name,collection_name].data.docs", "delete"},
		{"bucket[bucket_name].settings", "read"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"query_manage_index": {params: []string{"bucket_name", "scope_name", "collection_name"}, patterns: []rolePattern{
		{"collection[bucket_name,scope_name,collection_name].n1ql.index", "all"},
		{"collection[bucket_name,scope_name,collection_name].collections", "read"},
		{"bucket[bucket_name].settings", "read"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"query_system_catalog": {params: []string{}, patterns: []rolePattern{
		{"bucket[any].n1ql.index", "list"},
		{"bucket[any].settings", "read"},
		{"n1ql.meta", "read"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"query_external_access": {params: []string{}, patterns: []rolePattern{
		{"n1ql.curl", "execute"},
		{"bucket[any].settings", "read"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"query_manage_global_functions": {params: []string{}, patterns: []rolePattern{
		{"n1ql.udf", "manage"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"query_execute_global_functions": {params: []string{}, patterns: []rolePattern{
		{"n1ql.udf", "execute"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"query_manage_functions": {params: []string{"bucket_name", "scope_name"}, patterns: []rolePattern{
		{"collection[bucket_name,scope_name,any].n1ql.udf", "manage"},
		{"collection[bucket_name,scope_name,any].collections", "read"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"query_execute_functions": {params: []string{"bucket_name", "scope_name"}, patterns: []rolePattern{
		{"collection[bucket_name,scope_name,any].n1ql.udf", "execute"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"query_manage_global_external_functions": {params: []string{}, patterns: []rolePattern{
		{"n1ql.udf_external", "manage"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"query_execute_global_external_functions": {params: []string{}, patterns: []rolePattern{
		{"n1ql.udf_external", "execute"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"query_manage_external_functions": {params: []string{"bucket_name", "scope_name"}, patterns: []rolePattern{
		{"collection[bucket_name,scope_name,any].n1ql.udf_external", "manage"},
		{"collection[bucket_name,scope_name,any].collections", "read"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"query_execute_external_functions": {params: []string{"bucket_name", "scope_name"}, patterns: []rolePattern{
		{"collection[bucket_name,scope_name,any].n1ql.udf_external", "execute"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"replication_target": {params: []string{"bucket_name"}, patterns: []rolePattern{
		{"bucket[bucket_name].settings", "read"},
		{"bucket[bucket_name].data.meta", "read,write"},
		{"bucket[bucket_name].stats", "read"},
		{"bucket[bucket_name].collections", "read"},
		{"pools", "read"},
	}},
	"analytics_manager": {params: []string{"bucket_name"}, patterns: []rolePattern{
		{"bucket[bucket_name].analytics", "manage,select"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"analytics_reader": {params: []string{}, patterns: []rolePattern{
		{"analytics", "select"},
		{"bucket[any].analytics", "select"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"analytics_select": {params: []string{"bucket_name", "scope_name", "collection_name"}, patterns: []rolePattern{
		{"collection[bucket_name,scope_name,collection_name].analytics", "select"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"analytics_admin": {params: []string{}, patterns: []rolePattern{
		{"analytics", "manage"},
		{"bucket[any].analytics", "manage"},
		{"ui", "read"},
		{"pools", "read"},
	}},
	"mobile_sync_gateway": {params: []string{"bucket_name"}, patterns: []rolePattern{
		{"bucket[bucket_name].data", "all"},
		{"bucket[bucket_name].views", "all"},
		{"bucket[bucket_name].n1ql.index", "all"},
		{"bucket[bucket_name].n1ql", "execute"},
		{"bucket[bucket_name]", "read,flush"},
		{"bucket[bucket_name].settings", "read"},
		{"admin.memcached.idle", "write"},
		{"settings.autocompaction", "read"},
		{"pools", "read"},
	}},
	"external_stats_reader": {params: []string{}, patterns: []rolePattern{
		{"admin.stats_export", "read"},
	}},
}

// Param is a parameter of a vertex or a role: the name of a bucket, scope or
// collection, or one of the atoms any and all of ns_server, which no name
// can be.
type Param struct {
	Name string
	atom string
}

// The atoms of an object: any matches any value of a pattern, and all, which
// requests use for a scope or collection they do not name, only a pattern's
// any.
var (
	ParamAny = Param{atom: "any"}
	ParamAll = Param{atom: "all"}
)

// NameParam is the parameter naming a bucket, scope or collection.
func NameParam(name string) Param {
	return Param{Name: name}
}

// Vertex is a vertex of an ns_server RBAC object: a name, and for a bucket,
// scope or collection its parameters.
type Vertex struct {
	Name   string
	Params []Param
}

// RBACPermission is an ns_server permission: an object and an operation on it.
type RBACPermission struct {
	Object    []Vertex
	Operation string
}

// BucketVertex is the vertex {bucket, Bucket}.
func BucketVertex(bucket Param) Vertex {
	return Vertex{Name: "bucket", Params: []Param{bucket}}
}

// CollectionVertex is the vertex {collection, [Bucket, Scope, Collection]}.
func CollectionVertex(bucket, scope, collection Param) Vertex {
	return Vertex{Name: "collection", Params: []Param{bucket, scope, collection}}
}

// PlainVertices are vertices without parameters, such as [admin, security].
func PlainVertices(names ...string) []Vertex {
	vertices := make([]Vertex, len(names))
	for i, name := range names {
		vertices[i] = Vertex{Name: name}
	}
	return vertices
}

// String formats the permission as menelaus_web_rbac:format_permission/1.
func (p RBACPermission) String() string {
	parts := []string{"cluster"}
	for _, vertex := range p.Object {
		if vertex.Params == nil {
			parts = append(parts, vertex.Name)
			continue
		}
		params := make([]string, len(vertex.Params))
		for i, param := range vertex.Params {
			switch param {
			case ParamAll:
				params[i] = "*"
			case ParamAny:
				params[i] = "."
			default:
				params[i] = param.Name
			}
		}
		parts = append(parts, vertex.Name+"["+strings.Join(params, ":")+"]")
	}
	return strings.Join(parts, ".") + "!" + p.Operation
}

// parsePattern parses the vertices of a role pattern, replacing the
// parameters named in subst; the others are any.
func parsePattern(object string, subst map[string]Param) []Vertex {
	if object == "" {
		return nil
	}
	var vertices []Vertex
	for _, part := range strings.Split(object, ".") {
		name, params, hasParams := strings.Cut(part, "[")
		vertex := Vertex{Name: name}
		if hasParams {
			for _, param := range strings.Split(strings.TrimSuffix(params, "]"), ",") {
				value, ok := subst[param]
				if !ok {
					value = ParamAny
				}
				vertex.Params = append(vertex.Params, value)
			}
		}
		vertices = append(vertices, vertex)
	}
	return vertices
}

// expandVertex expands a bucket, scope or collection vertex to a collection
// vertex of three parameters, padded with pad, as menelaus_roles does. A
// vertex of that name without parameters is not expanded.
func expandVertex(vertex Vertex, pad Param) Vertex {
	if vertex.Params == nil {
		return vertex
	}
	switch vertex.Name {
	case "bucket", "scope", "collection":
		params := append([]Param{}, vertex.Params...)
		for len(params) < 3 {
			params = append(params, pad)
		}
		return Vertex{Name: "collection", Params: params}
	}
	return vertex
}

// objectMatch is menelaus_roles:object_match/2.
func objectMatch(object, pattern []Vertex) bool {
	if len(pattern) > len(object) {
		return false
	}
	for i, patternVertex := range pattern {
		objectVertex := expandVertex(object[i], ParamAll)
		patternVertex = expandVertex(patternVertex, ParamAny)
		if objectVertex.Name != patternVertex.Name || len(objectVertex.Params) != len(patternVertex.Params) {
			return false
		}
		for j, param := range objectVertex.Params {
			if param != ParamAny && patternVertex.Params[j] != ParamAny && param != patternVertex.Params[j] {
				return false
			}
		}
	}
	return true
}

type compiledPattern struct {
	object     []Vertex
	operations string
}

// compileRoles returns the permission patterns of the user's roles with
// their parameters filled in. A role on a bucket, scope or collection that
// does not exist grants nothing, as compile_roles leaves it out.
func (u *User) compileRoles(res Resources) [][]compiledPattern {
	var roles [][]compiledPattern
	for _, role := range u.allRoles() {
		if role.valid(res) {
			roles = append(roles, role.compile())
		}
	}
	return roles
}

// compile returns the permission patterns of the role with its parameters
// filled in.
func (r *UserRole) compile() []compiledPattern {
	definition := roleDefinitions[r.Name]
	subst := make(map[string]Param)
	for i, value := range r.params() {
		subst[definition.params[i]] = value
	}
	var patterns []compiledPattern
	for _, pattern := range definition.patterns {
		patterns = append(patterns, compiledPattern{
			object:     parsePattern(pattern.object, subst),
			operations: pattern.operations,
		})
	}
	return patterns
}

// isAllowed is menelaus_roles:is_allowed/2.
func isAllowed(roles [][]compiledPattern, permission RBACPermission) bool {
	for _, patterns := range roles {
		for _, pattern := range patterns {
			if !objectMatch(permission.Object, pattern.object) {
				continue
			}
			if pattern.operations == "all" || pattern.operations != "none" && permission.Operation == "any" {
				return true
			}
			for _, allowed := range strings.Split(pattern.operations, ",") {
				if allowed == permission.Operation {
					return true
				}
			}
			break
		}
	}
	return false
}

// IsAllowed reports whether ns_server allows the user permission.
func (u *User) IsAllowed(res Resources, permission RBACPermission) bool {
	return isAllowed(u.compileRoles(res), permission)
}

// Privileges are the privileges of a user on a bucket in the RBAC database
// ns_server's memcached_permissions gives kv_engine: those on the bucket, and
// those on scopes and collections it does not hold on the bucket.
type Privileges struct {
	privileges map[string]bool
	children   map[string]*Privileges
}

func newPrivileges() *Privileges {
	return &Privileges{privileges: make(map[string]bool), children: make(map[string]*Privileges)}
}

// granted is memcached_permissions:priv_is_granted/3.
func (p *Privileges) granted(privilege string, path []string) bool {
	node := p
	for _, key := range path {
		if node = node.children[key]; node == nil {
			return false
		}
		if node.privileges[privilege] {
			return true
		}
	}
	return false
}

// set is memcached_permissions:priv_set/3.
func (p *Privileges) set(privilege string, path []string) {
	node := p
	for _, key := range path {
		child := node.children[key]
		if child == nil {
			child = newPrivileges()
			node.children[key] = child
		}
		node = child
	}
	node.privileges[privilege] = true
	node.erase(privilege)
}

// erase is memcached_permissions:priv_erase/2, applied to the children.
func (p *Privileges) erase(privilege string) {
	for key, child := range p.children {
		delete(child.privileges, privilege)
		child.erase(privilege)
		if len(child.privileges) == 0 && len(child.children) == 0 {
			delete(p.children, key)
		}
	}
}

// collectionPermissions is memcached_permissions:collection_permissions_to_check/1.
var collectionPermissions = []struct {
	object    []string
	operation string
	privilege string
}{
	{[]string{"data", "docs"}, "read", "Read"},
	{[]string{"data", "docs"}, "insert", "Insert"},
	{[]string{"data", "docs"}, "delete", "Delete"},
	{[]string{"data", "docs"}, "upsert", "Upsert"},
	{[]string{"data", "meta"}, "read", "MetaRead"},
	{[]string{"data", "meta"}, "write", "MetaWrite"},
	{[]string{"data", "xattr"}, "read", "XattrRead"},
	{[]string{"data", "xattr"}, "write", "XattrWrite"},
	{[]string{"data", "sxattr"}, "read", "SystemXattrRead"},
	{[]string{"data", "sxattr"}, "write", "SystemXattrWrite"},
	{[]string{"data", "dcpstream"}, "read", "DcpStream"},
	{[]string{"stats"}, "read", "SimpleStats"},
}

// BucketPrivileges returns the privileges of the user on bucket, as
// memcached_permissions:check_permissions/4 computes them, or nil when the
// user has none, and kv_engine refuses SELECT_BUCKET.
func (u *User) BucketPrivileges(res Resources, bucket string) *Privileges {
	roles := u.compileRoles(res)
	tree := newPrivileges()
	for _, check := range []struct {
		permission RBACPermission
		privilege  string
	}{
		{RBACPermission{PlainVertices("admin", "internal", "stats"), "read"}, "SimpleStats"},
		{RBACPermission{append([]Vertex{BucketVertex(NameParam(bucket))}, PlainVertices("data", "dcp")...), "read"}, "DcpProducer"},
		{RBACPermission{append([]Vertex{BucketVertex(NameParam(bucket))}, PlainVertices("data", "dcp")...), "write"}, "DcpConsumer"},
	} {
		if isAllowed(roles, check.permission) {
			tree.set(check.privilege, []string{bucket})
		}
	}

	params := [][]Param{{NameParam(bucket), ParamAny, ParamAny}}
	for _, role := range u.allRoles() {
		if len(roleDefinitions[role.Name].params) != 3 || !role.valid(res) {
			continue
		}
		roleParams := role.params()
		if roleParams[0] == NameParam(bucket) && roleParams[1] != ParamAny {
			params = append(params, roleParams)
		}
	}
	for _, param := range params {
		toStore := []string{bucket}
		object := []Param{NameParam(bucket)}
		for _, value := range param[1:] {
			if value == ParamAny {
				object = append(object, ParamAll)
				continue
			}
			toStore = append(toStore, value.Name)
			object = append(object, value)
		}
		for _, check := range collectionPermissions {
			permission := RBACPermission{
				Object:    append([]Vertex{CollectionVertex(object[0], object[1], object[2])}, PlainVertices(check.object...)...),
				Operation: check.operation,
			}
			if !tree.granted(check.privilege, toStore) && isAllowed(roles, permission) {
				tree.set(check.privilege, toStore)
			}
		}
	}
	return tree.children[bucket]
}

// GlobalPrivileges returns the privileges of the user outside buckets, as
// memcached_permissions:global_permissions/1 computes them.
func (u *User) GlobalPrivileges(res Resources) map[string]bool {
	roles := u.compileRoles(res)
	privileges := make(map[string]bool)
	for _, check := range []struct {
		object    []string
		operation string
		privilege string
	}{
		{[]string{"stats", "memcached"}, "read", "Stats"},
		{[]string{"admin", "internal", "stats"}, "read", "Stats"},
		{[]string{"buckets"}, "create", "BucketManagement"},
		{[]string{"admin", "memcached", "node"}, "write", "NodeManagement"},
		{[]string{"admin", "memcached", "session"}, "write", "SessionManagement"},
		{[]string{"admin", "memcached", "idle"}, "write", "IdleConnection"},
		{[]string{"admin", "security", "audit"}, "write", "AuditManagement"},
		{[]string{"pools"}, "read", "SystemSettings"},
	} {
		if isAllowed(roles, RBACPermission{PlainVertices(check.object...), check.operation}) {
			privileges[check.privilege] = true
		}
	}
	return privileges
}

// Access is the result of kv_engine's PrivilegeContext::check.
type Access int

// The values of cb::rbac::PrivilegeAccess::Status.
const (
	AccessOk Access = iota
	AccessFail
	AccessFailNoPrivileges
)

// collectionPrivileges are kv_engine's is_collection_privilege/1.
var collectionPrivileges = map[string]bool{
	"Read": true, "Insert": true, "Delete": true, "Upsert": true, "MetaRead": true,
	"MetaWrite": true, "XattrRead": true, "SystemXattrRead": true, "XattrWrite": true,
	"SystemXattrWrite": true, "DcpStream": true, "SimpleStats": true,
}

// HasScopePrivileges reports whether the user has privileges on a scope or
// collection of the bucket, kv_engine's Bucket::hasScopePrivileges.
func (p *Privileges) HasScopePrivileges() bool {
	return p != nil && len(p.children) > 0
}

// Check is kv_engine's Bucket::check, Scope::check and Collection::check of
// privilege on the bucket, or on collection in scope when scoped. A nil p
// grants nothing.
func (p *Privileges) Check(privilege string, scoped bool, scope, collection string) Access {
	if p == nil {
		return AccessFail
	}
	if p.privileges[privilege] {
		return AccessOk
	}
	if !scoped || !collectionPrivileges[privilege] {
		return AccessFail
	}
	collectionPrivilegeExists := false
	for held := range p.privileges {
		if collectionPrivileges[held] {
			collectionPrivilegeExists = true
		}
	}
	scopePrivileges := p.children[scope]
	if scopePrivileges == nil {
		if collectionPrivilegeExists {
			return AccessFail
		}
		return AccessFailNoPrivileges
	}
	if scopePrivileges.privileges[privilege] {
		return AccessOk
	}
	if collection == "" {
		return AccessFail
	}
	if collectionNode := scopePrivileges.children[collection]; collectionNode != nil {
		if collectionNode.privileges[privilege] {
			return AccessOk
		}
		return AccessFail
	}
	if len(scopePrivileges.privileges) > 0 || collectionPrivilegeExists {
		return AccessFail
	}
	return AccessFailNoPrivileges
}

// CanRead reports whether kv_engine's privilege check for Read on the scope,
// or on the collection when it is not empty, finds a privilege there: the
// scope or collection is visible to the user.
func (p *Privileges) CanRead(scope, collection string) bool {
	return p != nil && p.Check("Read", true, scope, collection) != AccessFailNoPrivileges
}

// InvalidRolesError lists the roles ns_server refuses to assign: unknown,
// malformed, or with parameters naming a bucket, scope or collection that
// does not exist.
type InvalidRolesError struct {
	Roles []string
}

func (e *InvalidRolesError) Error() string {
	return "Cannot assign roles to user because the following roles are unknown, malformed or role " +
		"parameters are undefined: [" + strings.Join(e.Roles, ",") + "]"
}

// parseRole is menelaus_web_rbac:parse_role/2: a known role name in any
// case, alone or followed by its parameters in brackets, separated by colons,
// or "[*]". string:to_lower/1 lowers the bytes of a name, and one with a byte
// past ASCII names no role.
func parseRole(raw string) (*UserRole, bool) {
	name, rest, hasParams := strings.Cut(raw, "[")
	name = strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' {
			return r + 'a' - 'A'
		}
		return r
	}, name)
	if _, ok := roleDefinitions[name]; !ok {
		return nil, false
	}
	role := &UserRole{Name: name}
	if !hasParams {
		return role, true
	}
	params, tail, closed := strings.Cut(rest, "]")
	if !closed || tail != "" || params == "" {
		return nil, false
	}
	role.given = strings.Split(params, ":")
	for i, field := range []*string{&role.BucketName, &role.ScopeName, &role.CollectionName} {
		if i < len(role.given) {
			*field = role.given[i]
		}
	}
	return role, true
}

// params returns the parameters of the role, those not given as any, as
// menelaus_web_rbac:adjust_role/3 aligns them. A role given more parameters
// than it takes keeps them all.
func (r *UserRole) params() []Param {
	definition := roleDefinitions[r.Name].params
	params := make([]Param, len(definition))
	if len(r.given) > len(definition) {
		params = make([]Param, len(r.given))
	}
	for i := range params {
		params[i] = ParamAny
		if i < len(r.given) {
			params[i] = NameParam(r.given[i])
		}
	}
	if len(r.given) == 1 && r.given[0] == "*" {
		params[0] = ParamAny
	}
	return params
}

// valid is menelaus_roles:validate_role/3: the role takes as many parameters
// as it has, and each names an existing bucket, scope in it, or collection in
// that, unless any. Without res only the number of parameters is checked.
func (r *UserRole) valid(res Resources) bool {
	definition, ok := roleDefinitions[r.Name]
	if !ok || len(r.given) == 0 && len(definition.params) > 0 || len(r.given) > len(definition.params) {
		return false
	}
	if res == nil {
		return true
	}
	params := r.params()
	var bucket, scope string
	for i, param := range params {
		if param == ParamAny {
			continue
		}
		switch definition.params[i] {
		case "bucket_name":
			if !res.HasBucket(param.Name) {
				return false
			}
			bucket = param.Name
		case "scope_name":
			if bucket == "" || !res.HasScope(bucket, param.Name) {
				return false
			}
			scope = param.Name
		case "collection_name":
			if scope == "" || !res.HasCollection(bucket, scope, param.Name) {
				return false
			}
		}
	}
	return true
}

// String is menelaus_web_rbac:role_to_string/1: the parameters without
// those left as any at the end, or "*" when all are.
func (r *UserRole) String() string {
	if len(r.given) == 0 {
		return r.Name
	}
	params := r.params()
	end := len(params)
	for end > 0 && params[end-1] == ParamAny {
		end--
	}
	if end == 0 {
		return r.Name + "[*]"
	}
	names := make([]string, end)
	for i, param := range params[:end] {
		names[i] = param.Name
	}
	return r.Name + "[" + strings.Join(names, ":") + "]"
}

// erlangLess orders roles as Erlang orders the terms ns_server keeps them as:
// a role without parameters is an atom, before any role with parameters, a
// {Name, Params} tuple, and the atom any is before any name.
func erlangLess(a, b *UserRole) bool {
	if (len(a.given) == 0) != (len(b.given) == 0) {
		return len(a.given) == 0
	}
	if a.Name != b.Name {
		return a.Name < b.Name
	}
	pa, pb := a.params(), b.params()
	for i := 0; i < len(pa) && i < len(pb); i++ {
		if pa[i] == pb[i] {
			continue
		}
		if pa[i] == ParamAny || pb[i] == ParamAny {
			return pa[i] == ParamAny
		}
		return pa[i].Name < pb[i].Name
	}
	return len(pa) < len(pb)
}

// parseRoles parses roles and refuses invalid ones as
// menelaus_web_rbac:validate_roles/2 and menelaus_users:store_user/5 do: when
// some do not parse, those in the order given, then the others that are
// invalid in the reverse of that order; otherwise the invalid ones in the
// reverse of the order of the sorted, unique roles.
func parseRoles(raws []string, res Resources) ([]*UserRole, error) {
	var roles []*UserRole
	var malformed []string
	for _, raw := range raws {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		role, ok := parseRole(raw)
		if !ok {
			malformed = append(malformed, raw)
			continue
		}
		roles = append(roles, role)
	}

	checked := roles
	if len(malformed) == 0 {
		checked = sortedUniqueRoles(roles)
	}
	invalid := malformed
	for i := len(checked) - 1; i >= 0; i-- {
		if !checked[i].valid(res) {
			invalid = append(invalid, checked[i].String())
		}
	}
	if len(invalid) > 0 {
		return nil, &InvalidRolesError{Roles: invalid}
	}
	return roles, nil
}

// ValidateRoles refuses roles as menelaus_web_rbac:validate_roles/2 does,
// with an *InvalidRolesError.
func (e *Engine) ValidateRoles(raws []string) error {
	lock.RLock()
	defer lock.RUnlock()
	_, err := parseRoles(raws, e.resources)
	return err
}

// IsSecurityRole reports whether the role is one of
// menelaus_roles:get_security_roles/1: a role that allows some operation on
// [admin, security].
func (r *UserRole) IsSecurityRole() bool {
	definition, ok := roleDefinitions[r.Name]
	if !ok || len(definition.params) > 0 {
		return false
	}
	var patterns []compiledPattern
	for _, pattern := range definition.patterns {
		patterns = append(patterns, compiledPattern{object: parsePattern(pattern.object, nil), operations: pattern.operations})
	}
	return isAllowed([][]compiledPattern{patterns}, RBACPermission{Object: PlainVertices("admin", "security"), Operation: "any"})
}

// RoleParam is a parameter of a role and its value, "*" for any.
type RoleParam struct {
	Name  string
	Value string
}

// Params returns the parameters of the role by name, as
// menelaus_web_rbac:role_to_json/1 lists them.
func (r *UserRole) Params() []RoleParam {
	definition := roleDefinitions[r.Name].params
	var params []RoleParam
	for i, value := range r.params() {
		if i >= len(definition) {
			break
		}
		name := value.Name
		if value == ParamAny {
			name = "*"
		}
		params = append(params, RoleParam{Name: definition[i], Value: name})
	}
	return params
}

// ParseRole parses a role as menelaus_web_rbac:parse_role/2 does.
func ParseRole(raw string) (*UserRole, bool) {
	return parseRole(strings.TrimSpace(raw))
}

// ParsePermission parses a permission as menelaus_web_rbac:parse_permission/1
// does: "cluster", vertices each after a ".", with one parameter in brackets
// for a bucket, two for a scope and three for a collection, separated by
// colons, "." for any, then "!" and the operation. A name ns_server does not
// know is kept, and matches no pattern that names a vertex.
func ParsePermission(raw string) (RBACPermission, bool) {
	parts := strings.Split(raw, "!")
	if len(parts) != 2 || !strings.HasPrefix(parts[0], "cluster") {
		return RBACPermission{}, false
	}
	permission := RBACPermission{Operation: parts[1]}
	rest := strings.TrimPrefix(parts[0], "cluster")
	for rest != "" {
		if rest[0] != '.' {
			return RBACPermission{}, false
		}
		rest = rest[1:]
		end := strings.IndexAny(rest, ".[")
		if end < 0 {
			end = len(rest)
		}
		vertex := Vertex{Name: rest[:end]}
		rest = rest[end:]
		if strings.HasPrefix(rest, "[") {
			params, tail, ok := strings.Cut(rest[1:], "]")
			if !ok {
				return RBACPermission{}, false
			}
			values := []string{params}
			switch vertex.Name {
			case "bucket":
			case "scope", "collection":
				values = strings.Split(params, ":")
				if vertex.Name == "scope" && len(values) != 2 || vertex.Name == "collection" && len(values) != 3 {
					return RBACPermission{}, false
				}
			default:
				return RBACPermission{}, false
			}
			for _, value := range values {
				param := NameParam(value)
				if value == "." {
					param = ParamAny
				}
				vertex.Params = append(vertex.Params, param)
			}
			rest = tail
		}
		permission.Object = append(permission.Object, vertex)
	}
	return permission, true
}

// ErrPermissionParams is the error of a permission whose parameters
// menelaus_roles:compile_params/3 fails on: a name after any, past names of
// what exists.
var ErrPermissionParams = errors.New("parameters of the permission cannot be compiled")

// RolesByPermission returns the roles of the engine as
// menelaus_roles:produce_roles_by_permission/2 lists them: a role without
// parameters, and a role with parameters once with each as any and, when the
// permission names a bucket, scope or collection first, once with each
// leading part of its parameters, those that name what exists. With a
// permission, only the roles that allow it.
func (e *Engine) RolesByPermission(permission *RBACPermission) ([]*UserRole, error) {
	lock.RLock()
	res := e.resources
	lock.RUnlock()

	var permissionParams []Param
	if permission != nil && len(permission.Object) > 0 {
		switch first := permission.Object[0]; first.Name {
		case "bucket", "scope", "collection":
			permissionParams = first.Params
		}
	}
	if res != nil && paramsCrash(permissionParams, res) {
		return nil, ErrPermissionParams
	}
	var roles []*UserRole
	for _, clusterRole := range e.roles {
		count := len(roleDefinitions[clusterRole.Role].params)
		var candidates []*UserRole
		if count == 0 {
			candidates = append(candidates, &UserRole{Name: clusterRole.Role})
		}
		seen := make(map[string]bool)
		for leading := 0; leading <= count && count > 0; leading++ {
			var given []string
			for i := 0; i < leading && i < len(permissionParams) && permissionParams[i] != ParamAny; i++ {
				given = append(given, permissionParams[i].Name)
			}
			if len(given) == 0 {
				given = []string{"*"}
			}
			role := &UserRole{Name: clusterRole.Role, given: given}
			if key := role.String(); !seen[key] {
				seen[key] = true
				candidates = append(candidates, role)
			}
		}
		for _, role := range candidates {
			if !role.valid(res) {
				continue
			}
			if permission != nil && !isAllowed([][]compiledPattern{role.compile()}, *permission) {
				continue
			}
			roles = append(roles, role)
		}
	}
	return roles, nil
}

// paramsCrash reports whether compile_params/3 fails on the parameters
// calculate_possible_param_values/3 takes from those of a permission for the
// roles of three parameters: after any it looks a name up in the atom any,
// unless one before it named nothing.
func paramsCrash(params []Param, res Resources) bool {
	afterAny := false
	var bucket, scope string
	for i, param := range params[:min(len(params), 3)] {
		switch {
		case param == ParamAny:
			afterAny = true
			continue
		case afterAny:
			return true
		}
		switch i {
		case 0:
			if !res.HasBucket(param.Name) {
				return false
			}
			bucket = param.Name
		case 1:
			// A bucket without collections has no manifest to look in.
			if !res.HasScope(bucket, param.Name) {
				return false
			}
			scope = param.Name
		case 2:
			if !res.HasCollection(bucket, scope, param.Name) {
				return false
			}
		}
	}
	return false
}
