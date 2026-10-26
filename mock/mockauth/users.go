package mockauth

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

// UserRole represents the roles of a user.
type UserRole struct {
	Name           string
	BucketName     string
	ScopeName      string
	CollectionName string

	// given are the parameters in brackets, as given.
	given []string
}

// ClusterRole represents the roles of a cluster: the name of a role, and the
// name and description it is listed with.
type ClusterRole struct {
	Role        string
	Name        string
	Description string
}

// Group represents a group that a user may be part of.
type Group struct {
	Name         string
	Description  *string
	LDAPGroupRef *string
	Roles        []*UserRole
}

// User represents a single user in the system. Domain is "local" or
// "external".
type User struct {
	DisplayName     string
	Username        string
	Domain          string
	Password        string
	PasswordChanged time.Time
	Groups          []*Group
	Roles           []*UserRole

	// hasDisplayName tells whether the user has a name, which may be empty.
	hasDisplayName bool
}

// UpsertUserOptions allows you to specify initial options for a new user.
type UpsertUserOptions struct {
	Username string
	// Domain is "local", the default, or "external".
	Domain      string
	DisplayName string
	// DisplayNameGiven gives the user an empty name rather than none.
	DisplayNameGiven bool
	// Roles are the roles assigned to the user that are of type "user".
	Roles    []string
	Groups   []string
	Password string
}

// UpsertGroupOptions are the details of a group.
type UpsertGroupOptions struct {
	Name         string
	Description  *string
	LDAPGroupRef *string
	Roles        []string
}

// Errors of the user management engine, with ns_server's messages.
var (
	ErrPasswordRequired = errors.New("Password is required for new user.")
	ErrUserNotFound     = errors.New("User was not found.")
	ErrGroupNotFound    = errors.New("Group was not found.")
)

// MissingGroupsError lists the groups given to a user that do not exist.
type MissingGroupsError struct {
	Groups []string
}

func (e *MissingGroupsError) Error() string {
	return "Groups do not exist: " + strings.Join(e.Groups, ",")
}

// lock guards the users and groups of every engine, and their details,
// which request handlers read concurrently.
var lock sync.RWMutex

// Engine represents the high level user management engine.
type Engine struct {
	// admin is the administrator, of ns_server's admin domain, who is not
	// one of its users.
	admin     *User
	users     []*User
	groups    []*Group
	roles     []*ClusterRole
	resources Resources
}

// SetResources sets what the engine checks the parameters of a role
// against.
func (e *Engine) SetResources(res Resources) {
	lock.Lock()
	defer lock.Unlock()
	e.resources = res
}

// NewEngine creates a new user management engine, with the roles
// menelaus_roles:roles/0 of Couchbase Server 7.0.5 defines and the names and
// descriptions it lists them with.
func NewEngine() *Engine {
	return &Engine{
		roles: []*ClusterRole{
			{Role: "admin", Name: "Full Admin", Description: "Can manage all cluster features (including security). This user can access the web console. This user can read and write all data."},
			{Role: "ro_admin", Name: "Read-Only Admin", Description: "Can view all cluster statistics. This user can access the web console. This user can read some data."},
			{Role: "security_admin_local", Name: "Local User Security Admin", Description: "Can view all cluster statistics and manage local user roles, but not grant Full Admin or Security Admin roles to other users or alter their own role. This user can access the web console. This user cannot read data."},
			{Role: "security_admin_external", Name: "External User Security Admin", Description: "Can view all cluster statistics and manage external user roles, but not grant Full Admin or Security Admin roles to other users or alter their own role. This user can access the web console. This user cannot read data."},
			{Role: "cluster_admin", Name: "Cluster Admin", Description: "Can manage all cluster features except security. This user can access the web console. This user cannot read data."},
			{Role: "eventing_admin", Name: "Eventing Full Admin", Description: "Can create/manage eventing functions. This user can access the web console"},
			{Role: "backup_admin", Name: "Backup Full Admin", Description: "Can perform backup related tasks. This user can access the web console"},
			{Role: "bucket_admin", Name: "Bucket Admin", Description: "Can manage ALL bucket features for a given bucket (including start/stop XDCR). This user can access the web console. This user cannot read data."},
			{Role: "scope_admin", Name: "Manage Scopes", Description: "Can create/delete scopes and collections within a given bucket. This user cannot access the web console."},
			{Role: "bucket_full_access", Name: "Application Access", Description: "Full access to bucket data. This user cannot access the web console and is intended only for application access. This user can read and write data."},
			{Role: "views_admin", Name: "Views Admin", Description: "Can create and manage views of a given bucket. This user can access the web console. This user can read some data."},
			{Role: "views_reader", Name: "Views Reader", Description: "Can read data from the views of a given bucket. This user cannot access the web console and is intended only for application access. This user can read some data."},
			{Role: "replication_admin", Name: "XDCR Admin", Description: "Can administer XDCR features to create cluster references and replication streams out of this cluster. This user can access the web console. This user can read some data."},
			{Role: "data_reader", Name: "Data Reader", Description: "Can read data from a given bucket, scope or collection. This user cannot access the web console and is intended only for application access. This user can read data, but cannot write it."},
			{Role: "data_writer", Name: "Data Writer", Description: "Can write data to a given bucket, scope or collection. This user cannot access the web console and is intended only for application access. This user can write data, but cannot read it."},
			{Role: "data_dcp_reader", Name: "Data DCP Reader", Description: "Can initiate DCP streams for a given bucket, scope or collection. This user cannot access the web console and is intended only for application access. This user can read data."},
			{Role: "data_backup", Name: "Data Backup & Restore", Description: "Can backup and restore a given bucket\u0019s data. This user cannot access the web console and is intended only for application access. This user can read data."},
			{Role: "data_monitoring", Name: "Data Monitor", Description: "Can read statistics for a given bucket, scope or collection. This user cannot access the web console and is intended only for application access. This user cannot read data."},
			{Role: "fts_admin", Name: "Search Admin", Description: "Can administer all Full Text Search features. This user can access the web console. This user can read some data."},
			{Role: "fts_searcher", Name: "Search Reader", Description: "Can query Full Text Search indexes for a given bucket, scope or collection. This user can access the web console. This user can read some data."},
			{Role: "query_select", Name: "Query Select", Description: "Can execute a SELECT statement on a given bucket, scope or collection to retrieve data. This user can access the web console and can read data, but not write it."},
			{Role: "query_update", Name: "Query Update", Description: "Can execute an UPDATE statement on a given bucket, scope or collection to update data. This user can access the web console and write data, but cannot read it."},
			{Role: "query_insert", Name: "Query Insert", Description: "Can execute an INSERT statement on a given bucket, scope or collection to add data. This user can access the web console and insert data, but cannot read it."},
			{Role: "query_delete", Name: "Query Delete", Description: "Can execute a DELETE statement on a given bucket, scope or collection to delete data. This user can access the web console, but cannot read data. This user can delete data."},
			{Role: "query_manage_index", Name: "Query Manage Index", Description: "Can manage indexes for a given bucket, scope or collection. This user can access the web console, but cannot read data."},
			{Role: "query_system_catalog", Name: "Query System Catalog", Description: "Can look up system catalog information via N1QL. This user can access the web console, but cannot read user data."},
			{Role: "query_external_access", Name: "Query CURL Access", Description: "Can execute the CURL statement from within N1QL. This user can access the web console, but cannot read data (within Couchbase)."},
			{Role: "query_manage_global_functions", Name: "Manage Global Functions", Description: "Can manage global n1ql functions"},
			{Role: "query_execute_global_functions", Name: "Execute Global Functions", Description: "Can execute global n1ql functions"},
			{Role: "query_manage_functions", Name: "Manage Scope Functions", Description: "Can manage n1ql functions for a given scope"},
			{Role: "query_execute_functions", Name: "Execute Scope Functions", Description: "Can execute n1ql functions for a given scope"},
			{Role: "query_manage_global_external_functions", Name: "Manage Global External Functions", Description: "Can manage global external language functions"},
			{Role: "query_execute_global_external_functions", Name: "Execute Global External Functions", Description: "Can execute global external language functions"},
			{Role: "query_manage_external_functions", Name: "Manage Scope External Functions", Description: "Can manage external language functions for a given scope"},
			{Role: "query_execute_external_functions", Name: "Execute Scope External Functions", Description: "Can execute external language functions for a given scope"},
			{Role: "replication_target", Name: "XDCR Inbound", Description: "Can create XDCR streams into a given bucket. This user cannot access the web console or read any data."},
			{Role: "analytics_manager", Name: "Analytics Manager", Description: "Can manage Analytics local links. Can manage datasets on a given bucket. Can query datasets created on this bucket. This user can access the web console and read some data."},
			{Role: "analytics_reader", Name: "Analytics Reader", Description: "Can query datasets. This is a global role as datasets may be created on different buckets. This user can access the web console and read some data."},
			{Role: "analytics_select", Name: "Analytics Select", Description: "Can query datasets on a given bucket, scope or collection. This user can access the web console and read some data."},
			{Role: "analytics_admin", Name: "Analytics Admin", Description: "Can manage dataverses. Can manage all Analytics links. Can manage all datasets. This user can access the web console but cannot read data."},
			{Role: "mobile_sync_gateway", Name: "Sync Gateway", Description: "Full access to bucket data as required by Sync Gateway. This user cannot access the web console and is intended only for use by Sync Gateway. This user can read and write data, manage indexes and views, and read some cluster information."},
			{Role: "external_stats_reader", Name: "External Stats Reader", Description: "Access to /metrics endpoint for Prometheus integration. Can read all stats for all services. This user cannot access the web console"},
		},
	}
}

// UpsertUser creates or updates a user as menelaus_users:store_user/5 does.
// It refuses invalid roles with an *InvalidRolesError, and a new local user
// without a password with ErrPasswordRequired. It keeps the roles sorted and
// unique. store_user/5 does not check the groups, which its callers do, and
// the user gets those of them that exist.
func (e *Engine) UpsertUser(opts UpsertUserOptions) error {
	if opts.Username == "" {
		return errors.New("username must be set")
	}
	domain := opts.Domain
	if domain == "" {
		domain = "local"
	}

	lock.Lock()
	defer lock.Unlock()

	roles, err := parseRoles(opts.Roles, e.resources)
	if err != nil {
		return err
	}
	var groups []*Group
	for _, name := range opts.Groups {
		if group := e.getGroup(name); group != nil {
			groups = append(groups, group)
		}
	}

	user := e.getUser(opts.Username, domain)
	if user == nil {
		if domain == "local" && opts.Password == "" {
			return ErrPasswordRequired
		}
		user = &User{Username: opts.Username, Domain: domain}
		e.users = append(e.users, user)
	}

	user.DisplayName = opts.DisplayName
	user.hasDisplayName = opts.DisplayName != "" || opts.DisplayNameGiven
	user.Groups = groups
	user.Roles = sortedUniqueRoles(roles)
	if opts.Password != "" {
		user.Password = opts.Password
		// replicated_dets keeps the time a document changed in milliseconds.
		user.PasswordChanged = time.Now().UTC().Truncate(time.Millisecond)
	}

	return nil
}

// SetAdmin sets the credentials of the administrator, who has the role
// admin in ns_server's admin domain and is not listed among the users.
func (e *Engine) SetAdmin(username, password string) {
	lock.Lock()
	defer lock.Unlock()
	e.admin = &User{Username: username, Domain: "admin", Password: password, Roles: []*UserRole{{Name: "admin"}}}
}

// Authenticate returns the user REST credentials authenticate as, as
// menelaus_auth checks them: the administrator, else the local user of the
// name, with its password. It returns nil for neither.
func (e *Engine) Authenticate(username, password string) *User {
	lock.RLock()
	defer lock.RUnlock()
	if e.admin != nil && e.admin.Username == username && e.admin.Password == password {
		return e.admin
	}
	if user := e.getUser(username, "local"); user != nil && user.Password == password {
		return user
	}
	return nil
}

// GetUser retrieves the user a username authenticates as: the
// administrator, or else a local user.
func (e *Engine) GetUser(username string) *User {
	lock.RLock()
	defer lock.RUnlock()
	if e.admin != nil && e.admin.Username == username {
		return e.admin
	}
	return e.getUser(username, "local")
}

// GetUserInDomain retrieves a user of a domain by their username.
func (e *Engine) GetUserInDomain(username, domain string) *User {
	lock.RLock()
	defer lock.RUnlock()
	return e.getUser(username, domain)
}

func (e *Engine) getUser(username, domain string) *User {
	for _, user := range e.users {
		if user.Username == username && user.Domain == domain {
			return user
		}
	}

	return nil
}

// GetAllUsers returns a list of all registered users.
func (e *Engine) GetAllUsers() []*User {
	lock.RLock()
	defer lock.RUnlock()
	return append([]*User{}, e.users...)
}

// GetAllClusterRoles returns a list of all known cluster roles.
func (e *Engine) GetAllClusterRoles() []*ClusterRole {
	return e.roles
}

// DropUser deletes a local user.
func (e *Engine) DropUser(username string) error {
	return e.DropUserInDomain(username, "local")
}

// DropUserInDomain deletes a user of a domain, or returns ErrUserNotFound.
func (e *Engine) DropUserInDomain(username, domain string) error {
	lock.Lock()
	defer lock.Unlock()

	var users []*User
	for _, user := range e.users {
		if user.Username == username && user.Domain == domain {
			continue
		}
		users = append(users, user)
	}
	if len(users) == len(e.users) {
		return ErrUserNotFound
	}
	e.users = users

	return nil
}

// UpsertGroup creates or updates a group as menelaus_users:store_group/4
// does. It refuses invalid roles with an *InvalidRolesError, and keeps the
// roles sorted and unique.
func (e *Engine) UpsertGroup(opts UpsertGroupOptions) error {
	lock.Lock()
	defer lock.Unlock()

	roles, err := parseRoles(opts.Roles, e.resources)
	if err != nil {
		return err
	}
	group := e.getGroup(opts.Name)
	if group == nil {
		group = &Group{Name: opts.Name}
		e.groups = append(e.groups, group)
	}
	group.Description = opts.Description
	group.LDAPGroupRef = opts.LDAPGroupRef
	group.Roles = sortedUniqueRoles(roles)
	return nil
}

// GetGroup retrieves a group by name.
func (e *Engine) GetGroup(name string) *Group {
	lock.RLock()
	defer lock.RUnlock()
	return e.getGroup(name)
}

func (e *Engine) getGroup(name string) *Group {
	for _, group := range e.groups {
		if group.Name == name {
			return group
		}
	}
	return nil
}

// GetAllGroups returns all groups.
func (e *Engine) GetAllGroups() []*Group {
	lock.RLock()
	defer lock.RUnlock()
	return append([]*Group{}, e.groups...)
}

// MissingGroups returns the names that name no group, in the order given.
func (e *Engine) MissingGroups(names []string) []string {
	lock.RLock()
	defer lock.RUnlock()
	return e.missingGroups(names)
}

func (e *Engine) missingGroups(names []string) []string {
	var missing []string
	for _, name := range names {
		if e.getGroup(name) == nil {
			missing = append(missing, name)
		}
	}
	return missing
}

// DropGroup deletes a group, and takes it from its users, or returns
// ErrGroupNotFound.
func (e *Engine) DropGroup(name string) error {
	lock.Lock()
	defer lock.Unlock()

	group := e.getGroup(name)
	if group == nil {
		return ErrGroupNotFound
	}
	var groups []*Group
	for _, g := range e.groups {
		if g != group {
			groups = append(groups, g)
		}
	}
	e.groups = groups
	for _, user := range e.users {
		var kept []*Group
		for _, g := range user.Groups {
			if g != group {
				kept = append(kept, g)
			}
		}
		user.Groups = kept
	}
	return nil
}

// DropRolesOn runs drop, which drops a bucket, scope when scope is not
// empty, or collection when collection is not empty, and when it reports
// success removes the roles of users and groups on it, including those on
// what it holds. ns_server keeps the id of the bucket, scope or collection
// in such a role, and drops a role whose id is gone, even once one of the
// same name is created: drop and the removal run under the lock role changes
// take, so none on one created since is lost.
func (e *Engine) DropRolesOn(bucket, scope, collection string, drop func() bool) {
	lock.Lock()
	defer lock.Unlock()

	if !drop() {
		return
	}

	on := func(params []Param, i int, name string) bool {
		return name == "" || len(params) > i && params[i] == NameParam(name)
	}
	keep := func(roles []*UserRole) []*UserRole {
		var kept []*UserRole
		for _, role := range roles {
			params := role.params()
			if on(params, 0, bucket) && on(params, 1, scope) && on(params, 2, collection) {
				continue
			}
			kept = append(kept, role)
		}
		return kept
	}
	for _, user := range e.users {
		user.Roles = keep(user.Roles)
	}
	for _, group := range e.groups {
		group.Roles = keep(group.Roles)
	}
}

// PasswordIs reports whether password is the user's password.
func (u *User) PasswordIs(password string) bool {
	lock.RLock()
	defer lock.RUnlock()
	return u.Password == password
}

func (u *User) allRoles() []*UserRole {
	lock.RLock()
	defer lock.RUnlock()
	return u.allRolesLocked()
}

func (u *User) allRolesLocked() []*UserRole {
	roles := append([]*UserRole{}, u.Roles...)
	for _, g := range u.Groups {
		roles = append(roles, g.Roles...)
	}
	return roles
}

// RoleWithOrigins is a role of a user and where it has it from: "" for the
// user's own roles, else the name of a group.
type RoleWithOrigins struct {
	Role    *UserRole
	Origins []string
}

// UserInfo is a copy of the details of a user.
type UserInfo struct {
	Username string
	Domain   string
	// DisplayName is nil when the user has no name.
	DisplayName     *string
	PasswordChanged time.Time
	Groups          []string
	// Roles are the roles of the user and its groups as
	// menelaus_web_rbac:user_roles_to_json/2 lists them: in the reverse of
	// Erlang's order of the roles, each with its origins, the user's last.
	Roles []RoleWithOrigins
	// AllRoles are the roles of the user and its groups.
	AllRoles []*UserRole
}

// Info returns a copy of the details of the user.
func (u *User) Info() UserInfo {
	lock.RLock()
	defer lock.RUnlock()

	info := UserInfo{
		Username:        u.Username,
		Domain:          u.Domain,
		PasswordChanged: u.PasswordChanged,
		AllRoles:        u.allRolesLocked(),
	}
	if u.hasDisplayName {
		name := u.DisplayName
		info.DisplayName = &name
	}
	origins := make(map[string]*RoleWithOrigins)
	add := func(origin string, roles []*UserRole) {
		for _, role := range roles {
			key := role.String()
			entry := origins[key]
			if entry == nil {
				entry = &RoleWithOrigins{Role: role}
				origins[key] = entry
			}
			entry.Origins = append([]string{origin}, entry.Origins...)
		}
	}
	add("", u.Roles)
	for _, group := range u.Groups {
		info.Groups = append(info.Groups, group.Name)
		add(group.Name, group.Roles)
	}
	for _, entry := range origins {
		info.Roles = append(info.Roles, *entry)
	}
	sort.Slice(info.Roles, func(i, j int) bool { return erlangLess(info.Roles[j].Role, info.Roles[i].Role) })
	return info
}

// GroupInfo is a copy of the details of a group.
type GroupInfo struct {
	Name         string
	Description  *string
	LDAPGroupRef *string
	// Roles are in the order menelaus_users:store_group/4 keeps them: the
	// reverse of Erlang's order.
	Roles []*UserRole
}

// Info returns a copy of the details of the group.
func (g *Group) Info() GroupInfo {
	lock.RLock()
	defer lock.RUnlock()
	info := GroupInfo{Name: g.Name, Description: g.Description, LDAPGroupRef: g.LDAPGroupRef}
	for i := len(g.Roles) - 1; i >= 0; i-- {
		info.Roles = append(info.Roles, g.Roles[i])
	}
	return info
}

// sortedUniqueRoles sorts roles in Erlang's order and drops repeats, as
// lists:usort/1 does.
func sortedUniqueRoles(roles []*UserRole) []*UserRole {
	sorted := append([]*UserRole{}, roles...)
	sort.SliceStable(sorted, func(i, j int) bool { return erlangLess(sorted[i], sorted[j]) })
	var unique []*UserRole
	for _, role := range sorted {
		if len(unique) == 0 || erlangLess(unique[len(unique)-1], role) {
			unique = append(unique, role)
		}
	}
	return unique
}
