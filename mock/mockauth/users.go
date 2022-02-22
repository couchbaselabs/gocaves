package mockauth

import (
	"errors"
	"strings"
)

// UserRole represents the roles of a user.
type UserRole struct {
	Name           string
	BucketName     string
	ScopeName      string
	CollectionName string
}

// ClusterRole represents the roles of a cluster.
type ClusterRole struct {
	Role        string
	Name        string
	Description string
}

func (r *UserRole) anyBucket() bool {
	return r.BucketName == "" || r.BucketName == "*"
}

func (r *UserRole) anyScope() bool {
	return r.ScopeName == "" || r.ScopeName == "*"
}

func (r *UserRole) anyCollection() bool {
	return r.CollectionName == "" || r.CollectionName == "*"
}

// Group represents a group that a user may be part of.
type Group struct {
	Name  string
	Roles []*UserRole
}

// User represents a single user in the system.
type User struct {
	DisplayName string
	Username    string
	Password    string
	Groups      []*Group
	Roles       []*UserRole
}

// HasPermission checks whether this user has a specific permission, including all roles and groups.
func (u *User) HasPermission(permission Permission, bucket, scope, collection string) bool {
	for _, r := range u.Roles {
		// Check that we have access to the resources first.
		if r.BucketName != bucket && !r.anyBucket() {
			continue
		}
		if r.ScopeName != scope && !r.anyScope() {
			continue
		}
		if r.CollectionName != collection && !r.anyCollection() {
			continue
		}

		// Resource access looks ok so check the permissions for the role.
		rolePerms, ok := roleToPermissions[r.Name]
		if !ok {
			continue
		}

		for _, rp := range rolePerms {
			if rp == permission {
				// Resources are good and user has this permission so we're good.
				return true
			}
		}
	}

	for _, g := range u.Groups {
		for _, r := range g.Roles {
			// Check that we have access to the resources first.
			if r.BucketName != bucket && !r.anyBucket() {
				continue
			}
			if r.ScopeName != scope && !r.anyScope() {
				continue
			}
			if r.CollectionName != collection && !r.anyCollection() {
				continue
			}

			// Resource access looks ok so check the permissions for the role.
			rolePerms, ok := roleToPermissions[r.Name]
			if !ok {
				continue
			}

			for _, rp := range rolePerms {
				if rp == permission {
					// Resources are good and user has this permission so we're good.
					return true
				}
			}
		}
	}

	return false
}

// UpsertUserOptions allows you to specify initial options for a new user.
type UpsertUserOptions struct {
	Username    string
	DisplayName string
	// Roles are the roles assigned to the user that are of type "user".
	Roles    []string
	Groups   []string
	Password string
}

// Engine represents the high level user management engine.
type Engine struct {
	users  []*User
	groups []*Group
	roles  []*ClusterRole
}

// NewEngine creates a new user management engine.
func NewEngine() *Engine {
	return &Engine{
		roles: []*ClusterRole{
			{Role: "admin"},
			{Role: "ro_admin"},
			{Role: "security_admin"},
			{Role: "cluster_admin"},
			{Role: "bucket_admin"},
			{Role: "scope_admin"},
			{Role: "bucket_full_access"},
			{Role: "views_admin"},
			{Role: "views_reader"},
			{Role: "replication_admin"},
			{Role: "data_reader"},
			{Role: "data_writer"},
			{Role: "data_dcp_reader"},
			{Role: "data_backup"},
			{Role: "data_monitoring"},
			{Role: "fts_admin"},
			{Role: "fts_searcher"},
			{Role: "query_select"},
			{Role: "query_update"},
			{Role: "query_insert"},
			{Role: "query_delete"},
			{Role: "query_manage_index"},
			{Role: "query_system_catalog"},
			{Role: "query_external_access"},
			{Role: "query_manage_global_functions"},
			{Role: "query_execute_global_functions"},
			{Role: "query_manage_functions"},
			{Role: "query_execute_functions"},
			{Role: "query_manage_global_external_functions"},
			{Role: "query_execute_global_external_functions"},
			{Role: "query_manage_external_functions"},
			{Role: "query_execute_external_functions"},
			{Role: "replication_target"},
			{Role: "analytics_manager"},
			{Role: "analytics_reader"},
			{Role: "analytics_select"},
			{Role: "analytics_admin"},
			{Role: "mobile_sync_gateway"},
			{Role: "external_stats_reader"},
		},
	}
}

// UpsertUser creates or updates a user.
func (e *Engine) UpsertUser(opts UpsertUserOptions) error {
	if opts.Username == "" {
		return errors.New("username must be set")
	}

	var roles []*UserRole
	// TODO(chvck): role validation
	for _, role := range opts.Roles {
		r := &UserRole{}
		// Roles can be of the form "rolename" or "rolename[bucketname:<scope>:<collection>]"
		role = strings.TrimSuffix(role, "]")
		if split := strings.Split(role, "["); len(split) == 2 {
			r.Name = split[0]
			scopeSplit := strings.Split(split[1], ":")

			r.BucketName = scopeSplit[0]
			if len(scopeSplit) == 2 {
				r.ScopeName = scopeSplit[1]
			}
			if len(scopeSplit) == 3 {
				r.ScopeName = scopeSplit[1]
				r.CollectionName = scopeSplit[2]
			}
		} else if len(split) > 2 {
			return errors.New("invalid role syntax")
		} else {
			r.Name = role
		}

		roles = append(roles, r)
	}

	var groups []*Group
	for _, group := range opts.Groups {
		var found bool
		for _, g := range e.groups {
			if g.Name == group {
				groups = append(groups, g)
				found = true
				break
			}
		}

		if !found {
			return errors.New("unknown group")
		}
	}

	user := e.GetUser(opts.Username)
	if user == nil {
		user = &User{
			Username: opts.Username,
		}
	}

	user.DisplayName = opts.DisplayName
	user.Groups = groups
	user.Roles = roles
	if opts.Password != "" {
		user.Password = opts.Password
	}

	e.users = append(e.users, user)

	return nil
}

// GetUser retrieves a user by their username.
func (e *Engine) GetUser(username string) *User {
	for _, user := range e.users {
		if user.Username == username {
			return user
		}
	}

	return nil
}

// GetAllUsers returns a list of all registered users.
func (e *Engine) GetAllUsers() []*User {
	return e.users
}

// GetAllClusterRoles returns a list of all known cluster roles.
func (e *Engine) GetAllClusterRoles() []*ClusterRole {
	return e.roles
}

// DropUser deletes a user.
func (e *Engine) DropUser(username string) error {
	var users []*User
	for _, user := range e.users {
		if user.Username == username {
			continue
		}
		users = append(users, user)
	}
	if len(users) == len(e.users) {
		return errors.New("user not found")
	}
	e.users = users

	return nil
}

var roleToPermissions = map[string][]Permission{
	"admin": {PermissionDataRead, PermissionDataWrite, PermissionUserRead, PermissionUserManage, PermissionViewsRead, PermissionViewsManage,
		PermissionDCPRead, PermissionSearchRead, PermissionSearchManage, PermissionQueryRead, PermissionQueryWrite, PermissionQueryDelete,
		PermissionQueryManage, PermissionAnalyticsRead, PermissionsAnalyticsManage, PermissionSyncGateway, PermissionStatsRead,
		PermissionReplicationTarget, PermissionReplicationManage, PermissionClusterRead, PermissionClusterManage, PermissionBucketManage,
		PermissionSelect, PermissionSettings},
	"ro_admin": {PermissionUserRead, PermissionClusterRead},
	"cluster_admin": {PermissionUserRead, PermissionStatsRead, PermissionReplicationTarget, PermissionReplicationManage,
		PermissionClusterRead, PermissionClusterManage},
	"security_admin": {PermissionUserRead, PermissionUserManage, PermissionClusterRead},
	"bucket_admin":   {PermissionReplicationTarget, PermissionReplicationManage, PermissionClusterRead, PermissionBucketManage},
}
