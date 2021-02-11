package mockauth

import (
	"errors"
	"strings"
)

type Role struct {
	Name           string
	BucketName     string
	ScopeName      string
	CollectionName string
}

func (r *Role) anyBucket() bool {
	return r.BucketName == "" || r.BucketName == "*"
}

func (r *Role) anyScope() bool {
	return r.ScopeName == "" || r.ScopeName == "*"
}

func (r *Role) anyCollection() bool {
	return r.CollectionName == "" || r.CollectionName == "*"
}

type Group struct {
	Name  string
	Roles []*Role
}

type User struct {
	DisplayName string
	Username    string
	Password    string
	Groups      []*Group
	Roles       []*Role
}

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

type Engine struct {
	users  []*User
	groups []*Group
	roles  []string
}

func NewEngine() *Engine {
	return &Engine{
		roles: []string{
			"admin", "ro_admin", "security_admin", "cluster_admin", "bucket_admin", "scope_admin", "bucket_full_access",
			"views_admin", "views_reader", "replication_admin", "data_reader", "data_writer", "data_dcp_reader",
			"data_backup", "data_monitoring", "fts_admin", "fts_searcher", "query_select", "query_update",
			"query_insert", "query_delete", "query_manage_index", "query_system_catalog", "query_external_access",
			"query_manage_global_functions", "query_execute_global_functions", "query_manage_functions",
			"query_execute_functions", "query_manage_global_external_functions", "query_execute_global_external_functions",
			"query_manage_external_functions", "query_execute_external_functions", "replication_target", "analytics_manager",
			"analytics_reader", "analytics_select", "analytics_admin", "mobile_sync_gateway", "external_stats_reader",
		},
	}
}

func (e *Engine) UpsertUser(opts UpsertUserOptions) error {
	if opts.Username == "" {
		return errors.New("username must be set")
	}

	var roles []*Role
	// TODO(chvck): role validation
	for _, role := range opts.Roles {
		r := &Role{}
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

func (e *Engine) GetUser(username string) *User {
	for _, user := range e.users {
		if user.Username == username {
			return user
		}
	}

	return nil
}

func (e *Engine) GetAllUsers() []*User {
	return e.users
}

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
