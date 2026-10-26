package mock

import "github.com/couchbaselabs/gocaves/mock/mockauth"

// UserManager represents information about the users of the cluster.
type UserManager interface {
	// AddUser will add a new user to a cluster.
	UpsertUser(opts mockauth.UpsertUserOptions) error

	// SetAdmin sets the credentials of the administrator, who is not one of
	// the users.
	SetAdmin(username, password string)

	// Authenticate returns the user credentials authenticate as over REST,
	// or nil.
	Authenticate(username, password string) *mockauth.User

	// GetUser will return a specific user from the cluster.
	GetUser(username string) *mockauth.User

	// GetAllUsers will return all of the users from the cluster.
	GetAllUsers() []*mockauth.User

	// DropUser will remove a specific user from the cluster.
	DropUser(username string) error

	// GetUserInDomain will return a user of a domain, "local" or "external".
	GetUserInDomain(username, domain string) *mockauth.User

	// DropUserInDomain will remove a user of a domain.
	DropUserInDomain(username, domain string) error

	// ValidateRoles refuses roles ns_server would not assign.
	ValidateRoles(roles []string) error

	// UpsertGroup will add or update a group.
	UpsertGroup(opts mockauth.UpsertGroupOptions) error

	// GetGroup will return a group.
	GetGroup(name string) *mockauth.Group

	// GetAllGroups will return all groups.
	GetAllGroups() []*mockauth.Group

	// MissingGroups returns the names that name no group.
	MissingGroups(names []string) []string

	// DropGroup will remove a group.
	DropGroup(name string) error

	// DropRolesOn runs drop, which drops a bucket, scope or collection, and
	// when it succeeds removes the roles on it.
	DropRolesOn(bucket, scope, collection string, drop func() bool)

	// GetAllClusterRoles will return all roles from the cluster.
	GetAllClusterRoles() []*mockauth.ClusterRole

	// RolesByPermission returns the roles, with their parameters, that allow
	// permission, or all roles when it is nil. It fails on parameters
	// ns_server fails on.
	RolesByPermission(permission *mockauth.RBACPermission) ([]*mockauth.UserRole, error)
}
