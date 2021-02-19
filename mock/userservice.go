package mock

import "github.com/couchbaselabs/gocaves/mock/mockauth"

// UserManager represents information about the users of the cluster.
type UserManager interface {
	// AddUser will add a new user to a cluster.
	UpsertUser(opts mockauth.UpsertUserOptions) error

	// GetUser will return a specific user from the cluster.
	GetUser(username string) *mockauth.User

	// GetAllUsers will return all of the users from the cluster.
	GetAllUsers() []*mockauth.User

	// DropUser will remove a specific user from the cluster.
	DropUser(username string) error

	// GetAllClusterRoles will return all roles from the cluster.
	GetAllClusterRoles() []*mockauth.ClusterRole
}
