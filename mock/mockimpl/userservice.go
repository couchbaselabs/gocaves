package mockimpl

import (
	"github.com/couchbaselabs/gocaves/mock/mockauth"
)

// userService represents a user service running somewhere in the cluster.
type userService struct {
	users *mockauth.Engine
}

func newUserService() *userService {
	return &userService{
		users: mockauth.NewEngine(),
	}
}

// AddUser will add a new user to a cluster.
func (c *userService) UpsertUser(opts mockauth.UpsertUserOptions) error {
	return c.users.UpsertUser(mockauth.UpsertUserOptions{
		Username:    opts.Username,
		DisplayName: opts.DisplayName,
		Roles:       opts.Roles,
		Groups:      opts.Groups,
		Password:    opts.Password,
	})
}

// GetUser will return a specific user from the cluster.
func (c *userService) GetUser(username string) *mockauth.User {
	user := c.users.GetUser(username)
	if user == nil {
		return nil // TODO: update
	}

	return user
}

// GetAllUsers will return all of the users from the cluster.
func (c *userService) GetAllUsers() []*mockauth.User {
	users := c.users.GetAllUsers()
	var userInsts []*mockauth.User
	for _, u := range users {
		userInsts = append(userInsts, u)
	}

	return userInsts
}

// DropUser will remove a specific user from the cluster.
func (c *userService) DropUser(username string) error {
	return c.users.DropUser(username)
}
