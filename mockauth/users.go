package mockauth

type Role struct {
	Name           string
	BucketName     string
	ScopeName      string
	CollectionName string
}

type Group struct {
	Roles []*Role
}

type User struct {
	DisplayName string
	Username    string
	Password    string
	Groups      []*Group
	Roles       []*Role
}

type Engine struct {
	Groups []*Group
	Users  []*User
}
