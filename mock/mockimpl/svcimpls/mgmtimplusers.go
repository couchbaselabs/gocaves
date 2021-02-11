package svcimpls

import (
	"bytes"
	"encoding/json"
	"github.com/couchbaselabs/gocaves/contrib/pathparse"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
	"log"
	"strings"
	"time"

	"github.com/couchbaselabs/gocaves/mock"
)

type mgmtImplUser struct {
}

func (x *mgmtImplUser) Register(h *hookHelper) {
	h.RegisterMgmtHandler("PUT", "/settings/rbac/users/*/*", x.handleUpsertUser)
	h.RegisterMgmtHandler("GET", "/settings/rbac/users/*", x.handleGetAllUsers)
	h.RegisterMgmtHandler("GET", "/settings/rbac/users/*/*", x.handleGetUser)
	h.RegisterMgmtHandler("DELETE", "/settings/rbac/users/*/*", x.handleDropUser)
}

func (x *mgmtImplUser) handleUpsertUser(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	if !source.CheckAuthenticated(mockauth.PermissionUserManage, "", "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}
	pathParts := pathparse.ParseParts(req.URL.Path, "/settings/rbac/users/*/*")
	if len(pathParts) != 2 {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte("invalid path")),
		}
	}
	// domainName := pathParts[0]	// TODO(chvck): something with domains
	username := pathParts[1]
	var roles []string
	if req.Form.Get("roles") != "" {
		roles = strings.Split(req.Form.Get("roles"), ",")
	}
	var groups []string
	if req.Form.Get("groups") != "" {
		groups = strings.Split(req.Form.Get("groups"), ",")
	}
	err := source.Node().Cluster().Users().UpsertUser(mockauth.UpsertUserOptions{
		Username:    username,
		DisplayName: req.Form.Get("name"),
		Roles:       roles,
		Groups:      groups,
		Password:    req.Form.Get("password"),
	})
	if err != nil {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte(err.Error())),
		}
	}
	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte("")),
	}
}

func (x *mgmtImplUser) handleGetUser(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	if !source.CheckAuthenticated(mockauth.PermissionUserRead, "", "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}

	pathParts := pathparse.ParseParts(req.URL.Path, "/settings/rbac/users/*/*")
	if len(pathParts) != 2 {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte("invalid path")),
		}
	}
	// domainName := pathParts[0]	// TODO(chvck): something with domains
	username := pathParts[1]

	user := source.Node().Cluster().Users().GetUser(username)
	if user == nil {
		return &mock.HTTPResponse{
			StatusCode: 404,
			Body:       bytes.NewReader([]byte("Unknown user.")),
		}
	}

	b, err := json.Marshal(newJsonUser(user))
	if err != nil {
		return &mock.HTTPResponse{
			StatusCode: 500,
			Body:       bytes.NewReader([]byte(err.Error())),
		}
	}

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader(b),
	}
}

func (x *mgmtImplUser) handleGetAllUsers(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	if !source.CheckAuthenticated(mockauth.PermissionUserRead, "", "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}

	pathParts := pathparse.ParseParts(req.URL.Path, "/settings/rbac/users/*")
	if len(pathParts) != 1 {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte("invalid path")),
		}
	}
	// domainName := pathParts[0]	// TODO(chvck): something with domains

	users := source.Node().Cluster().Users().GetAllUsers()
	jsonUsers := make([]jsonUser, len(users))
	for i, u := range users {
		jsonUsers[i] = newJsonUser(u)
	}

	b, err := json.Marshal(jsonUsers)
	if err != nil {
		return &mock.HTTPResponse{
			StatusCode: 500,
			Body:       bytes.NewReader([]byte(err.Error())),
		}
	}

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader(b),
	}
}

func (x *mgmtImplUser) handleDropUser(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	if !source.CheckAuthenticated(mockauth.PermissionUserManage, "", "", "", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}
	pathParts := pathparse.ParseParts(req.URL.Path, "/settings/rbac/users/*/*")
	if len(pathParts) != 2 {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte("invalid path")),
		}
	}
	// domainName := pathParts[0]	// TODO(chvck): something with domains
	username := pathParts[1]

	if err := source.Node().Cluster().Users().DropUser(username); err != nil {
		log.Printf("USER: %v", err)
		return &mock.HTTPResponse{
			StatusCode: 500,
			Body:       bytes.NewReader([]byte(err.Error())),
		}
	}

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte("")),
	}
}

type jsonOrigin struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type"`
}

type jsonRole struct {
	Role       string       `json:"role"`
	Bucket     string       `json:"bucket_name,omitempty"`
	Scope      string       `json:"scope_name,omitempty"`
	Collection string       `json:"collection_name,omitempty"`
	Origins    []jsonOrigin `json:"origins,omitempty"`
}

type jsonUser struct {
	ID              string     `json:"id"`
	Name            string     `json:"name,omitempty"`
	Roles           []jsonRole `json:"roles"`
	Groups          []string   `json:"groups"`
	Domain          string     `json:"domain,omitempty"`
	ExternalGroups  []string   `json:"external_groups,omitempty"`
	PasswordChanged time.Time  `json:"password_change_date,omitempty"`
}

func newJsonUser(user *mockauth.User) jsonUser {
	groups := []string{} // We have to initialize this way to get [] in the json if there are no groups.
	for _, g := range user.Groups {
		groups = append(groups, g.Name)
	}
	roles := []jsonRole{}
	for _, r := range user.Roles {
		roles = append(roles, jsonRole{
			Role:       r.Name,
			Bucket:     r.BucketName,
			Scope:      r.ScopeName,
			Collection: r.CollectionName,
			Origins: []jsonOrigin{ // TODO(chvck): origins
				{
					Type: "user",
				},
			},
		})
	}
	return jsonUser{
		ID:     user.Username,
		Name:   user.DisplayName,
		Roles:  roles,
		Groups: groups,
		Domain: "local", // TODO(chvck)
		// ExternalGroups:  nil,	// TODO(chvck)
		// PasswordChanged: time.Time{},	// TODO(chvck)
	}
}
