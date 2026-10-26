package mockauth

import (
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type testResources struct{}

func (testResources) HasBucket(bucket string) bool {
	return bucket == "default" || bucket == "other"
}

func (testResources) HasScope(bucket, scope string) bool {
	return bucket == "default" && (scope == "_default" || scope == "s" || scope == "t") ||
		bucket == "other" && scope == "_default"
}

func (testResources) HasCollection(bucket, scope, collection string) bool {
	return bucket == "default" && map[string]bool{"_default._default": true, "s.c0": true, "s.c1": true, "t.d": true}[scope+"."+collection] ||
		bucket == "other" && scope == "_default" && collection == "_default"
}

// String formats the privileges as "P1,P2 child{...}", sorted.
func (p *Privileges) String() string {
	if p == nil {
		return "-"
	}
	var privileges []string
	for privilege := range p.privileges {
		privileges = append(privileges, privilege)
	}
	sort.Strings(privileges)
	out := []string{strings.Join(privileges, ",")}
	var keys []string
	for key := range p.children {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		out = append(out, key+"{"+p.children[key].String()+"}")
	}
	return strings.Join(out, " ")
}

// TestBucketPrivileges compares the privileges of users with different roles
// with the memcached.rbac that ns_server 7.0.5 writes for them, on bucket
// "default" with scopes s (c0, c1) and t (d), and bucket "other".
func TestBucketPrivileges(t *testing.T) {
	const all = "DcpConsumer,DcpProducer,DcpStream,Delete,Insert,MetaRead,MetaWrite,Read,SimpleStats," +
		"SystemXattrRead,SystemXattrWrite,Upsert,XattrRead,XattrWrite"
	const readAll = "DcpProducer,DcpStream,MetaRead,Read,SimpleStats,SystemXattrRead,XattrRead"
	tests := []struct {
		roles         []string
		bucket, other string
	}{
		{roles: []string{"admin"}, bucket: all, other: all},
		{roles: []string{"data_reader[default:s:c0]"}, bucket: " s{ c0{MetaRead,Read,XattrRead}}", other: "-"},
		{roles: []string{"bucket_admin[default]"}, bucket: "SimpleStats", other: "-"},
		{roles: []string{"ro_admin"}, bucket: "SimpleStats", other: "SimpleStats"},
		{roles: []string{"cluster_admin"}, bucket: "SimpleStats", other: "SimpleStats"},
		{roles: []string{"data_writer[default:s]"}, bucket: " s{Delete,Insert,Upsert,XattrWrite}", other: "-"},
		{roles: []string{"views_reader[default]"}, bucket: "Read", other: "-"},
		{roles: []string{"query_select[default:t:d]"}, bucket: " t{ d{Read}}", other: "-"},
		{roles: []string{"fts_searcher[default:s:c1]"}, bucket: "-", other: "-"},
		{roles: []string{"replication_admin"}, bucket: readAll, other: readAll},
		{roles: []string{"views_admin[default]"}, bucket: readAll, other: "-"},
		{roles: []string{"data_monitoring[default:s]"}, bucket: " s{SimpleStats}", other: "-"},
		{roles: []string{"data_monitoring[default:s:c1]"}, bucket: " s{ c1{SimpleStats}}", other: "-"},
		{roles: []string{"analytics_reader"}, bucket: "-", other: "-"},
		{roles: []string{"data_reader[default:s:c0]", "data_writer[default:t]"},
			bucket: " s{ c0{MetaRead,Read,XattrRead}} t{Delete,Insert,Upsert,XattrWrite}", other: "-"},
	}
	for _, tt := range tests {
		t.Run(strings.Join(tt.roles, ","), func(t *testing.T) {
			engine := NewEngine()
			if err := engine.UpsertUser(UpsertUserOptions{Username: "user", Password: "password", Roles: tt.roles}); err != nil {
				t.Fatalf("failed to add user: %v", err)
			}
			user := engine.GetUser("user")
			if got := user.BucketPrivileges(testResources{}, "default").String(); got != tt.bucket {
				t.Errorf("default: expected %q, got %q", tt.bucket, got)
			}
			if got := user.BucketPrivileges(testResources{}, "other").String(); got != tt.other {
				t.Errorf("other: expected %q, got %q", tt.other, got)
			}
		})
	}
}

type dottedResources struct{}

func (dottedResources) HasBucket(bucket string) bool { return bucket == "my.bucket" }

func (dottedResources) HasScope(bucket, scope string) bool {
	return bucket == "my.bucket" && scope == "_default"
}

func (dottedResources) HasCollection(bucket, scope, collection string) bool {
	return bucket == "my.bucket" && scope == "_default" && collection == "_default"
}

// TestBucketPrivilegesDottedBucket compares the privileges on a bucket whose
// name has a dot with the memcached.rbac ns_server 7.0.5 writes.
func TestBucketPrivilegesDottedBucket(t *testing.T) {
	tests := []struct {
		role, privileges string
	}{
		{"bucket_full_access[my.bucket]", "DcpConsumer,DcpProducer,DcpStream,Delete,Insert,MetaRead,MetaWrite,Read," +
			"SimpleStats,SystemXattrRead,SystemXattrWrite,Upsert,XattrRead,XattrWrite"},
		{"data_reader[my.bucket]", "MetaRead,Read,XattrRead"},
	}
	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			engine := NewEngine()
			engine.SetResources(dottedResources{})
			if err := engine.UpsertUser(UpsertUserOptions{Username: "user", Password: "password", Roles: []string{tt.role}}); err != nil {
				t.Fatalf("failed to add user: %v", err)
			}
			if got := engine.GetUser("user").BucketPrivileges(dottedResources{}, "my.bucket").String(); got != tt.privileges {
				t.Fatalf("expected %q, got %q", tt.privileges, got)
			}
		})
	}
}

// TestDropRolesOnGroups checks that dropping a collection removes the roles
// on it from every group, whether a user is in it or not.
func TestDropRolesOnGroups(t *testing.T) {
	engine := NewEngine()
	role := func(raw string) *UserRole {
		r, ok := parseRole(raw)
		if !ok {
			t.Fatalf("failed to parse %s", raw)
		}
		return r
	}
	engine.groups = []*Group{
		{Name: "a", Roles: []*UserRole{role("data_reader[default:s:c0]"), role("data_reader[default:s:c1]")}},
		{Name: "b", Roles: []*UserRole{role("data_reader[default:s:c0]"), role("data_writer[default:s]")}},
	}
	if err := engine.UpsertUser(UpsertUserOptions{Username: "user", Password: "password", Groups: []string{"a"}}); err != nil {
		t.Fatalf("failed to add user: %v", err)
	}
	engine.DropRolesOn("default", "s", "c0", func() bool { return true })
	for _, group := range engine.groups {
		for _, r := range group.Roles {
			if r.String() == "data_reader[default:s:c0]" {
				t.Fatalf("group %s keeps %s", group.Name, r)
			}
		}
	}
	if len(engine.groups[0].Roles) != 1 || len(engine.groups[1].Roles) != 1 {
		t.Fatalf("expected one role left in each group, got %d and %d", len(engine.groups[0].Roles), len(engine.groups[1].Roles))
	}
}

// TestUpsertUserDroppedGroup checks that a user is stored with a group that
// no longer exists, as menelaus_users:store_user/5, which does not check the
// groups, stores it when the group is dropped after the request is
// validated. The user gets the groups that exist.
func TestUpsertUserDroppedGroup(t *testing.T) {
	engine := NewEngine()
	if err := engine.UpsertGroup(UpsertGroupOptions{Name: "a", Roles: []string{"ro_admin"}}); err != nil {
		t.Fatalf("failed to add group: %v", err)
	}
	if err := engine.UpsertUser(UpsertUserOptions{Username: "user", Password: "password", Groups: []string{"gone", "a"}}); err != nil {
		t.Fatalf("failed to add user: %v", err)
	}
	if info := engine.GetUser("user").Info(); len(info.Groups) != 1 || info.Groups[0] != "a" {
		t.Fatalf("expected the user in group a, got %v", info.Groups)
	}
}

// scopeResources has bucket "default" with scope "s" while exists is set.
type scopeResources struct {
	exists *atomic.Bool
}

func (r scopeResources) HasBucket(bucket string) bool { return bucket == "default" }

func (r scopeResources) HasScope(bucket, scope string) bool {
	return bucket == "default" && (scope == "_default" || scope == "s" && r.exists.Load())
}

func (r scopeResources) HasCollection(bucket, scope, collection string) bool { return false }

// TestDropRolesOnRecreatedScope grants a role on a scope created again
// while the scope of the same name is dropped: the grant waits for the drop
// and the removal of the roles on the dropped scope, and is kept.
func TestDropRolesOnRecreatedScope(t *testing.T) {
	exists := &atomic.Bool{}
	exists.Store(true)
	engine := NewEngine()
	engine.SetResources(scopeResources{exists})
	if err := engine.UpsertUser(UpsertUserOptions{Username: "user", Password: "password", Roles: []string{"data_reader[default:s]"}}); err != nil {
		t.Fatalf("failed to add user: %v", err)
	}

	dropped := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		engine.DropRolesOn("default", "s", "", func() bool {
			exists.Store(false)
			close(dropped)
			<-release
			return true
		})
	}()
	<-dropped
	exists.Store(true)
	granted := make(chan error, 1)
	go func() {
		granted <- engine.UpsertUser(UpsertUserOptions{Username: "user", Roles: []string{"data_reader[default:s]"}})
	}()
	// The grant has the time to complete, unless the drop holds it back.
	select {
	case err := <-granted:
		granted <- err
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-done
	if err := <-granted; err != nil {
		t.Fatalf("failed to grant the role: %v", err)
	}
	if roles := engine.GetUser("user").Info().AllRoles; len(roles) != 1 || roles[0].String() != "data_reader[default:s]" {
		t.Fatalf("expected the role on the new scope, got %v", roles)
	}
}
