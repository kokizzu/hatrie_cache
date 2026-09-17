package hatAuth

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestRoleCatalogInheritsRolesAndHierarchicalNamespaces(t *testing.T) {
	catalog, err := NewRoleCatalog(RoleCatalogOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := catalog.CreateNamespace("admin", NamespaceSpec{Name: "tenant-eu", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateNamespace("admin", NamespaceSpec{Name: "tenant-eu/orders", Parent: "tenant-eu", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateRole("admin", RoleSpec{Name: "reader", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateRole("admin", RoleSpec{Name: "analyst", Owner: "admin", Parents: []string{"reader"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Grant("admin", RoleGrantSpec{
		Role: "reader",
		Rule: Rule{Commands: []string{"SELECT"}, Namespaces: []string{"tenant-eu"}, Sources: []string{"orders"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.GrantRole("admin", "alice", "analyst"); err != nil {
		t.Fatal(err)
	}

	if !catalog.Authorize("alice", AuthorizationRequest{Command: "select", Namespace: "tenant-eu/orders", Source: "orders"}) {
		t.Fatal("inherited grant should authorize a child namespace")
	}
	if catalog.Authorize("alice", AuthorizationRequest{Command: "SELECT", Namespace: "tenant-us/orders", Source: "orders"}) {
		t.Fatal("different namespace should be denied")
	}
	if catalog.Authorize("alice", AuthorizationRequest{Command: "SELECT", Namespace: "tenant-eu/orders", Source: "users"}) {
		t.Fatal("different source should be denied")
	}
	if catalog.Authorize("unknown", AuthorizationRequest{Command: "SELECT", Namespace: "tenant-eu/orders", Source: "orders"}) {
		t.Fatal("unassigned principal should be denied")
	}
	if catalog.Authorize("alice\n", AuthorizationRequest{Command: "select", Namespace: "tenant-eu/orders", Source: "orders"}) {
		t.Fatal("control-character principal should be denied")
	}
	if catalog.Authorize("alice", AuthorizationRequest{Command: "select\n", Namespace: "tenant-eu/orders", Source: "orders"}) {
		t.Fatal("control-character request should be denied")
	}
	if catalog.Authorize("alice", AuthorizationRequest{Command: "select\xff", Namespace: "tenant-eu/orders", Source: "orders"}) {
		t.Fatal("invalid UTF-8 request should be denied")
	}
	if !catalog.CanManageRole("admin", "reader") {
		t.Fatal("role owner should manage the role")
	}
	if catalog.CanManageRole("alice", "reader") {
		t.Fatal("role membership must not imply ownership")
	}
	if !catalog.CanManageNamespace("admin", "tenant-eu/orders") {
		t.Fatal("namespace owner should manage descendants")
	}
}

func TestRoleCatalogOwnershipVersionAndLimits(t *testing.T) {
	if _, err := NewRoleCatalog(RoleCatalogOptions{MaxRoles: -1}); !errors.Is(err, ErrRoleCatalogInvalid) {
		t.Fatalf("negative limit error = %v", err)
	}
	catalog, err := NewRoleCatalog(RoleCatalogOptions{MaxRoles: 1, MaxGrants: 1, MaxMemberships: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateRole("intruder", RoleSpec{Name: "reader", Owner: "admin"}); !errors.Is(err, ErrRoleCatalogAccessDenied) {
		t.Fatalf("unexpected unauthorized role creation error = %v", err)
	}
	if _, err := catalog.CreateRole("admin", RoleSpec{Name: "reader", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateNamespace("admin", NamespaceSpec{Name: "tenant*", Owner: "admin"}); !errors.Is(err, ErrRoleCatalogInvalid) {
		t.Fatalf("wildcard namespace error = %v", err)
	}
	if _, err := catalog.CreateRole("admin", RoleSpec{Name: "writer", Owner: "admin"}); !errors.Is(err, ErrRoleCatalogLimit) {
		t.Fatalf("role limit error = %v", err)
	}
	if _, err := catalog.CreateNamespace("admin", NamespaceSpec{Name: "tenant-eu", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateNamespace("admin", NamespaceSpec{Name: "tenant-eu/orders", Parent: "tenant-eu\n", Owner: "admin"}); !errors.Is(err, ErrRoleCatalogInvalid) {
		t.Fatalf("control-character namespace parent error = %v", err)
	}
	grant, err := catalog.Grant("admin", RoleGrantSpec{Role: "reader", Rule: Rule{Commands: []string{"SELECT"}, Namespaces: []string{"tenant-eu"}}})
	if err != nil {
		t.Fatal(err)
	}
	version := catalog.Version()
	if err := catalog.RevokeGrant("admin", grant.ID, version-1); !errors.Is(err, ErrRoleCatalogConflict) {
		t.Fatalf("stale grant version error = %v", err)
	}
	if _, err := catalog.GrantRole("intruder", "alice", "reader"); !errors.Is(err, ErrRoleCatalogAccessDenied) {
		t.Fatalf("unauthorized role grant error = %v", err)
	}
	if _, err := catalog.GrantRole("admin", "alice", "reader"); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateNamespace("admin", NamespaceSpec{Name: "tenant-us", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.GrantRole("admin", "bob", "reader"); !errors.Is(err, ErrRoleCatalogLimit) {
		t.Fatalf("membership limit error = %v", err)
	}
	if err := catalog.RevokeGrant("admin", grant.ID, catalog.Version()); err != nil {
		t.Fatal(err)
	}
	if err := catalog.DeleteRole("admin", "reader", catalog.Version()); !errors.Is(err, ErrRoleCatalogInUse) {
		t.Fatalf("role in-use error = %v", err)
	}
	if err := catalog.RevokeRole("admin", "alice", "reader", catalog.Version()); err != nil {
		t.Fatal(err)
	}
	if err := catalog.DeleteRole("admin", "reader", catalog.Version()); err != nil {
		t.Fatal(err)
	}
}

func TestRoleCatalogSnapshotRestoreAndCycleValidation(t *testing.T) {
	catalog, err := NewRoleCatalog(RoleCatalogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateNamespace("admin", NamespaceSpec{Name: "tenant-eu", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateRole("admin", RoleSpec{Name: "reader", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Grant("admin", RoleGrantSpec{Role: "reader", Rule: Rule{Commands: []string{"GET"}, Namespaces: []string{"tenant-eu"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.GrantRole("admin", "alice", "reader"); err != nil {
		t.Fatal(err)
	}

	if _, err := catalog.CreateNamespace("admin", NamespaceSpec{Name: "tenant-us", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	snapshot := catalog.Snapshot()
	if snapshot.Version == 0 || len(snapshot.Roles) != 1 || len(snapshot.Namespaces) != 2 || len(snapshot.Grants) != 1 || len(snapshot.Memberships) != 1 {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	snapshot.Roles[0].Parents = append(snapshot.Roles[0].Parents, "mutated")
	snapshot.Grants[0].Rule.Commands[0] = "MUTATED"
	cleanSnapshot := catalog.Snapshot()
	if len(cleanSnapshot.Roles[0].Parents) != 0 || cleanSnapshot.Grants[0].Rule.Commands[0] != "GET" {
		t.Fatal("snapshot must be independent")
	}

	restored, err := NewRoleCatalog(RoleCatalogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	original := catalog.Snapshot()
	if err := restored.Restore(original, 0); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, restored.Snapshot()) {
		t.Fatalf("restored snapshot differs:\noriginal=%#v\nrestored=%#v", original, restored.Snapshot())
	}
	if !restored.Authorize("alice", AuthorizationRequest{Command: "GET", Namespace: "tenant-eu/child"}) {
		t.Fatal("restored catalog should preserve authorization")
	}
	if err := restored.Restore(original, restored.Version()-1); !errors.Is(err, ErrRoleCatalogConflict) {
		t.Fatalf("restore conflict error = %v", err)
	}

	cycle := RoleCatalogSnapshot{
		Version: 1,
		Roles: []RoleMetadata{
			{Name: "a", Owner: "admin", Parents: []string{"b"}},
			{Name: "b", Owner: "admin", Parents: []string{"a"}},
		},
	}
	if err := restored.Restore(cycle, restored.Version()); !errors.Is(err, ErrRoleCatalogCycle) {
		t.Fatalf("cycle restore error = %v", err)
	}
}

func TestRoleCatalogConcurrentReadsAndSnapshots(t *testing.T) {
	catalog, err := NewRoleCatalog(RoleCatalogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateNamespace("admin", NamespaceSpec{Name: "tenant-eu", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateRole("admin", RoleSpec{Name: "reader", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Grant("admin", RoleGrantSpec{Role: "reader", Rule: Rule{Commands: []string{"GET"}, Namespaces: []string{"tenant-eu"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.GrantRole("admin", "alice", "reader"); err != nil {
		t.Fatal(err)
	}

	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for iteration := 0; iteration < 1000; iteration++ {
				if !catalog.Authorize("alice", AuthorizationRequest{Command: "GET", Namespace: "tenant-eu/items"}) {
					t.Error("concurrent authorization unexpectedly denied")
					return
				}
				_ = catalog.Snapshot()
			}
		}()
	}
	group.Wait()
}
