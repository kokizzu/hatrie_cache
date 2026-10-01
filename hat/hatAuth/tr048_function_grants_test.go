package hatAuth

import (
	"errors"
	"testing"
)

func TestPolicyAuthorizesFunctionScopedRole(t *testing.T) {
	policy := Policy{
		Principals: map[string][]string{"operator": {"maintenance"}},
		Roles: []Role{{Name: "maintenance", Rules: []Rule{{
			Functions:  []string{"orders.refresh", "orders.rebuild:*"},
			Namespaces: []string{"tenant-eu:*"},
		}}}},
	}

	if !policy.AuthorizeFunction("operator", "orders.refresh", "tenant-eu:orders") {
		t.Fatal("expected exact function grant allowed")
	}
	if !policy.AuthorizeFunction("operator", "orders.rebuild:daily", "tenant-eu:orders") {
		t.Fatal("expected function prefix grant allowed")
	}
	if policy.AuthorizeFunction("operator", "orders.delete", "tenant-eu:orders") {
		t.Fatal("unexpected function grant for unrelated function")
	}
	if policy.Authorize("operator", "CALL", "tenant-eu:orders", "") {
		t.Fatal("legacy authorization must not bypass a function-constrained rule")
	}
}

func TestRoleCatalogAuthorizesFunctionGrantAndRejectsMalformedRequest(t *testing.T) {
	catalog, err := NewRoleCatalog(RoleCatalogOptions{})
	if err != nil {
		t.Fatalf("NewRoleCatalog() error = %v", err)
	}
	if _, err := catalog.CreateRole("admin", RoleSpec{Name: "maintenance", Owner: "admin"}); err != nil {
		t.Fatalf("CreateRole() error = %v", err)
	}
	if _, err := catalog.CreateNamespace("admin", NamespaceSpec{Name: "tenant-eu", Owner: "admin"}); err != nil {
		t.Fatalf("CreateNamespace() error = %v", err)
	}
	if _, err := catalog.Grant("admin", RoleGrantSpec{
		Role: "maintenance",
		Rule: Rule{Functions: []string{"orders.refresh"}, Namespaces: []string{"tenant-eu"}},
	}); err != nil {
		t.Fatalf("Grant() error = %v", err)
	}
	if _, err := catalog.GrantRole("admin", "operator", "maintenance"); err != nil {
		t.Fatalf("GrantRole() error = %v", err)
	}
	if !catalog.Authorize("operator", AuthorizationRequest{Function: "orders.refresh", Namespace: "tenant-eu/orders"}) {
		t.Fatal("expected function grant allowed")
	}
	if !catalog.AuthorizeFunction("operator", "orders.refresh", "tenant-eu/orders") {
		t.Fatal("AuthorizeFunction() should allow the function grant")
	}
	snapshot := catalog.Snapshot()
	if len(snapshot.Grants) != 1 || len(snapshot.Grants[0].Rule.Functions) != 1 || snapshot.Grants[0].Rule.Functions[0] != "orders.refresh" {
		t.Fatalf("snapshot function grant = %#v, want orders.refresh", snapshot.Grants)
	}
	if catalog.Authorize("operator", AuthorizationRequest{Function: "orders.drop", Namespace: "tenant-eu/orders"}) {
		t.Fatal("unexpected unrelated function grant allowed")
	}
	if catalog.Authorize("operator\n", AuthorizationRequest{Function: "orders.refresh", Namespace: "tenant-eu/orders"}) {
		t.Fatal("control-character principal must be denied")
	}

	bad := Rule{Functions: []string{"orders.refresh\n"}}
	if _, err := catalog.Grant("admin", RoleGrantSpec{Role: "maintenance", Rule: bad}); !errors.Is(err, ErrRoleCatalogInvalid) {
		t.Fatalf("malformed function grant error = %v, want %v", err, ErrRoleCatalogInvalid)
	}
}

var tr048FunctionBenchmarkSink bool

func BenchmarkPolicyAuthorizeFunction(b *testing.B) {
	policy := Policy{
		Principals: map[string][]string{"operator": {"maintenance"}},
		Roles: []Role{{Name: "maintenance", Rules: []Rule{{
			Functions:  []string{"orders.refresh"},
			Namespaces: []string{"tenant-eu:*"},
		}}}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tr048FunctionBenchmarkSink = policy.AuthorizeFunction("operator", "orders.refresh", "tenant-eu:orders")
	}
}
