package hatAuth

import (
	"errors"
	"reflect"
	"testing"
)

func TestRoleCatalogFunctionGrantIsNarrowAndRestorable(t *testing.T) {
	catalog, err := NewRoleCatalog(RoleCatalogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateNamespace("admin", NamespaceSpec{Name: "tenant-eu", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateRole("admin", RoleSpec{Name: "executor", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Grant("admin", RoleGrantSpec{
		Role: "executor",
		Rule: Rule{
			Commands:   []string{"EXECUTE"},
			Namespaces: []string{"tenant-eu"},
			Functions:  []string{"math.*"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.GrantRole("admin", "alice", "executor"); err != nil {
		t.Fatal(err)
	}

	request := AuthorizationRequest{Command: "execute", Namespace: "tenant-eu/orders", Function: "math.sum"}
	if !catalog.Authorize("alice", request) {
		t.Fatal("matching function grant should authorize")
	}
	request.Function = "math.avg"
	if !catalog.Authorize("alice", request) {
		t.Fatal("matching function prefix should authorize")
	}
	for _, function := range []string{"other.sum", ""} {
		request.Function = function
		if catalog.Authorize("alice", request) {
			t.Fatalf("function %q should be denied", function)
		}
	}
	request.Function = "math.sum\n"
	if catalog.Authorize("alice", request) {
		t.Fatal("control-character function should be denied")
	}

	original := catalog.Snapshot()
	if len(original.Grants) != 1 || !reflect.DeepEqual(original.Grants[0].Rule.Functions, []string{"math.*"}) {
		t.Fatalf("function selector missing from snapshot: %#v", original)
	}
	original.Grants[0].Rule.Functions[0] = "mutated"
	if got := catalog.Snapshot().Grants[0].Rule.Functions[0]; got != "math.*" {
		t.Fatalf("snapshot aliased function selector: %q", got)
	}

	restored, err := NewRoleCatalog(RoleCatalogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	clean := catalog.Snapshot()
	if err := restored.Restore(clean, 0); err != nil {
		t.Fatal(err)
	}
	if !restored.Authorize("alice", AuthorizationRequest{Command: "EXECUTE", Namespace: "tenant-eu/orders", Function: "math.sum"}) {
		t.Fatal("restored function grant should authorize")
	}
}

func TestRoleCatalogFunctionGrantValidation(t *testing.T) {
	catalog, err := NewRoleCatalog(RoleCatalogOptions{MaxSelectors: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateRole("admin", RoleSpec{Name: "executor", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	for _, functions := range [][]string{{"math*sum"}, {"\n"}, {"math.sum", "math.avg"}} {
		_, err := catalog.Grant("admin", RoleGrantSpec{Role: "executor", Rule: Rule{Functions: functions}})
		if len(functions) > 1 {
			if !errors.Is(err, ErrRoleCatalogLimit) {
				t.Fatalf("too many function selectors error = %v", err)
			}
			continue
		}
		if !errors.Is(err, ErrRoleCatalogInvalid) {
			t.Fatalf("invalid function selectors %q error = %v", functions, err)
		}
	}

	distinct, err := NewRoleCatalog(RoleCatalogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := distinct.CreateRole("admin", RoleSpec{Name: "executor", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	for _, function := range []string{"math.sum", "math.avg"} {
		if _, err := distinct.Grant("admin", RoleGrantSpec{Role: "executor", Rule: Rule{Functions: []string{function}}}); err != nil {
			t.Fatalf("distinct function %q should be grantable: %v", function, err)
		}
	}
}

func TestLegacyPolicyFunctionGrantCannotBeBypassed(t *testing.T) {
	policy := Policy{
		Principals: map[string][]string{"alice": {"executor"}},
		Roles: []Role{{Name: "executor", Rules: []Rule{{
			Commands:  []string{"EXECUTE"},
			Functions: []string{"math.*"},
		}}}},
	}
	if policy.Authorize("alice", "EXECUTE", "", "") {
		t.Fatal("legacy request without function identity must not bypass function grant")
	}
	if !policy.AuthorizeRequest("alice", AuthorizationRequest{Command: "EXECUTE", Function: "math.sum"}) {
		t.Fatal("matching legacy policy function grant should authorize")
	}
}
