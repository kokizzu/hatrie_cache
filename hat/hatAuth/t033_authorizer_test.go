package hatAuth

import "testing"

func TestT33AuthorizerComposesLegacyPolicyAndRoleCatalog(t *testing.T) {
	catalog, err := NewRoleCatalog(RoleCatalogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateRole("admin", RoleSpec{Name: "reader", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateNamespace("admin", NamespaceSpec{Name: "tenant-eu", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Grant("admin", RoleGrantSpec{
		Role: "reader",
		Rule: Rule{
			Commands:   []string{"GETSTR"},
			Namespaces: []string{"tenant-eu"},
			Objects:    []string{"tenant-eu:orders"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.GrantRole("admin", "alice", "reader"); err != nil {
		t.Fatal(err)
	}

	authorizer := Authorizer{RoleCatalog: catalog}
	request := AuthorizationRequest{
		Command:   "GETSTR",
		Namespace: "tenant-eu",
		Object:    "tenant-eu:orders",
	}
	if !authorizer.AuthorizeRequest("alice", request) {
		t.Fatal("catalog-only request should be authorized")
	}
	if authorizer.AuthorizeRequest("bob", request) {
		t.Fatal("unassigned principal should be denied")
	}
	if authorizer.AuthorizeRequest("alice", AuthorizationRequest{
		Command:   "SETSTR",
		Namespace: "tenant-eu",
		Object:    "tenant-eu:orders",
	}) {
		t.Fatal("catalog command mismatch should be denied")
	}

	authorizer.Policy = Policy{
		Principals: map[string][]string{"alice": {"legacy-reader"}},
		Roles: []Role{{Name: "legacy-reader", Rules: []Rule{{
			Commands: []string{"GETSTR"}, Objects: []string{"tenant-eu:orders"},
		}}}},
	}
	if !authorizer.AuthorizeRequest("alice", request) {
		t.Fatal("both matching policy sources should authorize")
	}
	authorizer.Policy.Roles[0].Rules[0].Commands = []string{"SETSTR"}
	if authorizer.AuthorizeRequest("alice", request) {
		t.Fatal("legacy policy mismatch must not bypass catalog authorization")
	}
}
