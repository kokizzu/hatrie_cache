package hatAuth

import "testing"

var t33AuthorizerBaselineSink bool

func t33BaselinePolicy() Policy {
	return Policy{
		Principals: map[string][]string{"alice": {"reader"}},
		Roles: []Role{{Name: "reader", Rules: []Rule{{
			Commands: []string{"GETSTR"}, Objects: []string{"tenant-eu:orders"},
		}}}},
	}
}

func t33BaselineCatalog(b *testing.B) *RoleCatalog {
	b.Helper()
	catalog, err := NewRoleCatalog(RoleCatalogOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := catalog.CreateRole("admin", RoleSpec{Name: "reader", Owner: "admin"}); err != nil {
		b.Fatal(err)
	}
	if _, err := catalog.CreateNamespace("admin", NamespaceSpec{Name: "tenant-eu", Owner: "admin"}); err != nil {
		b.Fatal(err)
	}
	if _, err := catalog.Grant("admin", RoleGrantSpec{
		Role: "reader",
		Rule: Rule{Commands: []string{"GETSTR"}, Namespaces: []string{"tenant-eu"}, Objects: []string{"tenant-eu:orders"}},
	}); err != nil {
		b.Fatal(err)
	}
	if _, err := catalog.GrantRole("admin", "alice", "reader"); err != nil {
		b.Fatal(err)
	}
	return catalog
}

func BenchmarkT33BeforePolicyAuthorizeObject(b *testing.B) {
	policy := t33BaselinePolicy()
	request := AuthorizationRequest{Command: "GETSTR", Object: "tenant-eu:orders"}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		t33AuthorizerBaselineSink = policy.AuthorizeRequest("alice", request)
	}
}

func BenchmarkT33BeforeRoleCatalogAuthorize(b *testing.B) {
	catalog := t33BaselineCatalog(b)
	request := AuthorizationRequest{Command: "GETSTR", Namespace: "tenant-eu", Object: "tenant-eu:orders"}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		t33AuthorizerBaselineSink = catalog.Authorize("alice", request)
	}
}

func BenchmarkT33AfterRoleCatalogAuthorizer(b *testing.B) {
	catalog := t33BaselineCatalog(b)
	authorizer := Authorizer{RoleCatalog: catalog}
	request := AuthorizationRequest{Command: "GETSTR", Namespace: "tenant-eu", Object: "tenant-eu:orders"}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		t33AuthorizerBaselineSink = authorizer.AuthorizeRequest("alice", request)
	}
}

func BenchmarkT33AfterComposedAuthorizer(b *testing.B) {
	catalog := t33BaselineCatalog(b)
	authorizer := Authorizer{Policy: t33BaselinePolicy(), RoleCatalog: catalog}
	request := AuthorizationRequest{Command: "GETSTR", Namespace: "tenant-eu", Object: "tenant-eu:orders"}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		t33AuthorizerBaselineSink = authorizer.AuthorizeRequest("alice", request)
	}
}
