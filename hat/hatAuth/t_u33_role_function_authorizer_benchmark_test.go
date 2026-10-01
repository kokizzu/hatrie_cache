package hatAuth

import "testing"

func newTU33BenchmarkCatalog(b *testing.B) *RoleCatalog {
	b.Helper()
	catalog, err := NewRoleCatalog(RoleCatalogOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := catalog.CreateRole("owner", RoleSpec{Name: "operators", Owner: "owner"}); err != nil {
		b.Fatal(err)
	}
	if _, err := catalog.GrantRole("owner", "alice", "operators"); err != nil {
		b.Fatal(err)
	}
	if _, err := catalog.Grant("owner", RoleGrantSpec{
		Role: "operators",
		Rule: Rule{Commands: []string{StoredFunctionOperationCall}, Objects: []string{"function:echo"}},
	}); err != nil {
		b.Fatal(err)
	}
	return catalog
}

func BenchmarkTU33RoleCatalogAuthorizeBaseline(b *testing.B) {
	catalog := newTU33BenchmarkCatalog(b)
	request := AuthorizationRequest{Command: StoredFunctionOperationCall, Object: "function:echo"}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !catalog.Authorize("alice", request) {
			b.Fatal("catalog authorization denied benchmark request")
		}
	}
}

func BenchmarkTU33RoleCatalogStoredFunctionAuthorizer(b *testing.B) {
	catalog := newTU33BenchmarkCatalog(b)
	authorizer := RoleCatalogStoredFunctionAuthorizer(catalog)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !authorizer("alice", StoredFunctionOperationCall, "echo") {
			b.Fatal("stored-function authorization denied benchmark request")
		}
	}
}
