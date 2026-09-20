package hatAuth

import "testing"

var tu33FunctionGrantBenchmarkSink bool

func newTU33FunctionGrantBenchmarkCatalog(b *testing.B, functions []string) *RoleCatalog {
	b.Helper()
	catalog, err := NewRoleCatalog(RoleCatalogOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := catalog.CreateNamespace("admin", NamespaceSpec{Name: "tenant-eu", Owner: "admin"}); err != nil {
		b.Fatal(err)
	}
	if _, err := catalog.CreateRole("admin", RoleSpec{Name: "executor", Owner: "admin"}); err != nil {
		b.Fatal(err)
	}
	if _, err := catalog.Grant("admin", RoleGrantSpec{Role: "executor", Rule: Rule{
		Commands:   []string{"EXECUTE"},
		Namespaces: []string{"tenant-eu"},
		Functions:  functions,
	}}); err != nil {
		b.Fatal(err)
	}
	if _, err := catalog.GrantRole("admin", "alice", "executor"); err != nil {
		b.Fatal(err)
	}
	return catalog
}

func BenchmarkTU33NamespaceGrantAuthorize(b *testing.B) {
	catalog := newTU33FunctionGrantBenchmarkCatalog(b, nil)
	request := AuthorizationRequest{Command: "EXECUTE", Namespace: "tenant-eu/jobs"}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tu33FunctionGrantBenchmarkSink = catalog.Authorize("alice", request)
	}
}

func BenchmarkTU33FunctionGrantAuthorize(b *testing.B) {
	catalog := newTU33FunctionGrantBenchmarkCatalog(b, []string{"math.*"})
	request := AuthorizationRequest{Command: "EXECUTE", Namespace: "tenant-eu/jobs", Function: "math.sum"}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tu33FunctionGrantBenchmarkSink = catalog.Authorize("alice", request)
	}
}
