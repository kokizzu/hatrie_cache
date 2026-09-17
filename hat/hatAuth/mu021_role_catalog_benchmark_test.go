package hatAuth

import "testing"

var mu021RoleCatalogBenchmarkSink bool

func BenchmarkMU021AfterRoleCatalogAuthorize(b *testing.B) {
	catalog, err := NewRoleCatalog(RoleCatalogOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := catalog.CreateNamespace("admin", NamespaceSpec{Name: "tenant-eu", Owner: "admin"}); err != nil {
		b.Fatal(err)
	}
	if _, err := catalog.CreateRole("admin", RoleSpec{Name: "reader", Owner: "admin"}); err != nil {
		b.Fatal(err)
	}
	if _, err := catalog.Grant("admin", RoleGrantSpec{Role: "reader", Rule: Rule{
		Commands:   []string{"GET"},
		Namespaces: []string{"tenant-eu"},
		Sources:    []string{"orders"},
	}}); err != nil {
		b.Fatal(err)
	}
	if _, err := catalog.GrantRole("admin", "alice", "reader"); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		mu021RoleCatalogBenchmarkSink = catalog.Authorize("alice", AuthorizationRequest{
			Command:   "GET",
			Namespace: "tenant-eu:orders",
			Source:    "orders",
		})
	}
}
