package hatAuth

import "testing"

type mu020BeforeConnection struct {
	owner    string
	readers  []string
	endpoint string
}

func BenchmarkMU020BeforeConnectionLookup(b *testing.B) {
	connections := map[string]mu020BeforeConnection{
		"orders": {owner: "service-orders", readers: []string{"reporting"}, endpoint: "db.internal:5432"},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		connection, found := connections["orders"]
		if !found || (connection.owner != "service-orders" && connection.owner != "reporting") {
			b.Fatal("connection lookup failed")
		}
	}
}

func BenchmarkMU020AfterConnectionLookup(b *testing.B) {
	registry := newMU020BenchmarkRegistry(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		connection, err := registry.Connection("orders", "reporting")
		if err != nil || connection.Endpoint != "db.internal:5432" {
			b.Fatalf("Connection() = %#v/%v", connection, err)
		}
	}
}

func BenchmarkMU020AfterConnectionResolve(b *testing.B) {
	registry := newMU020BenchmarkRegistry(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		connection, err := registry.ResolveConnection("orders", "reporting")
		if err != nil || len(connection.secret) != len("secret-v1") {
			b.Fatalf("ResolveConnection() = %v", err)
		}
	}
}

func newMU020BenchmarkRegistry(b *testing.B) *ResourceRegistry {
	b.Helper()
	registry, err := NewResourceRegistry(ResourceRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := registry.CreateSecret(SecretSpec{Name: "db/password", Owner: "service-orders", Readers: []string{"reporting"}, Value: []byte("secret-v1")}); err != nil {
		b.Fatal(err)
	}
	if _, err := registry.CreateConnection(ConnectionSpec{Name: "orders", Owner: "service-orders", Readers: []string{"reporting"}, Driver: "postgres", Endpoint: "db.internal:5432", SecretName: "db/password"}); err != nil {
		b.Fatal(err)
	}
	return registry
}
