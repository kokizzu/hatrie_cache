package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkPluginRegistryResolve(b *testing.B) {
	registry := hatSql.NewPluginRegistry()
	if _, err := registry.Load(registryPlugin{name: "geo", version: "1.0.0"}, ""); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for range b.N {
		if _, ok := registry.Resolve("geo"); !ok {
			b.Fatal("Resolve() did not find plugin")
		}
	}
}

func BenchmarkPluginRegistryReplace(b *testing.B) {
	registry := hatriecacheNewPluginRegistryForBenchmark(b)
	one := registryPlugin{name: "geo", version: "1.0.0"}
	two := registryPlugin{name: "geo", version: "2.0.0"}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		current, next := one, two
		if index%2 != 0 {
			current, next = two, one
		}
		if _, err := registry.Load(next, current.version); err != nil {
			b.Fatal(err)
		}
	}
}

func hatriecacheNewPluginRegistryForBenchmark(b *testing.B) *hatSql.PluginRegistry {
	b.Helper()
	registry := hatSql.NewPluginRegistry()
	if _, err := registry.Load(registryPlugin{name: "geo", version: "1.0.0"}, ""); err != nil {
		b.Fatal(err)
	}
	return registry
}
