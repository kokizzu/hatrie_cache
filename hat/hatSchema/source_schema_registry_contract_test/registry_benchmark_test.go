package source_schema_registry_contract_test

import (
	"testing"

	"hatrie_cache/hat/hatSchema"
)

func BenchmarkSourceSchemaRegistryHotPath(b *testing.B) {
	registry, err := hatSchema.NewSourceSchemaRegistry(hatSchema.SourceSchemaRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	version, err := registry.Register(benchmarkSource(false), 1)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := registry.Validate("orders", version.Version, version.Fingerprint); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSourceSchemaRegistryRegister(b *testing.B) {
	source := benchmarkSource(false)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		registry, err := hatSchema.NewSourceSchemaRegistry(hatSchema.SourceSchemaRegistryOptions{
			MaxVersionsPerSource: 1,
		})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := registry.Register(source, 1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSourceSchemaRegistrySnapshot64(b *testing.B) {
	registry, err := hatSchema.NewSourceSchemaRegistry(hatSchema.SourceSchemaRegistryOptions{
		MaxVersionsPerSource: 64,
		Compatibility:        hatSchema.SourceSchemaCompatibilityAny,
	})
	if err != nil {
		b.Fatal(err)
	}
	source := benchmarkSource(false)
	for version := uint64(1); version <= 64; version++ {
		if _, err := registry.Register(source, version); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if got := registry.Snapshot(); len(got) != 64 {
			b.Fatalf("snapshot length = %d", len(got))
		}
	}
}
