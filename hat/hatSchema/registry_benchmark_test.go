package hatSchema

import "testing"

var mu40BenchmarkReport SchemaCompatibilityReport

func BenchmarkMU40DirectCompatibility(b *testing.B) {
	previous := schemaCompatibilityFixture(1)
	next := previous.Clone()
	next.Version = 2
	source := next.Sources["users"]
	source.Columns = append(source.Columns, Column{Name: "email", Type: TypeText})
	next.Sources["users"] = source
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		report, err := CheckRollingCompatibility(previous, next)
		if err != nil {
			b.Fatal(err)
		}
		mu40BenchmarkReport = report
	}
}

func BenchmarkMU40RegistryCheck(b *testing.B) {
	previous := schemaCompatibilityFixture(1)
	next := previous.Clone()
	next.Version = 2
	source := next.Sources["users"]
	source.Columns = append(source.Columns, Column{Name: "email", Type: TypeText})
	next.Sources["users"] = source
	registry, err := NewSchemaRegistry(SchemaRegistryOptions{Policy: SchemaRegistryPolicyRolling})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := registry.Register("orders", previous); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		report, err := registry.Check("orders", next)
		if err != nil {
			b.Fatal(err)
		}
		mu40BenchmarkReport = report
	}
}
