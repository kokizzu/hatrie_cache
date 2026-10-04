package hatResource

import "testing"

func BenchmarkMG45DirectMapLookupBaseline(b *testing.B) {
	values := map[string]string{"prod/database": "secret-value"}
	var value string
	b.ReportAllocs()
	for b.Loop() {
		value = values["prod/database"]
	}
	if value != "secret-value" {
		b.Fatalf("lookup = %q, want secret-value", value)
	}
}
