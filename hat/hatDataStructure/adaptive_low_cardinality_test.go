package hatDataStructure

import (
	"errors"
	"reflect"
	"testing"
)

func TestAdaptiveLowCardinalityFallsBackWithoutRejectingRows(t *testing.T) {
	builder := NewAdaptiveLowCardinalityStringBuilder(AdaptiveLowCardinalityStringBuilderOptions{
		MaxDistinctValues:  2,
		InitialRowCapacity: 4,
	})
	for _, value := range []string{"alpha", "beta", "gamma"} {
		if err := builder.Append(value); err != nil {
			t.Fatalf("Append(%q) error = %v", value, err)
		}
	}
	if err := builder.AppendNull(); err != nil {
		t.Fatalf("AppendNull() error = %v", err)
	}
	column, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if column.UsesDictionary() {
		t.Fatal("high-cardinality input unexpectedly kept dictionary encoding")
	}
	if column.Len() != 4 || column.Cardinality() != 3 {
		t.Fatalf("column length/cardinality = %d/%d, want 4/3", column.Len(), column.Cardinality())
	}
	for row, want := range []struct {
		value string
		valid bool
	}{{"alpha", true}, {"beta", true}, {"gamma", true}, {"", false}} {
		value, valid := column.ValueAt(row)
		if value != want.value || valid != want.valid {
			t.Fatalf("ValueAt(%d) = %q/%t, want %q/%t", row, value, valid, want.value, want.valid)
		}
	}
	if !column.Contains("gamma") || column.Contains("missing") {
		t.Fatal("fallback Contains() result mismatch")
	}
	if _, ok := column.LookupCode("gamma"); ok {
		t.Fatal("fallback LookupCode() unexpectedly returned a dictionary code")
	}
	if column.CodeWidth() != 0 {
		t.Fatalf("fallback CodeWidth() = %d, want 0", column.CodeWidth())
	}
	if column.MemoryBytes() <= 0 {
		t.Fatal("fallback MemoryBytes() was not positive")
	}
	if err := builder.Append("after-build"); !errors.Is(err, ErrLowCardinalityStringBuilderBuilt) {
		t.Fatalf("Append() after Build() error = %v, want ErrLowCardinalityStringBuilderBuilt", err)
	}
}

func TestAdaptiveLowCardinalityKeepsDictionaryForRepeatedValues(t *testing.T) {
	builder := NewAdaptiveLowCardinalityStringBuilder(AdaptiveLowCardinalityStringBuilderOptions{
		MaxDistinctValues:     8,
		MaxDistinctRatio:      0.5,
		MinRowsBeforeFallback: 4,
	})
	for _, value := range []string{"beta", "alpha", "beta", "alpha", "beta", "alpha"} {
		if err := builder.Append(value); err != nil {
			t.Fatalf("Append(%q) error = %v", value, err)
		}
	}
	column, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if !column.UsesDictionary() {
		t.Fatal("repeated low-cardinality input unexpectedly fell back")
	}
	if got, want := column.DictionaryValues(), []string{"alpha", "beta"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("DictionaryValues() = %#v, want %#v", got, want)
	}
	if code, ok := column.LookupCode("alpha"); !ok || code != 0 {
		t.Fatalf("LookupCode(alpha) = %d/%t, want 0/true", code, ok)
	}
	if !column.Contains("beta") {
		t.Fatal("dictionary Contains(beta) = false, want true")
	}
}

func TestAdaptiveLowCardinalityFallsBackOnDistinctRatio(t *testing.T) {
	builder := NewAdaptiveLowCardinalityStringBuilder(AdaptiveLowCardinalityStringBuilderOptions{
		MaxDistinctValues:     128,
		MaxDistinctRatio:      0.5,
		MinRowsBeforeFallback: 4,
	})
	for _, value := range []string{"a", "b", "c", "d"} {
		if err := builder.Append(value); err != nil {
			t.Fatalf("Append(%q) error = %v", value, err)
		}
	}
	column, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if column.UsesDictionary() {
		t.Fatal("high distinct-ratio input unexpectedly kept dictionary encoding")
	}
}

func TestAdaptiveLowCardinalitySamplingPreservesNullValidity(t *testing.T) {
	builder := NewAdaptiveLowCardinalityStringBuilder(AdaptiveLowCardinalityStringBuilderOptions{
		MaxDistinctValues:     8,
		MaxDistinctRatio:      0.5,
		MinRowsBeforeFallback: 4,
	})
	if err := builder.Append("alpha"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AppendNull(); err != nil {
		t.Fatal(err)
	}
	if err := builder.Append("alpha"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AppendNull(); err != nil {
		t.Fatal(err)
	}
	column, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if !column.UsesDictionary() {
		t.Fatal("repeated sampled values unexpectedly fell back")
	}
	for row, wantValid := range []bool{true, false, true, false} {
		_, valid := column.ValueAt(row)
		if valid != wantValid {
			t.Fatalf("ValueAt(%d) validity = %t, want %t", row, valid, wantValid)
		}
	}
}

func TestAdaptiveLowCardinalityNilBuilderIsSafe(t *testing.T) {
	var builder *AdaptiveLowCardinalityStringBuilder
	if err := builder.Append("value"); !errors.Is(err, ErrLowCardinalityStringBuilderBuilt) {
		t.Fatalf("nil Append() error = %v, want ErrLowCardinalityStringBuilderBuilt", err)
	}
	if _, err := builder.Build(); !errors.Is(err, ErrLowCardinalityStringBuilderBuilt) {
		t.Fatalf("nil Build() error = %v, want ErrLowCardinalityStringBuilderBuilt", err)
	}
}

func BenchmarkCHU16AdaptiveDictionaryBuild256(b *testing.B) {
	const size = 10_000
	rows := chu16BaselineRows(size, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		builder := NewAdaptiveLowCardinalityStringBuilder(AdaptiveLowCardinalityStringBuilderOptions{
			InitialCapacity:    256,
			InitialRowCapacity: size,
			MaxDistinctValues:  4096,
		})
		for _, row := range rows {
			if err := builder.Append(row); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := builder.Build(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU16AdaptiveDictionaryBuild4096(b *testing.B) {
	const size = 10_000
	rows := chu16BaselineRows(size, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		builder := NewAdaptiveLowCardinalityStringBuilder(AdaptiveLowCardinalityStringBuilderOptions{
			InitialCapacity:    4096,
			InitialRowCapacity: size,
			MaxDistinctValues:  4096,
		})
		for _, row := range rows {
			if err := builder.Append(row); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := builder.Build(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU16AdaptiveFallbackBuild10000(b *testing.B) {
	const size = 10_000
	rows := chu16BaselineRows(size, size)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		builder := NewAdaptiveLowCardinalityStringBuilder(AdaptiveLowCardinalityStringBuilderOptions{
			InitialCapacity:       64,
			InitialRowCapacity:    size,
			MaxDistinctValues:     4096,
			MaxDistinctRatio:      0.5,
			MinRowsBeforeFallback: 64,
		})
		for _, row := range rows {
			if err := builder.Append(row); err != nil {
				b.Fatal(err)
			}
		}
		column, err := builder.Build()
		if err != nil || column.UsesDictionary() {
			b.Fatalf("Build() = dictionary=%t err=%v, want raw fallback", column.UsesDictionary(), err)
		}
	}
}
