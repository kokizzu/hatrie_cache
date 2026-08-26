package hatCache

import (
	"reflect"
	"strconv"
	"testing"
	"time"
)

var benchmarkSetScalarGenericSink int

func TestSetScalarGenericCandidateMatchesReference(t *testing.T) {
	type structuredValue struct {
		Name   string                 `json:"name"`
		Nested map[string]interface{} `json:"nested"`
	}
	values := []interface{}{
		Map{"name": "alpha"},
		structuredValue{Name: "structured", Nested: map[string]interface{}{"count": float64(1)}},
		[]interface{}{"nested", float64(2)},
		Map{"name": "alpha"},
	}
	var candidate setData
	var reference setData
	for _, value := range values {
		got, gotErr := setScalarGenericAddCandidate(&candidate, value)
		want, wantErr := setScalarGenericAddReference(&reference, value)
		if gotErr != nil || wantErr != nil || got != want {
			t.Fatalf("scalar generic add %#v = %d/%v, want %d/%v", value, got, gotErr, want, wantErr)
		}
		if !reflect.DeepEqual(candidate, reference) {
			t.Fatalf("scalar generic add %#v state differs", value)
		}
	}
}

func TestSetScalarGenericCandidateRejectsWithoutMutation(t *testing.T) {
	candidate := newSetData(Set{"alpha"})
	reference := newSetData(Set{"alpha"})
	got, gotErr := setScalarGenericAddCandidate(&candidate, func() {})
	want, wantErr := setScalarGenericAddReference(&reference, func() {})
	if gotErr == nil || wantErr == nil || got != want || !reflect.DeepEqual(candidate, reference) {
		t.Fatalf("invalid scalar generic add = %d/%v/%#v, want %d/%v/%#v", got, gotErr, candidate, want, wantErr, reference)
	}
}

func TestSetScalarGenericPublicPathsPreservePackedAndGenericSemantics(t *testing.T) {
	ht := newTestTrie(t)
	if added, err := ht.AddSetChecked("set", "alpha", "beta"); err != nil || added != 2 {
		t.Fatalf("AddSetChecked(strings) = %d/%v, want 2/nil", added, err)
	}
	original := Map{"name": "nested"}
	if added, err := ht.AddSetChecked("set", original); err != nil || added != 1 {
		t.Fatalf("AddSetChecked(map) = %d/%v, want 1/nil", added, err)
	}
	original["name"] = "caller"
	stored := Map{"name": "nested"}
	if hit, err := ht.HasSetChecked("set", stored); err != nil || !hit {
		t.Fatalf("HasSetChecked(stored map) = %v/%v, want true/nil", hit, err)
	}
	if added, err := ht.AddSetChecked("set", stored); err != nil || added != 0 {
		t.Fatalf("AddSetChecked(duplicate map) = %d/%v, want 0/nil", added, err)
	}
	if removed, err := ht.RemoveSetChecked("set", stored); err != nil || removed != 1 {
		t.Fatalf("RemoveSetChecked(map) = %d/%v, want 1/nil", removed, err)
	}
	if hit, err := ht.HasSetChecked("set", stored); err != nil || hit {
		t.Fatalf("HasSetChecked(removed map) = %v/%v, want false/nil", hit, err)
	}
}

func BenchmarkSetScalarGenericAddAlternating(b *testing.B) {
	type structuredValue struct {
		Name string `json:"name"`
	}
	for _, benchmark := range []struct {
		name  string
		value interface{}
	}{
		{name: "Map", value: Map{"name": "value"}},
		{name: "Struct", value: structuredValue{Name: "value"}},
		{name: "Slice", value: []interface{}{"value", float64(1)}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			candidate := newSetData(Set{Map{"seed": float64(1)}, Map{"seed": float64(2)}, benchmark.value})
			reference := newSetData(Set{Map{"seed": float64(1)}, Map{"seed": float64(2)}, benchmark.value})
			const operationsPerBlock = 128
			var candidateDuration time.Duration
			var referenceDuration time.Duration
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if iteration&1 == 0 {
					candidateDuration += benchmarkSetScalarGenericCandidateBlock(b, &candidate, benchmark.value, operationsPerBlock)
					referenceDuration += benchmarkSetScalarGenericReferenceBlock(b, &reference, benchmark.value, operationsPerBlock)
				} else {
					referenceDuration += benchmarkSetScalarGenericReferenceBlock(b, &reference, benchmark.value, operationsPerBlock)
					candidateDuration += benchmarkSetScalarGenericCandidateBlock(b, &candidate, benchmark.value, operationsPerBlock)
				}
			}
			b.StopTimer()
			operations := float64(b.N * operationsPerBlock)
			b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/op")
			b.ReportMetric(float64(referenceDuration.Nanoseconds())/operations, "reference-ns/op")
		})
	}
}

func BenchmarkSetScalarGenericAddAllocations(b *testing.B) {
	type structuredValue struct {
		Name string `json:"name"`
	}
	for _, benchmark := range []struct {
		name      string
		value     interface{}
		candidate bool
	}{
		{name: "MapReference", value: Map{"name": "value"}},
		{name: "MapCandidate", value: Map{"name": "value"}, candidate: true},
		{name: "StructReference", value: structuredValue{Name: "value"}},
		{name: "StructCandidate", value: structuredValue{Name: "value"}, candidate: true},
		{name: "SliceReference", value: []interface{}{"value", float64(1)}},
		{name: "SliceCandidate", value: []interface{}{"value", float64(1)}, candidate: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			set := newSetData(Set{Map{"seed": float64(1)}, Map{"seed": float64(2)}, benchmark.value})
			var err error
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if benchmark.candidate {
					benchmarkSetScalarGenericSink, err = setScalarGenericAddCandidate(&set, benchmark.value)
				} else {
					benchmarkSetScalarGenericSink, err = setScalarGenericAddReference(&set, benchmark.value)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSetScalarGenericProductionControls(b *testing.B) {
	type structuredValue struct {
		Name string `json:"name"`
	}
	for _, benchmark := range []struct {
		name   string
		setup  []interface{}
		value  interface{}
		values []interface{}
		remove bool
	}{
		{name: "AddDuplicate", setup: setScalarGenericStructuredValues(3), value: structuredValue{Name: "value-1"}},
		{name: "RemoveMissing", setup: setScalarGenericStructuredValues(3), value: structuredValue{Name: "missing"}, remove: true},
		{name: "PlainStringDuplicate", setup: []interface{}{"value"}, value: "value"},
		{name: "StructuredBatch2", setup: setScalarGenericStructuredValues(2), value: structuredValue{Name: "value-0"}, values: []interface{}{structuredValue{Name: "value-1"}}},
		{name: "StructuredBatch16", setup: setScalarGenericStructuredValues(16), value: structuredValue{Name: "value-0"}, values: setScalarGenericStructuredValues(16)[1:]},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			ht := CreateHatTrie()
			defer ht.Destroy()
			if _, err := ht.AddSetChecked("set:key", benchmark.setup[0], benchmark.setup[1:]...); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				var err error
				if benchmark.remove {
					benchmarkSetScalarGenericSink, err = ht.RemoveSetChecked("set:key", benchmark.value, benchmark.values...)
				} else {
					benchmarkSetScalarGenericSink, err = ht.AddSetChecked("set:key", benchmark.value, benchmark.values...)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func setScalarGenericStructuredValues(count int) []interface{} {
	type structuredValue struct {
		Name string `json:"name"`
	}
	values := make([]interface{}, count)
	for index := range values {
		values[index] = structuredValue{Name: "value-" + strconv.Itoa(index)}
	}
	return values
}

func benchmarkSetScalarGenericCandidateBlock(b *testing.B, set *setData, value interface{}, operations int) time.Duration {
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		added, err := setScalarGenericAddCandidate(set, value)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkSetScalarGenericSink = added
	}
	return time.Since(start)
}

func benchmarkSetScalarGenericReferenceBlock(b *testing.B, set *setData, value interface{}, operations int) time.Duration {
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		added, err := setScalarGenericAddReference(set, value)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkSetScalarGenericSink = added
	}
	return time.Since(start)
}

func setScalarGenericAddCandidate(set *setData, value interface{}) (int, error) {
	key, err := setItemKey(value)
	if err != nil {
		return 0, err
	}
	return set.addKeyValue(key, value), nil
}

func setScalarGenericAddReference(set *setData, value interface{}) (int, error) {
	return set.AddOneChecked(value)
}
