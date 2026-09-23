package hatSql

import (
	"reflect"
	"testing"
)

func TestCHG04RuntimeBloomFilterHasNoFalseNegatives(t *testing.T) {
	index := newSQLJoinHashIndex(1024)
	inserted := []interface{}{float64(0), float64(1), float64(17), float64(99), false, true}

	for row, value := range inserted {
		if !index.Add(value, row) {
			t.Fatalf("Add(%#v) = false", value)
		}
	}
	for probe := 0; probe < sqlJoinRuntimeBloomSampleSize; probe++ {
		index.Lookup(float64(1_000_000 + probe))
	}
	if index.bloom == nil {
		t.Fatal("runtime bloom filter did not activate for a miss-heavy sample")
	}
	for _, value := range inserted {
		key, ok := newSQLJoinProbeKey(value)
		if !ok || !index.bloom.mayContain(key.hash) {
			t.Fatalf("runtime bloom filter rejected inserted value %#v", value)
		}
	}

	for row, value := range inserted {
		if got := index.Lookup(value); !reflect.DeepEqual(got, []int{row}) {
			t.Fatalf("Lookup(%#v) = %#v, want [%d]", value, got, row)
		}
	}
	if got := index.Lookup(float64(1000)); got != nil {
		t.Fatalf("Lookup(missing) = %#v, want nil", got)
	}
}

func TestCHG04RuntimeBloomFilterBuildsLazily(t *testing.T) {
	index := newSQLJoinHashIndex(1024)
	for row := 0; row < 1024; row++ {
		index.Add(float64(row), row)
	}
	if index.bloom != nil {
		t.Fatal("runtime bloom filter allocated before probe sampling")
	}
}

func TestCHG04RuntimeBloomFilterDoesNotChangeStringLookup(t *testing.T) {
	index := newSQLJoinHashIndex(4)
	if !index.Add("alpha", 3) || !index.Add("beta", 7) {
		t.Fatal("string Add() rejected a supported key")
	}
	if got := index.Lookup("alpha"); !reflect.DeepEqual(got, []int{3}) {
		t.Fatalf("Lookup(alpha) = %#v, want [3]", got)
	}
	if got := index.Lookup("missing"); got != nil {
		t.Fatalf("Lookup(missing string) = %#v, want nil", got)
	}
}

func TestCHG04RuntimeBloomFilterSkipsSmallIndexes(t *testing.T) {
	if index := newSQLJoinHashIndex(256); index.bloom != nil {
		t.Fatal("small join index allocated a runtime bloom filter")
	}
}

func TestCHG04RuntimeBloomFilterRequiresMissHeavySample(t *testing.T) {
	index := newSQLJoinHashIndex(1024)
	for row := 0; row < 1024; row++ {
		index.Add(float64(row), row)
	}
	for probe := 0; probe < sqlJoinRuntimeBloomSampleSize; probe++ {
		value := float64(probe / 2)
		if probe%2 == 1 {
			value = float64(1_000_000 + probe)
		}
		index.Lookup(value)
	}
	if index.bloom != nil {
		t.Fatal("runtime bloom filter activated for a 50% miss sample")
	}

	index = newSQLJoinHashIndex(1024)
	for row := 0; row < 1024; row++ {
		index.Add(float64(row), row)
	}
	for probe := 0; probe < sqlJoinRuntimeBloomSampleSize; probe++ {
		index.Lookup(float64(1_000_000 + probe))
	}
	if index.bloom == nil || index.bloomState != sqlJoinRuntimeBloomActive {
		t.Fatal("runtime bloom filter did not activate for an all-miss sample")
	}
}

func TestCHG04RuntimeBloomFilterFalsePositiveRate(t *testing.T) {
	index := newSQLJoinHashIndex(4096)
	for row := 0; row < 4096; row++ {
		index.Add(float64(row), row)
	}
	for probe := 0; probe < sqlJoinRuntimeBloomSampleSize; probe++ {
		index.Lookup(float64(1_000_000 + probe))
	}
	if index.bloom == nil {
		t.Fatal("runtime bloom filter did not activate")
	}

	const probes = 100_000
	falsePositives := 0
	for probe := 0; probe < probes; probe++ {
		key, _ := newSQLJoinProbeKey(float64(2_000_000 + probe))
		if index.bloom.mayContain(key.hash) {
			falsePositives++
		}
	}
	rate := float64(falsePositives) / probes
	t.Logf("false-positive rate = %d/%d (%.4f%%)", falsePositives, probes, rate*100)
	if rate > 0.05 {
		t.Fatalf("false-positive rate = %.4f, want <= 0.05", rate)
	}
}
