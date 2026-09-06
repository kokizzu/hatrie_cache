package hatMetrics_test

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"hatrie_cache/hat/hatMetrics"
)

func TestReadAmplificationRegistryTracksPartColumnCounters(t *testing.T) {
	registry := hatMetrics.NewReadAmplificationRegistry()
	if err := registry.Record("part-b", "value", 100, 25); err != nil {
		t.Fatal(err)
	}
	if err := registry.Record("part-a", "value", 60, 30); err != nil {
		t.Fatal(err)
	}
	if err := registry.Record("part-b", "value", 20, 10); err != nil {
		t.Fatal(err)
	}

	rows := registry.Snapshot()
	expected := []hatMetrics.ReadAmplification{
		{Part: "part-a", Column: "value", ReadOperations: 1, BytesRead: 60, BytesReturned: 30},
		{Part: "part-b", Column: "value", ReadOperations: 2, BytesRead: 120, BytesReturned: 35},
	}
	if !reflect.DeepEqual(expected, rows) {
		t.Fatalf("rows = %#v, want %#v", rows, expected)
	}
	if difference := rows[1].Ratio() - 120.0/35.0; difference < -0.000001 || difference > 0.000001 {
		t.Fatalf("ratio = %v, want %v", rows[1].Ratio(), 120.0/35.0)
	}

	rows[0].BytesRead = 999
	if got := registry.Snapshot()[0].BytesRead; got != 60 {
		t.Fatalf("snapshot mutation changed registry: bytes_read = %d", got)
	}
}

func TestReadAmplificationRegistryRejectsInvalidIdentityAndHandlesZeroOutput(t *testing.T) {
	registry := hatMetrics.NewReadAmplificationRegistry()
	if err := registry.Record("", "value", 1, 1); !errors.Is(err, hatMetrics.ErrReadAmplificationIdentityRequired) {
		t.Fatalf("empty part error = %v", err)
	}
	if err := registry.Record("part", "", 1, 1); !errors.Is(err, hatMetrics.ErrReadAmplificationIdentityRequired) {
		t.Fatalf("empty column error = %v", err)
	}
	if err := registry.Record("part", "value", 10, 0); err != nil {
		t.Fatal(err)
	}
	rows := registry.Snapshot()
	if len(rows) != 1 || rows[0].Ratio() != 0 {
		t.Fatalf("zero-output rows = %#v", rows)
	}

	var nilRegistry *hatMetrics.ReadAmplificationRegistry
	if err := nilRegistry.Record("part", "value", 1, 1); err != nil {
		t.Fatal(err)
	}
	if nilRegistry.Snapshot() != nil {
		t.Fatal("nil registry snapshot is not nil")
	}
}

func TestReadAmplificationRegistrySerializesConcurrentRecords(t *testing.T) {
	registry := hatMetrics.NewReadAmplificationRegistry()
	const workers = 8
	const recordsPerWorker = 100
	var waitGroup sync.WaitGroup
	waitGroup.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer waitGroup.Done()
			for record := 0; record < recordsPerWorker; record++ {
				if err := registry.Record("part", "value", 3, 2); err != nil {
					t.Errorf("record: %v", err)
					return
				}
			}
		}()
	}
	waitGroup.Wait()

	rows := registry.Snapshot()
	if len(rows) != 1 {
		t.Fatalf("rows = %#v", rows)
	}
	if rows[0].ReadOperations != workers*recordsPerWorker {
		t.Fatalf("read operations = %d", rows[0].ReadOperations)
	}
	if rows[0].BytesRead != workers*recordsPerWorker*3 {
		t.Fatalf("bytes read = %d", rows[0].BytesRead)
	}
	if rows[0].BytesReturned != workers*recordsPerWorker*2 {
		t.Fatalf("bytes returned = %d", rows[0].BytesReturned)
	}
}

func BenchmarkReadAmplificationRegistryRecord(b *testing.B) {
	registry := hatMetrics.NewReadAmplificationRegistry()
	if err := registry.Record("part", "value", 1, 1); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := registry.Record("part", "value", 64, 32); err != nil {
			b.Fatal(err)
		}
	}
}
