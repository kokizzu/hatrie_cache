package functional_multikey_contract_test

import (
	"reflect"
	"sync"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func newStringFunctionalMultikey(t *testing.T) *hatDataStructure.FunctionalMultikeyIndex[benchmarkRow, string] {
	t.Helper()
	index, err := hatDataStructure.NewFunctionalMultikeyIndex(
		func(row benchmarkRow) []string { return row.Tags },
		hatDataStructure.FunctionalMultikeyIndexOptions{MaxKeysPerItem: 8, MaxItems: 16},
	)
	if err != nil {
		t.Fatal(err)
	}
	return index
}

func TestFunctionalMultikeyLifecycleAndDeduplication(t *testing.T) {
	index := newStringFunctionalMultikey(t)
	if err := index.Upsert(2, benchmarkRow{Tags: []string{"sql", "go", "sql"}}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, benchmarkRow{Tags: []string{"go", "cache"}}); err != nil {
		t.Fatal(err)
	}
	if got := index.Lookup("go", nil); !reflect.DeepEqual(got, []uint64{1, 2}) {
		t.Fatalf("go lookup=%v", got)
	}
	dst := make([]uint64, 0, 4)
	if got := index.Lookup("sql", dst[:0]); !reflect.DeepEqual(got, []uint64{2}) {
		t.Fatalf("sql lookup=%v", got)
	}
	if !index.Contains("cache", 1) || index.Contains("sql", 1) {
		t.Fatal("contains result mismatch")
	}
	if index.Len() != 2 || index.KeyCount() != 3 {
		t.Fatalf("len=%d key-count=%d", index.Len(), index.KeyCount())
	}

	if err := index.Upsert(1, benchmarkRow{Tags: []string{"cache", "typed"}}); err != nil {
		t.Fatal(err)
	}
	if got := index.Lookup("go", nil); !reflect.DeepEqual(got, []uint64{2}) {
		t.Fatalf("go lookup after replacement=%v", got)
	}
	if !index.Delete(2) || index.Delete(2) {
		t.Fatal("delete result mismatch")
	}
	if index.Len() != 1 || index.KeyCount() != 2 {
		t.Fatalf("after delete len=%d key-count=%d", index.Len(), index.KeyCount())
	}
}

func TestFunctionalMultikeySupportsTypedComparableKeys(t *testing.T) {
	type typedRow struct{ Values []int }
	index, err := hatDataStructure.NewFunctionalMultikeyIndex(
		func(row typedRow) []int { return row.Values },
		hatDataStructure.FunctionalMultikeyIndexOptions{MaxKeysPerItem: 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(7, typedRow{Values: []int{10, 20, 10}}); err != nil {
		t.Fatal(err)
	}
	if got := index.Lookup(10, nil); !reflect.DeepEqual(got, []uint64{7}) {
		t.Fatalf("typed lookup=%v", got)
	}
}

func TestFunctionalMultikeyRejectsBoundExceededWithoutMutation(t *testing.T) {
	index, err := hatDataStructure.NewFunctionalMultikeyIndex(
		func(row benchmarkRow) []string { return row.Tags },
		hatDataStructure.FunctionalMultikeyIndexOptions{MaxKeysPerItem: 2, MaxItems: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, benchmarkRow{Tags: []string{"one", "two", "three"}}); err == nil {
		t.Fatal("expected key limit error")
	}
	if index.Len() != 0 {
		t.Fatal("key-limit rejection changed state")
	}
	if err := index.Upsert(1, benchmarkRow{Tags: []string{"one"}}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(2, benchmarkRow{Tags: []string{"two"}}); err == nil {
		t.Fatal("expected item limit error")
	}
	if index.Len() != 1 || !index.Contains("one", 1) {
		t.Fatal("item-limit rejection changed state")
	}
}

func TestFunctionalMultikeyRequiresExtractor(t *testing.T) {
	if _, err := hatDataStructure.NewFunctionalMultikeyIndex[benchmarkRow, string](nil, hatDataStructure.FunctionalMultikeyIndexOptions{}); err == nil {
		t.Fatal("expected nil extractor error")
	}
	var index *hatDataStructure.FunctionalMultikeyIndex[benchmarkRow, string]
	if index.Len() != 0 || index.Contains("anything", 1) || index.Lookup("anything", nil) != nil {
		t.Fatal("nil receiver was not inert")
	}
}

func TestFunctionalMultikeyConcurrentUse(t *testing.T) {
	index, err := hatDataStructure.NewFunctionalMultikeyIndex(
		func(row benchmarkRow) []string { return row.Tags },
		hatDataStructure.FunctionalMultikeyIndexOptions{MaxKeysPerItem: 4, MaxItems: 16},
	)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		worker := worker
		group.Add(1)
		go func() {
			defer group.Done()
			id := uint64(worker + 1)
			for iteration := 0; iteration < 100; iteration++ {
				keys := []string{"worker-" + string(rune('a'+worker)), "iteration-" + string(rune('a'+iteration%26))}
				if err := index.Upsert(id, benchmarkRow{Tags: keys}); err != nil {
					t.Errorf("upsert: %v", err)
					return
				}
				if got := index.Lookup(keys[0], nil); len(got) == 0 {
					t.Errorf("lookup failed")
					return
				}
			}
		}()
	}
	group.Wait()
	if index.Len() != 8 {
		t.Fatalf("concurrent length=%d", index.Len())
	}
}

func BenchmarkTU23CandidateUpsert(b *testing.B) {
	const size = 10_000
	rows := benchmarkRows(size)
	index, err := hatDataStructure.NewFunctionalMultikeyIndex(
		func(row benchmarkRow) []string { return row.Tags },
		hatDataStructure.FunctionalMultikeyIndexOptions{MaxKeysPerItem: 8, MaxItems: size},
	)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := index.Upsert(uint64(i%size+1), rows[i%size]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU23CandidateBuild10000(b *testing.B) {
	const size = 10_000
	rows := benchmarkRows(size)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		index, err := hatDataStructure.NewFunctionalMultikeyIndex(
			func(row benchmarkRow) []string { return row.Tags },
			hatDataStructure.FunctionalMultikeyIndexOptions{MaxKeysPerItem: 8, MaxItems: size},
		)
		if err != nil {
			b.Fatal(err)
		}
		for i, row := range rows {
			if err := index.Upsert(uint64(i+1), row); err != nil {
				b.Fatal(err)
			}
		}
	}
}
