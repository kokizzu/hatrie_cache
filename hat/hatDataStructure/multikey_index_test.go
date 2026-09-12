package hatDataStructure

import (
	"reflect"
	"strconv"
	"sync"
	"testing"
)

func TestStringMultikeyIndexSetLookupAndDelete(t *testing.T) {
	index := NewStringMultikeyIndex(StringMultikeyIndexOptions{
		MaxKeysPerItem: 4,
		MaxItems:       8,
	})
	if err := index.Set(2, []string{"red", "blue", "red"}); err != nil {
		t.Fatalf("set item 2: %v", err)
	}
	if err := index.Set(1, []string{"blue"}); err != nil {
		t.Fatalf("set item 1: %v", err)
	}
	if err := index.Set(3, []string{"red"}); err != nil {
		t.Fatalf("set item 3: %v", err)
	}
	if got := index.Lookup("red", nil); !reflect.DeepEqual(got, []uint64{2, 3}) {
		t.Fatalf("red lookup = %#v, want [2 3]", got)
	}
	if got := index.Lookup("blue", nil); !reflect.DeepEqual(got, []uint64{1, 2}) {
		t.Fatalf("blue lookup = %#v, want [1 2]", got)
	}

	destination := make([]uint64, 0, 4)
	got := index.Lookup("blue", destination)
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("reused lookup = %#v", got)
	}
	if err := index.Set(2, []string{"green"}); err != nil {
		t.Fatalf("update item 2: %v", err)
	}
	if got := index.Lookup("red", nil); !reflect.DeepEqual(got, []uint64{3}) {
		t.Fatalf("red lookup after update = %#v, want [3]", got)
	}
	if got := index.Lookup("blue", nil); !reflect.DeepEqual(got, []uint64{1}) {
		t.Fatalf("blue lookup after update = %#v, want [1]", got)
	}
	if got := index.Lookup("green", nil); !reflect.DeepEqual(got, []uint64{2}) {
		t.Fatalf("green lookup after update = %#v, want [2]", got)
	}
	if !index.Delete(1) {
		t.Fatal("delete item 1 returned false")
	}
	if index.Delete(1) {
		t.Fatal("deleting item 1 twice returned true")
	}
	if got := index.Lookup("blue", nil); len(got) != 0 {
		t.Fatalf("blue lookup after delete = %#v, want empty", got)
	}
	if got := index.Len(); got != 2 {
		t.Fatalf("item count = %d, want 2", got)
	}
}

func TestStringMultikeyIndexSetIsAtomicAndBounded(t *testing.T) {
	index := NewStringMultikeyIndex(StringMultikeyIndexOptions{
		MaxKeysPerItem: 2,
		MaxItems:       1,
	})
	if err := index.Set(7, []string{"a", "b"}); err != nil {
		t.Fatalf("set initial item: %v", err)
	}
	if err := index.Set(7, []string{"a", "b", "c"}); err == nil {
		t.Fatal("key limit violation unexpectedly succeeded")
	}
	if got := index.Lookup("a", nil); !reflect.DeepEqual(got, []uint64{7}) {
		t.Fatalf("a lookup after rejected update = %#v, want [7]", got)
	}
	if got := index.Lookup("c", nil); len(got) != 0 {
		t.Fatalf("c lookup after rejected update = %#v, want empty", got)
	}
	if err := index.Set(8, []string{"d"}); err == nil {
		t.Fatal("item limit violation unexpectedly succeeded")
	}
	if err := index.Set(7, nil); err != nil {
		t.Fatalf("clear item: %v", err)
	}
	if index.Len() != 0 {
		t.Fatalf("item count after clear = %d, want 0", index.Len())
	}
}

func TestStringMultikeyIndexContainsAndEmptyLookups(t *testing.T) {
	index := NewStringMultikeyIndex(StringMultikeyIndexOptions{})
	if err := index.Set(11, []string{"x"}); err != nil {
		t.Fatalf("set item: %v", err)
	}
	if !index.Contains("x", 11) {
		t.Fatal("contains returned false for indexed item")
	}
	if index.Contains("x", 12) || index.Contains("missing", 11) {
		t.Fatal("contains returned true for missing item")
	}
	destination := []uint64{99}
	if got := index.Lookup("missing", destination[:0]); len(got) != 0 {
		t.Fatalf("missing lookup = %#v, want empty", got)
	}
}

func TestStringMultikeyIndexConcurrentReadsAndWrites(t *testing.T) {
	index := NewStringMultikeyIndex(StringMultikeyIndexOptions{MaxKeysPerItem: 3, MaxItems: 64})
	if err := index.Set(0, []string{"initial"}); err != nil {
		t.Fatalf("set initial item: %v", err)
	}
	var waitGroup sync.WaitGroup
	waitGroup.Add(3)
	go func() {
		defer waitGroup.Done()
		for iteration := 0; iteration < 1000; iteration++ {
			id := uint64(iteration % 64)
			if err := index.Set(id, []string{"tag-" + strconv.Itoa(iteration%8)}); err != nil {
				t.Errorf("concurrent set: %v", err)
				return
			}
		}
	}()
	go func() {
		defer waitGroup.Done()
		destination := make([]uint64, 0, 64)
		for iteration := 0; iteration < 1000; iteration++ {
			destination = index.Lookup("tag-"+strconv.Itoa(iteration%8), destination)
		}
	}()
	go func() {
		defer waitGroup.Done()
		for iteration := 0; iteration < 1000; iteration++ {
			index.Contains("tag-"+strconv.Itoa(iteration%8), uint64(iteration%64))
			index.Len()
			index.KeyCount()
		}
	}()
	waitGroup.Wait()
}

func BenchmarkStringMultikeyIndexLookup(b *testing.B) {
	type row struct {
		id   uint64
		keys [2]string
	}
	rows := make([]row, 100000)
	index := NewStringMultikeyIndex(StringMultikeyIndexOptions{})
	for rowIndex := range rows {
		rows[rowIndex] = row{
			id: uint64(rowIndex),
			keys: [2]string{
				"tag-" + strconv.Itoa(rowIndex%100),
				"group-" + strconv.Itoa(rowIndex%32),
			},
		}
		if err := index.Set(rows[rowIndex].id, rows[rowIndex].keys[:]); err != nil {
			b.Fatal(err)
		}
	}
	want := len(index.Lookup("tag-42", nil))
	mapPostings := make(map[string]map[uint64]struct{}, 132)
	for _, row := range rows {
		for _, key := range row.keys {
			ids := mapPostings[key]
			if ids == nil {
				ids = make(map[uint64]struct{})
				mapPostings[key] = ids
			}
			ids[row.id] = struct{}{}
		}
	}

	b.Run("indexed-lookup", func(b *testing.B) {
		b.ReportAllocs()
		destination := make([]uint64, 0, want)
		for range b.N {
			ids := index.Lookup("tag-42", destination)
			if len(ids) != want {
				b.Fatalf("indexed result length = %d, want %d", len(ids), want)
			}
			destination = ids
		}
	})
	b.Run("map-of-sets-lookup", func(b *testing.B) {
		b.ReportAllocs()
		destination := make([]uint64, 0, want)
		for range b.N {
			destination = destination[:0]
			for id := range mapPostings["tag-42"] {
				destination = append(destination, id)
			}
			if len(destination) != want {
				b.Fatalf("map result length = %d, want %d", len(destination), want)
			}
		}
	})
	b.Run("linear-scan", func(b *testing.B) {
		b.ReportAllocs()
		destination := make([]uint64, 0, want)
		for range b.N {
			destination = destination[:0]
			for _, candidate := range rows {
				if candidate.keys[0] == "tag-42" || candidate.keys[1] == "tag-42" {
					destination = append(destination, candidate.id)
				}
			}
			if len(destination) != want {
				b.Fatalf("linear result length = %d, want %d", len(destination), want)
			}
		}
	})
}

func BenchmarkStringMultikeyIndexBuild(b *testing.B) {
	const itemCount = 10000
	type row struct {
		id   uint64
		keys [2]string
	}
	rows := make([]row, itemCount)
	for index := range rows {
		rows[index] = row{
			id: uint64(index),
			keys: [2]string{
				"tag-" + strconv.Itoa(index%100),
				"group-" + strconv.Itoa(index%32),
			},
		}
	}

	b.Run("sorted-postings", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			index := NewStringMultikeyIndex(StringMultikeyIndexOptions{})
			for _, row := range rows {
				if err := index.Set(row.id, row.keys[:]); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("map-of-sets", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			postings := make(map[string]map[uint64]struct{}, 132)
			for _, row := range rows {
				for _, key := range row.keys {
					ids := postings[key]
					if ids == nil {
						ids = make(map[uint64]struct{})
						postings[key] = ids
					}
					ids[row.id] = struct{}{}
				}
			}
		}
	})
	b.Run("map-of-sets-with-reverse", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			postings := make(map[string]map[uint64]struct{}, 132)
			entries := make(map[uint64][]string, itemCount)
			for _, row := range rows {
				keys := append([]string(nil), row.keys[:]...)
				entries[row.id] = keys
				for _, key := range keys {
					ids := postings[key]
					if ids == nil {
						ids = make(map[uint64]struct{})
						postings[key] = ids
					}
					ids[row.id] = struct{}{}
				}
			}
		}
	})
}
