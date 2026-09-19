package hatDataStructure

import (
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestTupleMultikeyIndexSeparatesTypesAndCopiesValues(t *testing.T) {
	index := NewTupleMultikeyIndex(TupleMultikeyIndexOptions{MaxKeysPerItem: 12})
	bytes := []byte{1, 2, 3}
	date := time.Date(2024, time.January, 2, 23, 59, 58, 0, time.FixedZone("test", 8*60*60))
	timestamp := time.Date(2024, time.January, 2, 23, 59, 58, 123, time.FixedZone("test", 8*60*60))
	keys := []TupleFieldValue{
		TupleString("1"),
		TupleInt64(1),
		TupleUint64(1),
		TupleBytes(bytes),
		TupleNull(),
		TupleBool(true),
		TupleDate(date),
		TupleTimestamp(timestamp),
		TupleFloat64(math.Pi),
	}
	if err := index.Set(7, keys); err != nil {
		t.Fatalf("set tuple keys: %v", err)
	}
	bytes[0] = 9

	tests := []struct {
		name string
		key  TupleFieldValue
	}{
		{name: "string", key: TupleString("1")},
		{name: "int64", key: TupleInt64(1)},
		{name: "uint64", key: TupleUint64(1)},
		{name: "bytes", key: TupleBytes([]byte{1, 2, 3})},
		{name: "null", key: TupleNull()},
		{name: "bool", key: TupleBool(true)},
		{name: "date", key: TupleDate(time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC))},
		{name: "timestamp", key: TupleTimestamp(timestamp)},
		{name: "float64", key: TupleFloat64(math.Pi)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := index.Lookup(test.key, nil)
			if err != nil {
				t.Fatalf("lookup: %v", err)
			}
			if !reflect.DeepEqual(got, []uint64{7}) {
				t.Fatalf("lookup = %#v, want [7]", got)
			}
		})
	}
	for _, different := range []TupleFieldValue{
		TupleString("2"),
		TupleInt64(2),
		TupleUint64(2),
		TupleBytes([]byte{1, 2, 4}),
		TupleBool(false),
	} {
		got, err := index.Lookup(different, nil)
		if err != nil {
			t.Fatalf("lookup different value: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("lookup for %#v = %#v, want empty", different, got)
		}
	}
}

func TestTupleMultikeyIndexDeduplicatesUpdatesAndDeletes(t *testing.T) {
	index := NewTupleMultikeyIndex(TupleMultikeyIndexOptions{})
	if err := index.Set(2, []TupleFieldValue{TupleString("red"), TupleString("blue"), TupleString("red")}); err != nil {
		t.Fatalf("set item 2: %v", err)
	}
	if err := index.Set(1, []TupleFieldValue{TupleString("blue")}); err != nil {
		t.Fatalf("set item 1: %v", err)
	}
	if got, err := index.Lookup(TupleString("red"), nil); err != nil || !reflect.DeepEqual(got, []uint64{2}) {
		t.Fatalf("red lookup = %#v, %v; want [2]", got, err)
	}
	if got, err := index.Lookup(TupleString("blue"), nil); err != nil || !reflect.DeepEqual(got, []uint64{1, 2}) {
		t.Fatalf("blue lookup = %#v, %v; want [1 2]", got, err)
	}
	if err := index.Set(2, []TupleFieldValue{TupleString("green")}); err != nil {
		t.Fatalf("update item 2: %v", err)
	}
	if got, err := index.Lookup(TupleString("red"), nil); err != nil || len(got) != 0 {
		t.Fatalf("red lookup after update = %#v, %v; want empty", got, err)
	}
	if !index.Delete(1) || index.Delete(1) {
		t.Fatal("delete result did not report exactly one existing item")
	}
	if got, err := index.Lookup(TupleString("blue"), nil); err != nil || len(got) != 0 {
		t.Fatalf("blue lookup after delete = %#v, %v; want empty", got, err)
	}
	if index.Len() != 1 || index.KeyCount() != 1 {
		t.Fatalf("counts = (%d items, %d keys), want (1, 1)", index.Len(), index.KeyCount())
	}
}

func TestTupleMultikeyIndexBoundsAreAtomic(t *testing.T) {
	index := NewTupleMultikeyIndex(TupleMultikeyIndexOptions{MaxKeysPerItem: 2, MaxItems: 1})
	if err := index.Set(7, []TupleFieldValue{TupleString("a"), TupleString("b")}); err != nil {
		t.Fatalf("set initial item: %v", err)
	}
	if err := index.Set(7, []TupleFieldValue{TupleString("a"), TupleString("b"), TupleString("c")}); err == nil {
		t.Fatal("key limit violation unexpectedly succeeded")
	}
	if got, err := index.Lookup(TupleString("a"), nil); err != nil || !reflect.DeepEqual(got, []uint64{7}) {
		t.Fatalf("a lookup after rejected update = %#v, %v; want [7]", got, err)
	}
	if got, err := index.Lookup(TupleString("c"), nil); err != nil || len(got) != 0 {
		t.Fatalf("c lookup after rejected update = %#v, %v; want empty", got, err)
	}
	if err := index.Set(8, []TupleFieldValue{TupleString("d")}); err == nil {
		t.Fatal("item limit violation unexpectedly succeeded")
	}
	if err := index.Set(7, nil); err != nil {
		t.Fatalf("clear item: %v", err)
	}
	if index.Len() != 0 {
		t.Fatalf("item count after clear = %d, want 0", index.Len())
	}
}

func TestTupleMultikeyIndexRejectsInvalidValues(t *testing.T) {
	index := NewTupleMultikeyIndex(TupleMultikeyIndexOptions{})
	if err := index.Set(1, []TupleFieldValue{TupleString("kept")}); err != nil {
		t.Fatalf("set initial value: %v", err)
	}
	invalid := TupleFieldValue{Kind: TupleFieldInvalid, Valid: true}
	if err := index.Set(1, []TupleFieldValue{TupleString("new"), invalid}); err == nil {
		t.Fatal("invalid tuple value unexpectedly succeeded")
	}
	if got, err := index.Lookup(TupleString("kept"), nil); err != nil || !reflect.DeepEqual(got, []uint64{1}) {
		t.Fatalf("kept lookup after rejected update = %#v, %v; want [1]", got, err)
	}
	if got, err := index.Lookup(TupleString("new"), nil); err != nil || len(got) != 0 {
		t.Fatalf("new lookup after rejected update = %#v, %v; want empty", got, err)
	}
	if _, err := index.Lookup(invalid, nil); err == nil {
		t.Fatal("invalid lookup unexpectedly succeeded")
	}
	if _, err := index.Contains(invalid, 1); err == nil {
		t.Fatal("invalid contains unexpectedly succeeded")
	}
}

func BenchmarkTupleMultikeyIndexLookup(b *testing.B) {
	const itemCount = 100000
	type row struct {
		id    uint64
		tag   string
		group int64
	}
	rows := make([]row, itemCount)
	typedIndex := NewTupleMultikeyIndex(TupleMultikeyIndexOptions{})
	stringIndex := NewStringMultikeyIndex(StringMultikeyIndexOptions{})
	mapPostings := make(map[string]map[uint64]struct{}, 132)
	for rowIndex := range rows {
		rows[rowIndex] = row{
			id:    uint64(rowIndex),
			tag:   "tag-" + strconv.Itoa(rowIndex%100),
			group: int64(rowIndex % 32),
		}
		if err := typedIndex.Set(rows[rowIndex].id, []TupleFieldValue{
			TupleString(rows[rowIndex].tag),
			TupleInt64(rows[rowIndex].group),
		}); err != nil {
			b.Fatal(err)
		}
		if err := stringIndex.Set(rows[rowIndex].id, []string{
			rows[rowIndex].tag,
			"group-" + strconv.FormatInt(rows[rowIndex].group, 10),
		}); err != nil {
			b.Fatal(err)
		}
		for _, key := range []string{rows[rowIndex].tag, "group-" + strconv.FormatInt(rows[rowIndex].group, 10)} {
			ids := mapPostings[key]
			if ids == nil {
				ids = make(map[uint64]struct{})
				mapPostings[key] = ids
			}
			ids[rows[rowIndex].id] = struct{}{}
		}
	}
	want := len(rows) / 100

	b.Run("typed-index", func(b *testing.B) {
		b.ReportAllocs()
		destination := make([]uint64, 0, want)
		key := TupleString("tag-42")
		for range b.N {
			ids, err := typedIndex.Lookup(key, destination)
			if err != nil {
				b.Fatal(err)
			}
			if len(ids) != want {
				b.Fatalf("typed result length = %d, want %d", len(ids), want)
			}
			destination = ids
		}
	})
	b.Run("typed-int64-index", func(b *testing.B) {
		b.ReportAllocs()
		destination := make([]uint64, 0, itemCount/32)
		key := TupleInt64(7)
		for range b.N {
			ids, err := typedIndex.Lookup(key, destination)
			if err != nil {
				b.Fatal(err)
			}
			if len(ids) != itemCount/32 {
				b.Fatalf("typed int64 result length = %d, want %d", len(ids), itemCount/32)
			}
			destination = ids
		}
	})
	b.Run("string-index", func(b *testing.B) {
		b.ReportAllocs()
		destination := make([]uint64, 0, want)
		for range b.N {
			ids := stringIndex.Lookup("tag-42", destination)
			if len(ids) != want {
				b.Fatalf("string result length = %d, want %d", len(ids), want)
			}
			destination = ids
		}
	})
	b.Run("map-of-sets", func(b *testing.B) {
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
				if candidate.tag == "tag-42" {
					destination = append(destination, candidate.id)
				}
			}
			if len(destination) != want {
				b.Fatalf("linear result length = %d, want %d", len(destination), want)
			}
		}
	})
}

func BenchmarkTupleMultikeyIndexBuild(b *testing.B) {
	const itemCount = 10000
	type row struct {
		id    uint64
		tag   string
		group int64
	}
	rows := make([]row, itemCount)
	for index := range rows {
		rows[index] = row{
			id:    uint64(index),
			tag:   "tag-" + strconv.Itoa(index%100),
			group: int64(index % 32),
		}
	}

	b.Run("typed-index", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			index := NewTupleMultikeyIndex(TupleMultikeyIndexOptions{})
			for _, row := range rows {
				if err := index.Set(row.id, []TupleFieldValue{
					TupleString(row.tag),
					TupleInt64(row.group),
				}); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("string-index", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			index := NewStringMultikeyIndex(StringMultikeyIndexOptions{})
			for _, row := range rows {
				if err := index.Set(row.id, []string{
					row.tag,
					"group-" + strconv.FormatInt(row.group, 10),
				}); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}
