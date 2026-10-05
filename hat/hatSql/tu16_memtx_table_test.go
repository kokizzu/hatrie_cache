package hatSql

import (
	"context"
	"math"
	"strconv"
	"sync"
	"testing"
)

func TestMemtxTableLifecycle(t *testing.T) {
	table, err := NewMemtxTable(MemtxTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "name", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
		},
		Indexes: []MemtxIndexDefinition{{Name: "events_name", Field: "name"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := []TypedTableValue{TypedString("Ada"), TypedInt64(10)}
	change, err := table.Upsert("a", input)
	if err != nil {
		t.Fatal(err)
	}
	input[0] = TypedString("mutated")
	change.After[0] = TypedString("changed-result")
	row, ok, err := table.Get("a")
	if err != nil || !ok || row["name"] != "Ada" {
		t.Fatalf("tuple ownership = %#v, %v, %v", row, ok, err)
	}
	if _, err := table.Upsert("b", []TypedTableValue{TypedString("Bob"), TypedInt64(20)}); err != nil {
		t.Fatal(err)
	}
	usage := table.MemoryUsage()
	if usage.Rows != 2 || usage.IndexEntries != 2 || usage.LogicalBytes <= 0 {
		t.Fatalf("memory usage = %#v", usage)
	}
	change, err = table.Upsert("a", []TypedTableValue{TypedString("Ada"), TypedInt64(11)})
	if err != nil {
		t.Fatal(err)
	}
	if change.Operation != "UPDATE" || change.Before[1].Int64 != 10 || change.After[1].Int64 != 11 {
		t.Fatalf("update change = %#v", change)
	}
	row, ok, err = table.Get("a")
	if err != nil || !ok || row["score"] != int64(11) {
		t.Fatalf("Get(a) = %#v, %v, %v", row, ok, err)
	}
	candidates, available, err := table.ResolveSQLIndexedSource("CACHE", "events", "name", "Ada")
	if err != nil || !available || len(candidates) != 1 || candidates[0]["name"] != "Ada" {
		t.Fatalf("indexed source = %#v, %v, %v", candidates, available, err)
	}
	filtered, err := ExecuteQueryParameters(context.Background(), "FROM CACHE('events') SELECT name, score WHERE name = 'Ada'", table, nil, QueryOptions{})
	if err != nil || len(filtered.Rows) != 1 || filtered.Rows[0]["name"] != "Ada" {
		t.Fatalf("indexed SQL result = %#v, %v", filtered, err)
	}
	if _, err := table.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if rows := table.Rows(); len(rows) != 1 || rows[0]["name"] != "Bob" {
		t.Fatalf("rows after delete = %#v", rows)
	}
	if usage := table.MemoryUsage(); usage.Rows != 1 || usage.IndexEntries != 1 {
		t.Fatalf("memory usage after delete = %#v", usage)
	}
	result, err := ExecuteQueryParameters(context.Background(), "FROM CACHE('events') SELECT name, score", table, nil, QueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["name"] != "Bob" {
		t.Fatalf("SQL result = %#v, %v", result, err)
	}
}

func TestMemtxTableIndexTracksSwapDelete(t *testing.T) {
	table, err := NewMemtxTable(MemtxTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "group", Kind: TypedTableString}},
		Indexes: []MemtxIndexDefinition{{Name: "events_group", Field: "group"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key   string
		group string
	}{
		{key: "a", group: "one"},
		{key: "b", group: "two"},
		{key: "c", group: "three"},
	} {
		if _, err := table.Upsert(row.key, []TypedTableValue{TypedString(row.group)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := table.Delete("a"); err != nil {
		t.Fatal(err)
	}
	candidates, available, err := table.ResolveSQLIndexedSource("CACHE", "events", "group", "three")
	if err != nil || !available || len(candidates) != 1 || candidates[0]["group"] != "three" {
		t.Fatalf("index after swap delete = %#v, %v, %v", candidates, available, err)
	}
}

func TestMemtxTableValidation(t *testing.T) {
	if _, err := NewMemtxTable(MemtxTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableString, DictionaryEncoded: true}},
	}); err == nil {
		t.Fatal("expected unsupported dictionary option error")
	}
	if _, err := NewMemtxTable(MemtxTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableString, GeneratedMode: TypedTableGeneratedDefault}},
	}); err == nil {
		t.Fatal("expected unsupported generated-mode error")
	}
	if _, err := NewMemtxTable(MemtxTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableString, TTL: TypedTableTTLOptions{Lifetime: 1}}},
	}); err == nil {
		t.Fatal("expected unsupported TTL option error")
	}
	if _, err := NewMemtxTable(MemtxTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableString}},
		Indexes: []MemtxIndexDefinition{{Name: "", Field: "value"}},
	}); err == nil {
		t.Fatal("expected invalid index error")
	}
}

func TestMemtxTableUniqueIndex(t *testing.T) {
	table, err := NewMemtxTable(MemtxTableSchema{
		Name:    "users",
		Columns: []TypedTableColumn{{Name: "email", Kind: TypedTableString}},
		Indexes: []MemtxIndexDefinition{{Name: "users_email", Field: "email", Unique: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("a", []TypedTableValue{TypedString("a@example.test")}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("b", []TypedTableValue{TypedString("a@example.test")}); err == nil {
		t.Fatal("expected unique-index violation")
	}
	if _, err := table.Upsert("b", []TypedTableValue{TypedNull()}); err != nil {
		t.Fatal(err)
	}
}

func TestMemtxTableConcurrentAccess(t *testing.T) {
	table, err := NewMemtxTable(MemtxTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
		Indexes: []MemtxIndexDefinition{{Name: "events_value", Field: "value"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		worker := worker
		group.Add(1)
		go func() {
			defer group.Done()
			for index := 0; index < 64; index++ {
				key := "key-" + strconv.Itoa(worker*64+index)
				if _, err := table.Upsert(key, []TypedTableValue{TypedInt64(int64(index))}); err != nil {
					t.Errorf("Upsert(%q): %v", key, err)
					return
				}
				if _, ok, err := table.Get(key); err != nil || !ok {
					t.Errorf("Get(%q) = %v, %v", key, ok, err)
					return
				}
			}
		}()
	}
	group.Wait()
	if rows := table.Rows(); len(rows) != 512 {
		t.Fatalf("concurrent rows = %d, want 512", len(rows))
	}
}

func TestMemtxTableFloatIndexCanonicalizesSignedZero(t *testing.T) {
	table, err := NewMemtxTable(MemtxTableSchema{
		Name:    "measurements",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableFloat64}},
		Indexes: []MemtxIndexDefinition{{Name: "measurements_value", Field: "value"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("zero", []TypedTableValue{TypedFloat64(math.Copysign(0, -1))}); err != nil {
		t.Fatal(err)
	}
	rows, available, err := table.ResolveSQLIndexedSource("CACHE", "measurements", "value", float64(0))
	if err != nil || !available || len(rows) != 1 {
		t.Fatalf("signed-zero index = %#v, %v, %v", rows, available, err)
	}
}

func BenchmarkMemtxTableUpsert(b *testing.B) {
	table := newMemtxBenchmarkTable(b)
	values := []TypedTableValue{TypedString("region-a"), TypedInt64(42)}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := table.Upsert("key", values); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedTableUpsertControl(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "region", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	values := []TypedTableValue{TypedString("region-a"), TypedInt64(42)}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := table.Upsert("key", values); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMemtxTableIndexedLookup(b *testing.B) {
	table := newMemtxBenchmarkTable(b)
	for index := 0; index < 1024; index++ {
		if _, err := table.Upsert(strconv.Itoa(index), []TypedTableValue{TypedString("region-" + strconv.Itoa(index)), TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows, available, err := table.ResolveSQLIndexedSource("CACHE", "events", "region", "region-"+strconv.Itoa(index%1024))
		if err != nil || !available || len(rows) != 1 {
			b.Fatalf("indexed lookup = %d, %v, %v", len(rows), available, err)
		}
	}
}

func BenchmarkMemtxTableRows(b *testing.B) {
	table := newMemtxRowsBenchmarkTable(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if rows := table.Rows(); len(rows) != 1024 {
			b.Fatalf("rows = %d", len(rows))
		}
	}
}

func BenchmarkTypedTableRowsControl(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "region", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 1024; index++ {
		if _, err := table.Upsert("key-"+strconv.Itoa(index), []TypedTableValue{TypedString("region-a"), TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if rows := table.Rows(); len(rows) != 1024 {
			b.Fatalf("rows = %d", len(rows))
		}
	}
}

func BenchmarkTypedTableFullScanLookupControl(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "region", Kind: TypedTableString}, {Name: "score", Kind: TypedTableInt64}},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 1024; index++ {
		if _, err := table.Upsert("key-"+strconv.Itoa(index), []TypedTableValue{TypedString("region-" + strconv.Itoa(index)), TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	target := "region-777"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows, err := table.ResolveSQLSource("CACHE", "events")
		if err != nil {
			b.Fatal(err)
		}
		found := 0
		for _, row := range rows {
			if row["region"] == target {
				found++
			}
		}
		if found != 1 {
			b.Fatalf("full-scan lookup found %d rows", found)
		}
	}
}

func newMemtxBenchmarkTable(tb testing.TB) *MemtxTable {
	tb.Helper()
	table, err := NewMemtxTable(MemtxTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "region", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
		},
		Indexes: []MemtxIndexDefinition{{Name: "events_region", Field: "region"}},
	})
	if err != nil {
		tb.Fatal(err)
	}
	return table
}

func newMemtxRowsBenchmarkTable(tb testing.TB) *MemtxTable {
	tb.Helper()
	table, err := NewMemtxTable(MemtxTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "region", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		tb.Fatal(err)
	}
	for index := 0; index < 1024; index++ {
		if _, err := table.Upsert("key-"+strconv.Itoa(index), []TypedTableValue{TypedString("region-a"), TypedInt64(int64(index))}); err != nil {
			tb.Fatal(err)
		}
	}
	return table
}
