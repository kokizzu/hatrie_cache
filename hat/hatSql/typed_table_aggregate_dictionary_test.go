package hatSql

import (
	"reflect"
	"strconv"
	"testing"
	"unsafe"
)

var typedTableAggregateDictionaryBenchmarkRowsResult []Row

func BenchmarkTypedTableAggregateDictionaryEncoding(b *testing.B) {
	table, changes := newTypedTableAggregateDictionaryBenchmarkInput(b)
	benchmarkTypedTableAggregateDictionaryEncoding(b, table, changes)
}

func BenchmarkTypedTableAggregateDictionaryEncodingRepeatedGroups(b *testing.B) {
	table, changes := newTypedTableAggregateDictionaryBenchmarkInputWith(b, 10_000, 64, 8)
	benchmarkTypedTableAggregateDictionaryEncoding(b, table, changes)
}

func benchmarkTypedTableAggregateDictionaryEncoding(b *testing.B, table *TypedTable, changes []TypedTableChange) {
	for _, benchmark := range []struct {
		name       string
		dictionary bool
	}{
		{name: "legacy", dictionary: false},
		{name: "dictionary_encoded", dictionary: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			definition := TypedTableAggregateDefinition{
				GroupBy:                []string{"team", "region"},
				SumField:               "points",
				DictionaryEncodeGroups: benchmark.dictionary,
			}
			b.Run("apply", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					aggregate, err := NewTypedTableAggregate(table, definition)
					if err != nil {
						b.Fatal(err)
					}
					if err := aggregate.Apply(changes); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("apply_rows", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					aggregate, err := NewTypedTableAggregate(table, definition)
					if err != nil {
						b.Fatal(err)
					}
					if err := aggregate.Apply(changes); err != nil {
						b.Fatal(err)
					}
					typedTableAggregateDictionaryBenchmarkRowsResult = aggregate.Rows()
				}
			})
			b.Run("rows_existing_state", func(b *testing.B) {
				reference, err := NewTypedTableAggregate(table, definition)
				if err != nil {
					b.Fatal(err)
				}
				if err := reference.Apply(changes); err != nil {
					b.Fatal(err)
				}
				typedTableAggregateDictionaryBenchmarkRowsResult = reference.Rows()
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					typedTableAggregateDictionaryBenchmarkRowsResult = reference.Rows()
				}
				b.StopTimer()
				b.ReportMetric(float64(typedTableAggregateGroupKeyBytes(reference)), "group-key-bytes-est")
			})
		})
	}
}

func newTypedTableAggregateDictionaryBenchmarkInput(b *testing.B) (*TypedTable, []TypedTableChange) {
	return newTypedTableAggregateDictionaryBenchmarkInputWith(b, 2_048, 512, 4)
}

func newTypedTableAggregateDictionaryBenchmarkInputWith(b *testing.B, rows, teamCount, regionCount int) (*TypedTable, []TypedTableChange) {
	b.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "region", Kind: TypedTableString},
			{Name: "points", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	changes := make([]TypedTableChange, rows)
	for index := range changes {
		teamIndex := index % teamCount
		regionIndex := (index / teamCount) % regionCount
		changes[index] = TypedTableChange{
			Sequence: uint64(index + 1),
			After: []TypedTableValue{
				TypedString("team-" + strconv.Itoa(teamIndex)),
				TypedString("region-" + strconv.Itoa(regionIndex)),
				TypedInt64(int64(index)),
			},
		}
	}
	return table, changes
}

func typedTableAggregateGroupKeyBytes(aggregate *TypedTableAggregate) int {
	bytes := 0
	for _, bucket := range aggregate.groups {
		bytes += typedTableAggregateGroupKeyBytesForGroup(bucket.group)
		for _, collision := range bucket.collisions {
			bytes += typedTableAggregateGroupKeyBytesForGroup(collision)
		}
	}
	if aggregate.dictionaryEncodeGroups {
		bytes += len(aggregate.compactGroupOrder) * int(unsafe.Sizeof(typedTableAggregateGroupReference{}))
	} else {
		for _, bucket := range aggregate.groups {
			bytes += typedTableAggregateSortKeyBytes(bucket.group)
			for _, collision := range bucket.collisions {
				bytes += typedTableAggregateSortKeyBytes(collision)
			}
		}
	}
	for _, dictionary := range aggregate.groupDictionaries {
		bytes += len(dictionary.values) * int(unsafe.Sizeof(string("")))
		for index, value := range dictionary.values {
			if index < len(dictionary.counts) && dictionary.counts[index] > 0 {
				bytes += len(value)
			}
		}
	}
	return bytes
}

func typedTableAggregateGroupKeyBytesForGroup(group typedTableAggregateGroup) int {
	bytes := len(group.values)*int(unsafe.Sizeof(TypedTableValue{})) + len(group.codes)*int(unsafe.Sizeof(uint32(0)))
	for _, value := range group.values {
		if value.Valid && value.Kind == TypedTableString {
			bytes += int(unsafe.Sizeof(string(""))) + len(value.String)
		}
	}
	return bytes
}

func typedTableAggregateSortKeyBytes(group typedTableAggregateGroup) int {
	if group.key == "" {
		return 0
	}
	return int(unsafe.Sizeof(string(""))) + len(group.key)
}

func TestTypedTableAggregateDictionaryEncodedGroupsRemainExactAcrossDeletes(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "region", Kind: TypedTableString},
			{Name: "points", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	aggregate, err := NewTypedTableAggregate(table, TypedTableAggregateDefinition{
		GroupBy:                []string{"team", "region"},
		SumField:               "points",
		DictionaryEncodeGroups: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	changes := []TypedTableChange{
		{Sequence: 1, After: []TypedTableValue{TypedString("red"), TypedString("east"), TypedInt64(10)}},
		{Sequence: 2, After: []TypedTableValue{TypedString("blue"), TypedString("west"), TypedInt64(7)}},
		{Sequence: 3, After: []TypedTableValue{TypedString("blue"), TypedString("west"), TypedInt64(3)}},
		{Sequence: 4, Before: []TypedTableValue{TypedString("red"), TypedString("east"), TypedInt64(10)}},
		{Sequence: 5, After: []TypedTableValue{TypedString("green"), TypedString("north"), TypedInt64(2)}},
	}
	if err := aggregate.Apply(changes[:3]); err != nil {
		t.Fatal(err)
	}
	initialWant := []Row{
		{"team": "red", "region": "east", "count": int64(1), "sum": float64(10)},
		{"team": "blue", "region": "west", "count": int64(2), "sum": float64(10)},
	}
	if got := aggregate.Rows(); !reflect.DeepEqual(got, initialWant) {
		t.Fatalf("initial Rows() = %#v, want %#v", got, initialWant)
	}
	if err := aggregate.Apply(changes[3:]); err != nil {
		t.Fatal(err)
	}
	want := []Row{
		{"team": "blue", "region": "west", "count": int64(2), "sum": float64(10)},
		{"team": "green", "region": "north", "count": int64(1), "sum": float64(2)},
	}
	if got := aggregate.Rows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Rows() = %#v, want %#v", got, want)
	}
	if err := aggregate.Apply([]TypedTableChange{
		{Sequence: 6, After: []TypedTableValue{TypedString("blue"), TypedString("west"), TypedInt64(5)}},
	}); err != nil {
		t.Fatal(err)
	}
	want[0]["count"] = int64(3)
	want[0]["sum"] = float64(15)
	if got := aggregate.Rows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Rows() after cached-order update = %#v, want %#v", got, want)
	}

	for _, bucket := range aggregate.groups {
		if len(bucket.group.values) != 0 || len(bucket.group.codes) != 2 {
			t.Fatalf("compressed group storage = values:%d codes:%d, want values:0 codes:2", len(bucket.group.values), len(bucket.group.codes))
		}
		for _, group := range bucket.collisions {
			if len(group.values) != 0 || len(group.codes) != 2 {
				t.Fatalf("compressed collision storage = values:%d codes:%d, want values:0 codes:2", len(group.values), len(group.codes))
			}
		}
	}
}

func TestTypedTableAggregateDictionaryEncodedGroupsPreserveNullEmptyAndMixedKeys(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "shard", Kind: TypedTableInt64},
			{Name: "region", Kind: TypedTableString},
			{Name: "points", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	aggregate, err := NewTypedTableAggregate(table, TypedTableAggregateDefinition{
		GroupBy:                []string{"team", "shard", "region"},
		SumField:               "points",
		DictionaryEncodeGroups: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := aggregate.Apply([]TypedTableChange{
		{Sequence: 1, After: []TypedTableValue{TypedString(""), TypedInt64(1), TypedString("east"), TypedInt64(2)}},
		{Sequence: 2, After: []TypedTableValue{TypedNull(), TypedInt64(1), TypedString("east"), TypedInt64(3)}},
	}); err != nil {
		t.Fatal(err)
	}
	want := []Row{
		{"team": nil, "shard": int64(1), "region": "east", "count": int64(1), "sum": float64(3)},
		{"team": "", "shard": int64(1), "region": "east", "count": int64(1), "sum": float64(2)},
	}
	if got := aggregate.Rows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Rows() = %#v, want %#v", got, want)
	}
	legacy, err := NewTypedTableAggregate(table, TypedTableAggregateDefinition{GroupBy: []string{"team", "shard", "region"}, SumField: "points"})
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.Apply([]TypedTableChange{
		{Sequence: 1, After: []TypedTableValue{TypedString(""), TypedInt64(1), TypedString("east"), TypedInt64(2)}},
		{Sequence: 2, After: []TypedTableValue{TypedNull(), TypedInt64(1), TypedString("east"), TypedInt64(3)}},
	}); err != nil {
		t.Fatal(err)
	}
	if got, want := aggregate.Rows(), legacy.Rows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("compact Rows() = %#v, legacy Rows() = %#v", got, want)
	}
	if len(aggregate.groupDictionaries) != 2 || aggregate.groupValueCount != 1 {
		t.Fatalf("group storage metadata = dictionaries:%d values:%d, want dictionaries:2 values:1", len(aggregate.groupDictionaries), aggregate.groupValueCount)
	}
}

func TestTypedTableAggregateDictionaryEncodedGroupsRemapPartialCodes(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "region", Kind: TypedTableString},
			{Name: "points", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	definition := TypedTableAggregateDefinition{GroupBy: []string{"team", "region"}, SumField: "points", DictionaryEncodeGroups: true}
	target, err := NewTypedTableAggregate(table, definition)
	if err != nil {
		t.Fatal(err)
	}
	partial, err := NewTypedTableAggregate(table, definition)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Apply([]TypedTableChange{{Sequence: 1, After: []TypedTableValue{TypedString("alpha"), TypedString("east"), TypedInt64(2)}}}); err != nil {
		t.Fatal(err)
	}
	if err := partial.Apply([]TypedTableChange{
		{Sequence: 1, After: []TypedTableValue{TypedString("beta"), TypedString("west"), TypedInt64(4)}},
		{Sequence: 2, After: []TypedTableValue{TypedString("alpha"), TypedString("east"), TypedInt64(3)}},
	}); err != nil {
		t.Fatal(err)
	}
	targetCode, targetFound := target.groupDictionaries[0].lookup("alpha")
	partialCode, partialFound := partial.groupDictionaries[0].lookup("alpha")
	if !targetFound || !partialFound || targetCode == partialCode {
		t.Fatal("test input did not create distinct team dictionary codes")
	}
	if err := target.MergePartial(partial); err != nil {
		t.Fatal(err)
	}
	want := []Row{
		{"team": "beta", "region": "west", "count": int64(1), "sum": float64(4)},
		{"team": "alpha", "region": "east", "count": int64(2), "sum": float64(5)},
	}
	if got := target.Rows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Rows() = %#v, want %#v", got, want)
	}
	legacy, err := NewTypedTableAggregate(table, TypedTableAggregateDefinition{GroupBy: []string{"team", "region"}, SumField: "points"})
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.MergePartial(target); err != ErrTypedTableAggregatePartialDefinition {
		t.Fatalf("legacy.MergePartial(compact) error = %v, want %v", err, ErrTypedTableAggregatePartialDefinition)
	}
}

func TestTypedTableAggregateArrangementsSeparateDictionaryEncoding(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "team", Kind: TypedTableString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	arrangements, err := NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := arrangements.Acquire(TypedTableAggregateDefinition{GroupBy: []string{"team"}})
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Release()
	compact, err := arrangements.Acquire(TypedTableAggregateDefinition{GroupBy: []string{"team"}, DictionaryEncodeGroups: true})
	if err != nil {
		t.Fatal(err)
	}
	defer compact.Release()
	if arrangements.Active() != 2 {
		t.Fatalf("Active() = %d, want 2", arrangements.Active())
	}
	snapshot := arrangements.Snapshot()
	if len(snapshot) != 2 || snapshot[0].Definition.DictionaryEncodeGroups || !snapshot[1].Definition.DictionaryEncodeGroups {
		t.Fatalf("Snapshot() = %#v, want one legacy and one compact definition", snapshot)
	}
}
