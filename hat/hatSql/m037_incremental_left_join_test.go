package hatSql

import (
	"reflect"
	"sort"
	"strconv"
	"testing"
)

func TestM037IncrementalLeftJoinMaintainsMatchedAndUnmatchedDeltas(t *testing.T) {
	join := newM037IncrementalLeftJoin(t)
	seed := []IncrementalJoinUpdate{
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "l1", Diff: 1, Row: Row{"id": "l1", "group": "a"}}},
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "l2", Diff: 1, Row: Row{"id": "l2", "group": "b"}}},
	}
	got, err := join.Apply(seed)
	if err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	if want := []DifferentialRow{
		{Key: incrementalLeftJoinUnmatchedKey("l1"), Diff: 1, Row: Row{"left": "l1", "right": nil}},
		{Key: incrementalLeftJoinUnmatchedKey("l2"), Diff: 1, Row: Row{"left": "l2", "right": nil}},
	}; !reflect.DeepEqual(m037CanonicalDifferentialRows(got), m037CanonicalDifferentialRows(want)) {
		t.Fatalf("seed deltas = %#v, want %#v", got, want)
	}

	got, err = join.Apply([]IncrementalJoinUpdate{{
		Side: IncrementalJoinRight,
		Row:  DifferentialRow{Key: "r1", Diff: 1, Row: Row{"id": "r1", "group": "a"}},
	}})
	if err != nil {
		t.Fatalf("right insert Apply() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: incrementalJoinPairKey("l1", "r1"), Diff: 1, Row: Row{"left": "l1", "right": "r1"}},
		{Key: incrementalLeftJoinUnmatchedKey("l1"), Diff: -1, Row: Row{"left": "l1", "right": nil}},
	}
	if !reflect.DeepEqual(m037CanonicalDifferentialRows(got), m037CanonicalDifferentialRows(want)) {
		t.Fatalf("right insert deltas = %#v, want %#v", got, want)
	}

	got, err = join.Apply([]IncrementalJoinUpdate{{
		Side: IncrementalJoinRight,
		Row:  DifferentialRow{Key: "r1", Diff: -1},
	}})
	if err != nil {
		t.Fatalf("right delete Apply() error = %v", err)
	}
	want = []DifferentialRow{
		{Key: incrementalJoinPairKey("l1", "r1"), Diff: -1, Row: Row{"left": "l1", "right": "r1"}},
		{Key: incrementalLeftJoinUnmatchedKey("l1"), Diff: 1, Row: Row{"left": "l1", "right": nil}},
	}
	if !reflect.DeepEqual(m037CanonicalDifferentialRows(got), m037CanonicalDifferentialRows(want)) {
		t.Fatalf("right delete deltas = %#v, want %#v", got, want)
	}

	snapshot, err := join.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	want = []DifferentialRow{
		{Key: incrementalLeftJoinUnmatchedKey("l1"), Diff: 1, Row: Row{"left": "l1", "right": nil}},
		{Key: incrementalLeftJoinUnmatchedKey("l2"), Diff: 1, Row: Row{"left": "l2", "right": nil}},
	}
	if !reflect.DeepEqual(m037CanonicalDifferentialRows(snapshot), m037CanonicalDifferentialRows(want)) {
		t.Fatalf("snapshot = %#v, want %#v", snapshot, want)
	}
}

func TestM037IncrementalLeftJoinPreservesWeightedMultiplicityAndAtomicity(t *testing.T) {
	join := newM037IncrementalLeftJoin(t)
	if _, err := join.Apply([]IncrementalJoinUpdate{{
		Side: IncrementalJoinLeft,
		Row:  DifferentialRow{Key: "l1", Diff: 2, Row: Row{"id": "l1", "group": "a"}},
	}}); err != nil {
		t.Fatal(err)
	}
	got, err := join.Apply([]IncrementalJoinUpdate{{
		Side: IncrementalJoinRight,
		Row:  DifferentialRow{Key: "r1", Diff: 3, Row: Row{"id": "r1", "group": "a"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	want := []DifferentialRow{
		{Key: incrementalJoinPairKey("l1", "r1"), Diff: 6, Row: Row{"left": "l1", "right": "r1"}},
		{Key: incrementalLeftJoinUnmatchedKey("l1"), Diff: -2, Row: Row{"left": "l1", "right": nil}},
	}
	if !reflect.DeepEqual(m037CanonicalDifferentialRows(got), m037CanonicalDifferentialRows(want)) {
		t.Fatalf("weighted deltas = %#v, want %#v", got, want)
	}

	before, err := join.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := join.Apply([]IncrementalJoinUpdate{{
		Side: IncrementalJoinRight,
		Row:  DifferentialRow{Key: "missing", Diff: -1},
	}}); err == nil {
		t.Fatal("missing right retraction unexpectedly succeeded")
	}
	after, err := join.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("snapshot changed after rejected batch: before %#v, after %#v", before, after)
	}
}

func BenchmarkM037IncrementalLeftJoin(b *testing.B) {
	join := newM037BenchmarkLeftJoin(b, 10000)
	rightInsert := IncrementalJoinUpdate{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "r1", Diff: 1, Row: Row{"id": "r1", "group": "g0005"}}}
	rightDelete := IncrementalJoinUpdate{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "r1", Diff: -1}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := join.Apply([]IncrementalJoinUpdate{rightInsert}); err != nil {
			b.Fatal(err)
		}
		if _, err := join.Apply([]IncrementalJoinUpdate{rightDelete}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM037RebuildLeftOuterJoin(b *testing.B) {
	left := m037BenchmarkLeftRows(10000)
	right := Row{"id": "r1", "group": "g0005"}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		m037RebuildLeftOuterJoin(left, []Row{right})
		m037RebuildLeftOuterJoin(left, nil)
	}
}

func newM037IncrementalLeftJoin(t testing.TB) *IncrementalLeftJoin {
	t.Helper()
	join, err := NewIncrementalLeftJoin(IncrementalLeftJoinDefinition{
		LeftKey:  func(row Row) (string, error) { return row["group"].(string), nil },
		RightKey: func(row Row) (string, error) { return row["group"].(string), nil },
		Merge: func(left, right Row) (Row, error) {
			return Row{"left": left["id"], "right": right["id"]}, nil
		},
		Unmatched: func(left Row) (Row, error) {
			return Row{"left": left["id"], "right": nil}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return join
}

func m037CanonicalDifferentialRows(rows []DifferentialRow) []DifferentialRow {
	result := append([]DifferentialRow(nil), rows...)
	sort.Slice(result, func(left, right int) bool {
		if result[left].Key != result[right].Key {
			return result[left].Key < result[right].Key
		}
		return result[left].Diff < result[right].Diff
	})
	return result
}

func newM037BenchmarkLeftJoin(b testing.TB, count int) *IncrementalLeftJoin {
	b.Helper()
	join, err := NewIncrementalLeftJoin(IncrementalLeftJoinDefinition{
		LeftKey:  func(row Row) (string, error) { return row["group"].(string), nil },
		RightKey: func(row Row) (string, error) { return row["group"].(string), nil },
		Merge: func(left, right Row) (Row, error) {
			return Row{"left": left["id"], "right": right["id"]}, nil
		},
		Unmatched: func(left Row) (Row, error) {
			return Row{"left": left["id"], "right": nil}, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	updates := make([]IncrementalJoinUpdate, count)
	for index, row := range m037BenchmarkLeftRows(count) {
		updates[index] = IncrementalJoinUpdate{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: row["id"].(string), Diff: 1, Row: row}}
	}
	if _, err := join.Apply(updates); err != nil {
		b.Fatal(err)
	}
	return join
}

func m037BenchmarkLeftRows(count int) []Row {
	rows := make([]Row, count)
	for index := range rows {
		rows[index] = Row{"id": "l" + strconv.Itoa(index), "group": "g" + strconv.Itoa(index%1000)}
	}
	return rows
}

func m037RebuildLeftOuterJoin(left []Row, right []Row) []Row {
	matched := make(map[string]struct{}, len(right))
	for _, row := range right {
		matched[row["group"].(string)] = struct{}{}
	}
	result := make([]Row, 0, len(left))
	for _, row := range left {
		if _, ok := matched[row["group"].(string)]; ok {
			result = append(result, Row{"left": row["id"], "right": "r1"})
			continue
		}
		result = append(result, Row{"left": row["id"], "right": nil})
	}
	return result
}
