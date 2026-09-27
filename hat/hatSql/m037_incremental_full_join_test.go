package hatSql

import (
	"errors"
	"reflect"
	"sort"
	"strconv"
	"testing"
)

func TestM037IncrementalFullJoinMaintainsBothUnmatchedBoundaries(t *testing.T) {
	join := newM037FullJoinForTest(t)

	got, err := join.Apply([]IncrementalJoinUpdate{
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left-1", Time: 1, Diff: 1, Row: Row{"id": "l1", "group": "a"}}},
		{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "right-2", Time: 2, Diff: 1, Row: Row{"id": "r2", "group": "b"}}},
	})
	if err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	assertM037FullJoinRows(t, got, []DifferentialRow{
		{Key: "\x01left-1", Time: 1, Diff: 1, Row: Row{"kind": "left-unmatched", "id": "l1"}},
		{Key: "\x02right-2", Time: 2, Diff: 1, Row: Row{"kind": "right-unmatched", "id": "r2"}},
	})

	got, err = join.Apply([]IncrementalJoinUpdate{{
		Side: IncrementalJoinRight,
		Row:  DifferentialRow{Key: "right-1", Time: 3, Diff: 1, Row: Row{"id": "r1", "group": "a"}},
	}})
	if err != nil {
		t.Fatalf("matching right Apply() error = %v", err)
	}
	assertM037FullJoinRows(t, got, []DifferentialRow{
		{Key: "left-1\x00right-1", Time: 3, Diff: 1, Row: Row{"kind": "matched", "left": "l1", "right": "r1"}},
		{Key: "\x01left-1", Time: 1, Diff: -1, Row: Row{"kind": "left-unmatched", "id": "l1"}},
	})

	got, err = join.Apply([]IncrementalJoinUpdate{{
		Side: IncrementalJoinLeft,
		Row:  DifferentialRow{Key: "left-1", Time: 4, Diff: -1},
	}})
	if err != nil {
		t.Fatalf("matching left retraction Apply() error = %v", err)
	}
	assertM037FullJoinRows(t, got, []DifferentialRow{
		{Key: "left-1\x00right-1", Time: 3, Diff: -1, Row: Row{"kind": "matched", "left": "l1", "right": "r1"}},
		{Key: "\x02right-1", Time: 3, Diff: 1, Row: Row{"kind": "right-unmatched", "id": "r1"}},
	})

	got, err = join.Apply([]IncrementalJoinUpdate{{
		Side: IncrementalJoinRight,
		Row:  DifferentialRow{Key: "right-1", Time: 5, Diff: -1},
	}})
	if err != nil {
		t.Fatalf("right unmatched retraction Apply() error = %v", err)
	}
	assertM037FullJoinRows(t, got, []DifferentialRow{
		{Key: "\x02right-1", Time: 3, Diff: -1, Row: Row{"kind": "right-unmatched", "id": "r1"}},
	})
}

func TestM037IncrementalFullJoinSnapshotIsExact(t *testing.T) {
	join := newM037FullJoinForTest(t)
	_, err := join.Apply([]IncrementalJoinUpdate{
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left-1", Time: 1, Diff: 2, Row: Row{"id": "l1", "group": "a"}}},
		{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "right-1", Time: 2, Diff: 3, Row: Row{"id": "r1", "group": "a"}}},
	})
	if err != nil {
		t.Fatalf("weighted Apply() error = %v", err)
	}
	got, err := join.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	assertM037FullJoinRows(t, got, []DifferentialRow{{
		Key: "left-1\x00right-1", Time: 2, Diff: 6,
		Row: Row{"kind": "matched", "left": "l1", "right": "r1"},
	}})
}

func TestM037IncrementalFullJoinKeepsStateWhenUnmatchedCallbackFails(t *testing.T) {
	callbackErr := errors.New("left unmatched projection failed")
	failUnmatched := false
	join, err := NewIncrementalFullJoin(IncrementalFullJoinDefinition{
		LeftKey:  func(row Row) (string, error) { return row["group"].(string), nil },
		RightKey: func(row Row) (string, error) { return row["group"].(string), nil },
		Merge: func(left, right Row) (Row, error) {
			return Row{"left": left["id"], "right": right["id"]}, nil
		},
		LeftUnmatched: func(left Row) (Row, error) {
			if failUnmatched && left["id"] == "bad" {
				return nil, callbackErr
			}
			return Row{"left": left["id"]}, nil
		},
		RightUnmatched: func(right Row) (Row, error) {
			return Row{"right": right["id"]}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewIncrementalFullJoin() error = %v", err)
	}
	_, err = join.Apply([]IncrementalJoinUpdate{{
		Side: IncrementalJoinLeft,
		Row:  DifferentialRow{Key: "left-bad", Time: 1, Diff: 1, Row: Row{"id": "bad", "group": "a"}},
	}})
	if err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	before, err := join.Snapshot()
	if err != nil {
		t.Fatalf("seed Snapshot() error = %v", err)
	}
	failUnmatched = true
	_, err = join.Apply([]IncrementalJoinUpdate{{
		Side: IncrementalJoinRight,
		Row:  DifferentialRow{Key: "right-a", Time: 2, Diff: 1, Row: Row{"id": "r1", "group": "a"}},
	}})
	if !errors.Is(err, callbackErr) {
		t.Fatalf("Apply() error = %v, want callback error", err)
	}
	failUnmatched = false
	after, err := join.Snapshot()
	if err != nil {
		t.Fatalf("failed Snapshot() error = %v", err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("state changed after callback failure: got %#v, want %#v", after, before)
	}
}

func newM037FullJoinForTest(t *testing.T) *IncrementalFullJoin {
	t.Helper()
	join, err := newM037FullJoinForBenchmarkWithError()
	if err != nil {
		t.Fatalf("NewIncrementalFullJoin() error = %v", err)
	}
	return join
}

func newM037FullJoinForBenchmark() *IncrementalFullJoin {
	join, err := newM037FullJoinForBenchmarkWithError()
	if err != nil {
		panic(err)
	}
	return join
}

func newM037FullJoinForBenchmarkWithError() (*IncrementalFullJoin, error) {
	return NewIncrementalFullJoin(IncrementalFullJoinDefinition{
		LeftKey:  func(row Row) (string, error) { return row["group"].(string), nil },
		RightKey: func(row Row) (string, error) { return row["group"].(string), nil },
		Merge: func(left, right Row) (Row, error) {
			return Row{"kind": "matched", "left": left["id"], "right": right["id"]}, nil
		},
		LeftUnmatched: func(left Row) (Row, error) {
			return Row{"kind": "left-unmatched", "id": left["id"]}, nil
		},
		RightUnmatched: func(right Row) (Row, error) {
			return Row{"kind": "right-unmatched", "id": right["id"]}, nil
		},
	})
}

func assertM037FullJoinRows(t *testing.T, got, want []DifferentialRow) {
	t.Helper()
	sort.Slice(got, func(left, right int) bool { return got[left].Key < got[right].Key })
	sort.Slice(want, func(left, right int) bool { return want[left].Key < want[right].Key })
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %#v, want %#v", got, want)
	}
}

func BenchmarkM037RebuildFullOuterJoin(b *testing.B) {
	left, right := newM037FullJoinBenchmarkRows()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if index%2 == 0 {
			_ = rebuildM037FullOuterJoin(left, right)
		} else {
			_ = rebuildM037FullOuterJoin(left, nil)
		}
	}
}

func BenchmarkM037IncrementalFullJoin(b *testing.B) {
	left, right := newM037FullJoinBenchmarkRows()
	join := newM037FullJoinForBenchmark()
	seed := make([]IncrementalJoinUpdate, len(left))
	for index, row := range left {
		seed[index] = IncrementalJoinUpdate{Side: IncrementalJoinLeft, Row: row}
	}
	if _, err := join.Apply(seed); err != nil {
		b.Fatal(err)
	}
	insert := IncrementalJoinUpdate{Side: IncrementalJoinRight, Row: right[0]}
	remove := insert
	remove.Row.Diff = -1
	remove.Row.Row = nil
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		update := insert
		if index%2 != 0 {
			update = remove
		}
		if _, err := join.Apply([]IncrementalJoinUpdate{update}); err != nil {
			b.Fatal(err)
		}
	}
}

func newM037FullJoinBenchmarkRows() ([]DifferentialRow, []DifferentialRow) {
	left := make([]DifferentialRow, 10_000)
	for index := range left {
		left[index] = DifferentialRow{
			Key:  "left-" + m037FullJoinBenchmarkID(index),
			Time: 1,
			Diff: 1,
			Row:  Row{"id": m037FullJoinBenchmarkID(index), "group": "g" + m037FullJoinBenchmarkID(index%10)},
		}
	}
	right := []DifferentialRow{{
		Key:  "right-1",
		Time: 2,
		Diff: 1,
		Row:  Row{"id": "right-1", "group": "g5"},
	}}
	return left, right
}

func m037FullJoinBenchmarkID(value int) string {
	return strconv.Itoa(value)
}

func rebuildM037FullOuterJoin(left, right []DifferentialRow) []DifferentialRow {
	leftByGroup := make(map[string][]DifferentialRow, len(left))
	rightByGroup := make(map[string][]DifferentialRow, len(right))
	for _, row := range left {
		group := row.Row["group"].(string)
		leftByGroup[group] = append(leftByGroup[group], row)
	}
	for _, row := range right {
		group := row.Row["group"].(string)
		rightByGroup[group] = append(rightByGroup[group], row)
	}
	result := make([]DifferentialRow, 0, len(left)+len(right))
	for _, leftRow := range left {
		group := leftRow.Row["group"].(string)
		matches := rightByGroup[group]
		if len(matches) == 0 {
			result = append(result, DifferentialRow{
				Key:  "left-unmatched:" + leftRow.Key,
				Time: leftRow.Time,
				Diff: leftRow.Diff,
				Row:  Row{"left": leftRow.Row["id"], "right": nil},
			})
			continue
		}
		for _, rightRow := range matches {
			result = append(result, DifferentialRow{
				Key:  leftRow.Key + "\x00" + rightRow.Key,
				Time: rightRow.Time,
				Diff: leftRow.Diff * rightRow.Diff,
				Row:  Row{"left": leftRow.Row["id"], "right": rightRow.Row["id"]},
			})
		}
	}
	for _, rightRow := range right {
		group := rightRow.Row["group"].(string)
		if len(leftByGroup[group]) != 0 {
			continue
		}
		result = append(result, DifferentialRow{
			Key:  "right-unmatched:" + rightRow.Key,
			Time: rightRow.Time,
			Diff: rightRow.Diff,
			Row:  Row{"left": nil, "right": rightRow.Row["id"]},
		})
	}
	return result
}
