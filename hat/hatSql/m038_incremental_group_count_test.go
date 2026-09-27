package hatSql

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func m038GroupKey(row SQLRow) string {
	return row["group"].(string)
}

func m038InitialRows(count int) []DifferentialRow {
	rows := make([]DifferentialRow, count)
	for index := range rows {
		rows[index] = DifferentialRow{
			Key:  fmt.Sprintf("row-%d", index),
			Time: 1,
			Diff: 1,
			Row:  Row{"group": fmt.Sprintf("group-%d", index%100)},
		}
	}
	return rows
}

func BenchmarkM038RebuildGroupCount(b *testing.B) {
	updates := m038InitialRows(10_000)
	updates = append(updates, DifferentialRow{
		Key:  "new-row",
		Time: 2,
		Diff: 1,
		Row:  Row{"group": "group-42"},
	})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := GroupCountDifferentialRows(updates, m038GroupKey); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM038IncrementalGroupCount(b *testing.B) {
	operator, err := NewIncrementalGroupCount(m038GroupKey)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := operator.Apply(m038InitialRows(10_000)); err != nil {
		b.Fatal(err)
	}
	update := []DifferentialRow{{
		Key:  "new-row",
		Time: 2,
		Diff: 1,
		Row:  Row{"group": "group-42"},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := operator.Apply(update); err != nil {
			b.Fatal(err)
		}
	}
}

func TestM038IncrementalGroupCountApplyTransitions(t *testing.T) {
	operator, err := NewIncrementalGroupCount(m038GroupKey)
	if err != nil {
		t.Fatalf("NewIncrementalGroupCount() error = %v", err)
	}

	got, err := operator.Apply([]DifferentialRow{{
		Key:  "row-a",
		Time: 1,
		Diff: 1,
		Row:  Row{"group": "red"},
	}})
	if err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	want := []DifferentialRow{{Key: "red", Time: 1, Diff: 1, Row: Row{"count": int64(1)}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("initial Apply() = %#v, want %#v", got, want)
	}

	got, err = operator.Apply([]DifferentialRow{{
		Key:  "row-b",
		Time: 2,
		Diff: 1,
		Row:  Row{"group": "red"},
	}})
	if err != nil {
		t.Fatalf("incrementing Apply() error = %v", err)
	}
	want = []DifferentialRow{
		{Key: "red", Time: 2, Diff: -1, Row: Row{"count": int64(1)}},
		{Key: "red", Time: 2, Diff: 1, Row: Row{"count": int64(2)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("incrementing Apply() = %#v, want %#v", got, want)
	}

	got, err = operator.Apply([]DifferentialRow{{
		Key:  "row-a",
		Time: 3,
		Diff: -1,
		Row:  Row{"group": "red"},
	}})
	if err != nil {
		t.Fatalf("retraction Apply() error = %v", err)
	}
	want = []DifferentialRow{
		{Key: "red", Time: 3, Diff: -1, Row: Row{"count": int64(2)}},
		{Key: "red", Time: 3, Diff: 1, Row: Row{"count": int64(1)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("retraction Apply() = %#v, want %#v", got, want)
	}

	got, err = operator.Apply([]DifferentialRow{{
		Key:  "row-b",
		Time: 4,
		Diff: -1,
		Row:  Row{"group": "red"},
	}})
	if err != nil {
		t.Fatalf("last retraction Apply() error = %v", err)
	}
	want = []DifferentialRow{{Key: "red", Time: 4, Diff: -1, Row: Row{"count": int64(1)}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("last retraction Apply() = %#v, want %#v", got, want)
	}

	if snapshot := operator.Snapshot(); snapshot != nil {
		t.Fatalf("Snapshot() = %#v, want nil", snapshot)
	}
}

func TestM038IncrementalGroupCountBatchIsAtomicAndCoalesces(t *testing.T) {
	operator, err := NewIncrementalGroupCount(m038GroupKey)
	if err != nil {
		t.Fatalf("NewIncrementalGroupCount() error = %v", err)
	}
	if _, err := operator.Apply([]DifferentialRow{{
		Key:  "row-a",
		Time: 1,
		Diff: 1,
		Row:  Row{"group": "red"},
	}}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}

	got, err := operator.Apply([]DifferentialRow{
		{Key: "row-b", Time: 2, Diff: 1, Row: Row{"group": "blue"}},
		{Key: "row-b", Time: 2, Diff: -1, Row: Row{"group": "blue"}},
	})
	if err != nil {
		t.Fatalf("coalesced Apply() error = %v", err)
	}
	if got != nil {
		t.Fatalf("coalesced Apply() = %#v, want nil", got)
	}

	_, err = operator.Apply([]DifferentialRow{
		{Key: "row-c", Time: 3, Diff: 1, Row: Row{"group": "green"}},
		{Key: "row-a", Time: 3, Diff: -2, Row: Row{"group": "red"}},
	})
	if !errors.Is(err, ErrDifferentialGroupByNegativeCount) {
		t.Fatalf("invalid batch error = %v, want ErrDifferentialGroupByNegativeCount", err)
	}
	want := []DifferentialRow{{Key: "red", Time: 0, Diff: 1, Row: Row{"count": int64(1)}}}
	if snapshot := operator.Snapshot(); !reflect.DeepEqual(snapshot, want) {
		t.Fatalf("Snapshot() after rejected batch = %#v, want %#v", snapshot, want)
	}
}

func TestM038IncrementalGroupCountValidatesAndSnapshotsDeterministically(t *testing.T) {
	if _, err := NewIncrementalGroupCount(nil); !errors.Is(err, ErrDifferentialGroupByKeyRequired) {
		t.Fatalf("nil key callback error = %v, want ErrDifferentialGroupByKeyRequired", err)
	}

	operator, err := NewIncrementalGroupCount(m038GroupKey)
	if err != nil {
		t.Fatalf("NewIncrementalGroupCount() error = %v", err)
	}
	if _, err := operator.Apply([]DifferentialRow{{Key: "row-a", Diff: -1, Row: Row{"group": "red"}}}); !errors.Is(err, ErrDifferentialGroupByNegativeCount) {
		t.Fatalf("negative count error = %v, want ErrDifferentialGroupByNegativeCount", err)
	}

	if _, err := operator.Apply([]DifferentialRow{
		{Key: "row-z", Time: 1, Diff: 1, Row: Row{"group": "z"}},
		{Key: "row-a", Time: 1, Diff: 1, Row: Row{"group": "a"}},
	}); err != nil {
		t.Fatalf("seed groups Apply() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Diff: 1, Row: Row{"count": int64(1)}},
		{Key: "z", Diff: 1, Row: Row{"count": int64(1)}},
	}
	if snapshot := operator.Snapshot(); !reflect.DeepEqual(snapshot, want) {
		t.Fatalf("Snapshot() = %#v, want %#v", snapshot, want)
	}
}

func TestM038IncrementalGroupCountRejectsOverflow(t *testing.T) {
	operator, err := NewIncrementalGroupCount(m038GroupKey)
	if err != nil {
		t.Fatalf("NewIncrementalGroupCount() error = %v", err)
	}
	if _, err := operator.Apply([]DifferentialRow{{
		Key:  "row-a",
		Diff: int64(^uint64(0) >> 1),
		Row:  Row{"group": "red"},
	}}); err != nil {
		t.Fatalf("max-count Apply() error = %v", err)
	}
	if _, err := operator.Apply([]DifferentialRow{{
		Key:  "row-b",
		Diff: 1,
		Row:  Row{"group": "red"},
	}}); !errors.Is(err, ErrDifferentialGroupByCountOverflow) {
		t.Fatalf("overflow error = %v, want ErrDifferentialGroupByCountOverflow", err)
	}
}
