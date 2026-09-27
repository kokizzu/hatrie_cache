package hatSql

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func m039GroupCountDistinctKey(row SQLRow) string {
	return row["group"].(string)
}

func m039GroupCountDistinctValue(row SQLRow) (int64, error) {
	return row["value"].(int64), nil
}

func m039GroupCountDistinctInitialRows(count int) []DifferentialRow {
	rows := make([]DifferentialRow, count)
	for index := range rows {
		rows[index] = DifferentialRow{
			Key:  fmt.Sprintf("row-%d", index),
			Time: 1,
			Diff: 1,
			Row: Row{
				"group": fmt.Sprintf("group-%d", index%100),
				"value": int64(index / 100),
			},
		}
	}
	return rows
}

func BenchmarkM039RebuildGroupCountDistinct(b *testing.B) {
	updates := m039GroupCountDistinctInitialRows(10_000)
	updates = append(updates, DifferentialRow{
		Key:  "remove-row",
		Time: 2,
		Diff: -1,
		Row: Row{
			"group": "group-42",
			"value": int64(0),
		},
	}, DifferentialRow{
		Key:  "add-row",
		Time: 2,
		Diff: 1,
		Row: Row{
			"group": "group-42",
			"value": int64(100),
		},
	})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := GroupCountDistinctInt64DifferentialRows(updates, m039GroupCountDistinctKey, m039GroupCountDistinctValue); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM039IncrementalGroupCountDistinct(b *testing.B) {
	operator, err := NewIncrementalGroupCountDistinctInt64(m039GroupCountDistinctKey, m039GroupCountDistinctValue)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := operator.Apply(m039GroupCountDistinctInitialRows(10_000)); err != nil {
		b.Fatal(err)
	}
	removeZero := []DifferentialRow{{
		Key:  "remove-row",
		Time: 2,
		Diff: -1,
		Row: Row{
			"group": "group-42",
			"value": int64(0),
		},
	}, {
		Key:  "add-row",
		Time: 2,
		Diff: 1,
		Row: Row{
			"group": "group-42",
			"value": int64(100),
		},
	}}
	removeHundred := []DifferentialRow{{
		Key:  "remove-row",
		Time: 2,
		Diff: -1,
		Row: Row{
			"group": "group-42",
			"value": int64(100),
		},
	}, {
		Key:  "add-row",
		Time: 2,
		Diff: 1,
		Row: Row{
			"group": "group-42",
			"value": int64(0),
		},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := range b.N {
		updates := removeZero
		if index%2 != 0 {
			updates = removeHundred
		}
		if _, err := operator.Apply(updates); err != nil {
			b.Fatal(err)
		}
	}
}

func TestM039IncrementalGroupCountDistinctApplyTransitions(t *testing.T) {
	operator, err := NewIncrementalGroupCountDistinctInt64(m039GroupCountDistinctKey, m039GroupCountDistinctValue)
	if err != nil {
		t.Fatalf("NewIncrementalGroupCountDistinctInt64() error = %v", err)
	}

	got, err := operator.Apply([]DifferentialRow{{
		Key:  "row-a",
		Time: 1,
		Diff: 1,
		Row:  Row{"group": "red", "value": int64(5)},
	}})
	if err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	want := []DifferentialRow{{Key: "red", Time: 1, Diff: 1, Row: Row{"count_distinct": int64(1)}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("initial Apply() = %#v, want %#v", got, want)
	}

	if got, err := operator.Apply([]DifferentialRow{{
		Key:  "row-b",
		Time: 2,
		Diff: 1,
		Row:  Row{"group": "red", "value": int64(5)},
	}}); err != nil || got != nil {
		t.Fatalf("duplicate Apply() = %#v, error %v, want nil output and nil error", got, err)
	}

	got, err = operator.Apply([]DifferentialRow{{
		Key:  "row-c",
		Time: 3,
		Diff: 1,
		Row:  Row{"group": "red", "value": int64(7)},
	}})
	if err != nil {
		t.Fatalf("second-value Apply() error = %v", err)
	}
	want = []DifferentialRow{
		{Key: "red", Time: 3, Diff: -1, Row: Row{"count_distinct": int64(1)}},
		{Key: "red", Time: 3, Diff: 1, Row: Row{"count_distinct": int64(2)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("second-value Apply() = %#v, want %#v", got, want)
	}

	if got, err := operator.Apply([]DifferentialRow{{
		Key:  "row-a",
		Time: 4,
		Diff: -1,
		Row:  Row{"group": "red", "value": int64(5)},
	}}); err != nil || got != nil {
		t.Fatalf("first duplicate retraction = %#v, error %v, want nil output and nil error", got, err)
	}

	got, err = operator.Apply([]DifferentialRow{{
		Key:  "row-b",
		Time: 5,
		Diff: -1,
		Row:  Row{"group": "red", "value": int64(5)},
	}})
	if err != nil {
		t.Fatalf("value disappearance Apply() error = %v", err)
	}
	want = []DifferentialRow{
		{Key: "red", Time: 5, Diff: -1, Row: Row{"count_distinct": int64(2)}},
		{Key: "red", Time: 5, Diff: 1, Row: Row{"count_distinct": int64(1)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("value disappearance Apply() = %#v, want %#v", got, want)
	}

	got, err = operator.Apply([]DifferentialRow{{
		Key:  "row-c",
		Time: 6,
		Diff: -1,
		Row:  Row{"group": "red", "value": int64(7)},
	}})
	if err != nil {
		t.Fatalf("last value Apply() error = %v", err)
	}
	want = []DifferentialRow{{Key: "red", Time: 6, Diff: -1, Row: Row{"count_distinct": int64(1)}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("last value Apply() = %#v, want %#v", got, want)
	}
	if snapshot := operator.Snapshot(); snapshot != nil {
		t.Fatalf("Snapshot() = %#v, want nil", snapshot)
	}
}

func TestM039IncrementalGroupCountDistinctBatchIsAtomic(t *testing.T) {
	operator, err := NewIncrementalGroupCountDistinctInt64(m039GroupCountDistinctKey, m039GroupCountDistinctValue)
	if err != nil {
		t.Fatalf("NewIncrementalGroupCountDistinctInt64() error = %v", err)
	}
	if _, err := operator.Apply([]DifferentialRow{
		{Key: "row-a", Time: 1, Diff: 1, Row: Row{"group": "red", "value": int64(5)}},
		{Key: "row-b", Time: 1, Diff: 1, Row: Row{"group": "red", "value": int64(7)}},
	}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}

	_, err = operator.Apply([]DifferentialRow{
		{Key: "row-c", Time: 2, Diff: 1, Row: Row{"group": "blue", "value": int64(9)}},
		{Key: "row-a", Time: 2, Diff: -2, Row: Row{"group": "red", "value": int64(5)}},
	})
	if !errors.Is(err, ErrDifferentialGroupByDistinctValueMultiplicity) {
		t.Fatalf("invalid value multiplicity error = %v, want ErrDifferentialGroupByDistinctValueMultiplicity", err)
	}
	want := []DifferentialRow{{Key: "red", Diff: 1, Row: Row{"count_distinct": int64(2)}}}
	if snapshot := operator.Snapshot(); !reflect.DeepEqual(snapshot, want) {
		t.Fatalf("Snapshot() after rejected batch = %#v, want %#v", snapshot, want)
	}

	if _, err := operator.Apply([]DifferentialRow{{Key: "row-a", Diff: -3, Row: Row{"group": "red", "value": int64(5)}}}); !errors.Is(err, ErrDifferentialGroupByNegativeCount) {
		t.Fatalf("negative group count error = %v, want ErrDifferentialGroupByNegativeCount", err)
	}
}

func TestM039IncrementalGroupCountDistinctValidatesAndSnapshotsDeterministically(t *testing.T) {
	if _, err := NewIncrementalGroupCountDistinctInt64(nil, m039GroupCountDistinctValue); !errors.Is(err, ErrDifferentialGroupByKeyRequired) {
		t.Fatalf("nil key callback error = %v, want ErrDifferentialGroupByKeyRequired", err)
	}
	if _, err := NewIncrementalGroupCountDistinctInt64(m039GroupCountDistinctKey, nil); !errors.Is(err, ErrDifferentialGroupByValueRequired) {
		t.Fatalf("nil value callback error = %v, want ErrDifferentialGroupByValueRequired", err)
	}

	callbackErr := errors.New("bad distinct value")
	failingValue := func(row SQLRow) (int64, error) {
		if row["bad"] == true {
			return 0, callbackErr
		}
		return row["value"].(int64), nil
	}
	operator, err := NewIncrementalGroupCountDistinctInt64(m039GroupCountDistinctKey, failingValue)
	if err != nil {
		t.Fatalf("NewIncrementalGroupCountDistinctInt64() error = %v", err)
	}
	if _, err := operator.Apply([]DifferentialRow{{
		Key:  "row-z",
		Time: 1,
		Diff: 1,
		Row:  Row{"group": "z", "value": int64(1)},
	}}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	_, err = operator.Apply([]DifferentialRow{
		{Key: "row-a", Time: 2, Diff: 1, Row: Row{"group": "a", "value": int64(2)}},
		{Key: "row-bad", Time: 2, Diff: 1, Row: Row{"group": "bad", "value": int64(3), "bad": true}},
	})
	if !errors.Is(err, callbackErr) {
		t.Fatalf("callback error = %v, want wrapped callback error", err)
	}

	if _, err := operator.Apply([]DifferentialRow{{
		Key:  "row-a",
		Time: 3,
		Diff: 1,
		Row:  Row{"group": "a", "value": int64(2)},
	}}); err != nil {
		t.Fatalf("second group Apply() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Diff: 1, Row: Row{"count_distinct": int64(1)}},
		{Key: "z", Diff: 1, Row: Row{"count_distinct": int64(1)}},
	}
	if snapshot := operator.Snapshot(); !reflect.DeepEqual(snapshot, want) {
		t.Fatalf("Snapshot() = %#v, want %#v", snapshot, want)
	}
}
