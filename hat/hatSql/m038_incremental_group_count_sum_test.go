package hatSql

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func m038GroupCountSumKey(row SQLRow) string {
	return row["group"].(string)
}

func m038GroupCountSumValue(row SQLRow) (int64, error) {
	return row["value"].(int64), nil
}

func m038GroupCountSumInitialRows(count int) []DifferentialRow {
	rows := make([]DifferentialRow, count)
	for index := range rows {
		rows[index] = DifferentialRow{
			Key:  fmt.Sprintf("row-%d", index),
			Time: 1,
			Diff: 1,
			Row: Row{
				"group": fmt.Sprintf("group-%d", index%100),
				"value": int64(index%17 + 1),
			},
		}
	}
	return rows
}

func BenchmarkM038RebuildGroupCountSum(b *testing.B) {
	updates := m038GroupCountSumInitialRows(10_000)
	updates = append(updates, DifferentialRow{
		Key:  "new-row",
		Time: 2,
		Diff: 1,
		Row: Row{
			"group": "group-42",
			"value": int64(7),
		},
	})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := GroupCountSumInt64DifferentialRows(updates, m038GroupCountSumKey, m038GroupCountSumValue); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM038IncrementalGroupCountSum(b *testing.B) {
	operator, err := NewIncrementalGroupCountSumInt64(m038GroupCountSumKey, m038GroupCountSumValue)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := operator.Apply(m038GroupCountSumInitialRows(10_000)); err != nil {
		b.Fatal(err)
	}
	update := []DifferentialRow{{
		Key:  "new-row",
		Time: 2,
		Diff: 1,
		Row: Row{
			"group": "group-42",
			"value": int64(7),
		},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := operator.Apply(update); err != nil {
			b.Fatal(err)
		}
	}
}

func TestM038IncrementalGroupCountSumApplyTransitions(t *testing.T) {
	operator, err := NewIncrementalGroupCountSumInt64(m038GroupCountSumKey, m038GroupCountSumValue)
	if err != nil {
		t.Fatalf("NewIncrementalGroupCountSumInt64() error = %v", err)
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
	want := []DifferentialRow{{Key: "red", Time: 1, Diff: 1, Row: Row{"count": int64(1), "sum": int64(5)}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("initial Apply() = %#v, want %#v", got, want)
	}

	got, err = operator.Apply([]DifferentialRow{{
		Key:  "row-b",
		Time: 2,
		Diff: 1,
		Row:  Row{"group": "red", "value": int64(3)},
	}})
	if err != nil {
		t.Fatalf("incrementing Apply() error = %v", err)
	}
	want = []DifferentialRow{
		{Key: "red", Time: 2, Diff: -1, Row: Row{"count": int64(1), "sum": int64(5)}},
		{Key: "red", Time: 2, Diff: 1, Row: Row{"count": int64(2), "sum": int64(8)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("incrementing Apply() = %#v, want %#v", got, want)
	}

	got, err = operator.Apply([]DifferentialRow{{
		Key:  "row-b",
		Time: 3,
		Diff: -1,
		Row:  Row{"group": "red", "value": int64(3)},
	}})
	if err != nil {
		t.Fatalf("retraction Apply() error = %v", err)
	}
	want = []DifferentialRow{
		{Key: "red", Time: 3, Diff: -1, Row: Row{"count": int64(2), "sum": int64(8)}},
		{Key: "red", Time: 3, Diff: 1, Row: Row{"count": int64(1), "sum": int64(5)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("retraction Apply() = %#v, want %#v", got, want)
	}

	got, err = operator.Apply([]DifferentialRow{{
		Key:  "row-a",
		Time: 4,
		Diff: -1,
		Row:  Row{"group": "red", "value": int64(5)},
	}})
	if err != nil {
		t.Fatalf("last retraction Apply() error = %v", err)
	}
	want = []DifferentialRow{{Key: "red", Time: 4, Diff: -1, Row: Row{"count": int64(1), "sum": int64(5)}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("last retraction Apply() = %#v, want %#v", got, want)
	}
	if snapshot := operator.Snapshot(); snapshot != nil {
		t.Fatalf("Snapshot() = %#v, want nil", snapshot)
	}
}

func TestM038IncrementalGroupCountSumBatchIsAtomicAndCoalesces(t *testing.T) {
	operator, err := NewIncrementalGroupCountSumInt64(m038GroupCountSumKey, m038GroupCountSumValue)
	if err != nil {
		t.Fatalf("NewIncrementalGroupCountSumInt64() error = %v", err)
	}
	if _, err := operator.Apply([]DifferentialRow{{
		Key:  "row-a",
		Time: 1,
		Diff: 1,
		Row:  Row{"group": "red", "value": int64(5)},
	}}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}

	got, err := operator.Apply([]DifferentialRow{
		{Key: "row-b", Time: 2, Diff: 1, Row: Row{"group": "blue", "value": int64(4)}},
		{Key: "row-b", Time: 2, Diff: -1, Row: Row{"group": "blue", "value": int64(4)}},
	})
	if err != nil {
		t.Fatalf("coalesced Apply() error = %v", err)
	}
	if got != nil {
		t.Fatalf("coalesced Apply() = %#v, want nil", got)
	}

	_, err = operator.Apply([]DifferentialRow{
		{Key: "row-c", Time: 3, Diff: 1, Row: Row{"group": "green", "value": int64(2)}},
		{Key: "row-a", Time: 3, Diff: -2, Row: Row{"group": "red", "value": int64(5)}},
	})
	if !errors.Is(err, ErrDifferentialGroupByNegativeCount) {
		t.Fatalf("invalid batch error = %v, want ErrDifferentialGroupByNegativeCount", err)
	}
	want := []DifferentialRow{{Key: "red", Diff: 1, Row: Row{"count": int64(1), "sum": int64(5)}}}
	if snapshot := operator.Snapshot(); !reflect.DeepEqual(snapshot, want) {
		t.Fatalf("Snapshot() after rejected batch = %#v, want %#v", snapshot, want)
	}
}

func TestM038IncrementalGroupCountSumValidatesAndSnapshotsDeterministically(t *testing.T) {
	if _, err := NewIncrementalGroupCountSumInt64(nil, m038GroupCountSumValue); !errors.Is(err, ErrDifferentialGroupByKeyRequired) {
		t.Fatalf("nil key callback error = %v, want ErrDifferentialGroupByKeyRequired", err)
	}
	if _, err := NewIncrementalGroupCountSumInt64(m038GroupCountSumKey, nil); !errors.Is(err, ErrDifferentialGroupByValueRequired) {
		t.Fatalf("nil value callback error = %v, want ErrDifferentialGroupByValueRequired", err)
	}

	callbackErr := errors.New("bad value")
	failingValue := func(row SQLRow) (int64, error) {
		if row["bad"] == true {
			return 0, callbackErr
		}
		return row["value"].(int64), nil
	}
	operator, err := NewIncrementalGroupCountSumInt64(m038GroupCountSumKey, failingValue)
	if err != nil {
		t.Fatalf("NewIncrementalGroupCountSumInt64() error = %v", err)
	}
	if _, err := operator.Apply([]DifferentialRow{{
		Key:  "row-a",
		Time: 1,
		Diff: 1,
		Row:  Row{"group": "z", "value": int64(1)},
	}}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	_, err = operator.Apply([]DifferentialRow{
		{Key: "row-b", Time: 2, Diff: 1, Row: Row{"group": "a", "value": int64(2)}},
		{Key: "row-c", Time: 2, Diff: 1, Row: Row{"group": "bad", "value": int64(3), "bad": true}},
	})
	if !errors.Is(err, callbackErr) {
		t.Fatalf("callback error = %v, want wrapped callback error", err)
	}
	want := []DifferentialRow{{Key: "z", Diff: 1, Row: Row{"count": int64(1), "sum": int64(1)}}}
	if snapshot := operator.Snapshot(); !reflect.DeepEqual(snapshot, want) {
		t.Fatalf("Snapshot() after callback error = %#v, want %#v", snapshot, want)
	}

	if _, err := operator.Apply([]DifferentialRow{{
		Key:  "row-z",
		Time: 3,
		Diff: 1,
		Row:  Row{"group": "a", "value": int64(1)},
	}}); err != nil {
		t.Fatalf("second group Apply() error = %v", err)
	}
	want = []DifferentialRow{
		{Key: "a", Diff: 1, Row: Row{"count": int64(1), "sum": int64(1)}},
		{Key: "z", Diff: 1, Row: Row{"count": int64(1), "sum": int64(1)}},
	}
	if snapshot := operator.Snapshot(); !reflect.DeepEqual(snapshot, want) {
		t.Fatalf("Snapshot() = %#v, want %#v", snapshot, want)
	}
}

func TestM038IncrementalGroupCountSumRejectsSumOverflow(t *testing.T) {
	operator, err := NewIncrementalGroupCountSumInt64(m038GroupCountSumKey, m038GroupCountSumValue)
	if err != nil {
		t.Fatalf("NewIncrementalGroupCountSumInt64() error = %v", err)
	}
	if _, err := operator.Apply([]DifferentialRow{{
		Key:  "row-a",
		Diff: 1,
		Row:  Row{"group": "red", "value": int64(^uint64(0) >> 1)},
	}}); err != nil {
		t.Fatalf("max-sum Apply() error = %v", err)
	}
	if _, err := operator.Apply([]DifferentialRow{{
		Key:  "row-b",
		Diff: 1,
		Row:  Row{"group": "red", "value": int64(1)},
	}}); !errors.Is(err, ErrDifferentialGroupBySumOverflow) {
		t.Fatalf("overflow error = %v, want ErrDifferentialGroupBySumOverflow", err)
	}
	want := []DifferentialRow{{Key: "red", Diff: 1, Row: Row{"count": int64(1), "sum": int64(^uint64(0) >> 1)}}}
	if snapshot := operator.Snapshot(); !reflect.DeepEqual(snapshot, want) {
		t.Fatalf("Snapshot() after overflow = %#v, want %#v", snapshot, want)
	}
}
