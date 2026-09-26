package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestGroupCountDistinctStringDifferentialRowsEmitsDistinctTransitions(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "x1", Time: 1, Diff: 2, Row: Row{"group": "a", "value": "x"}},
		{Key: "x2", Time: 2, Diff: 1, Row: Row{"group": "a", "value": "x"}},
		{Key: "y1", Time: 3, Diff: 1, Row: Row{"group": "a", "value": "y"}},
		{Key: "x1", Time: 4, Diff: -3, Row: Row{"group": "a", "value": "x"}},
		{Key: "y1", Time: 5, Diff: -1, Row: Row{"group": "a", "value": "y"}},
	}
	got, err := GroupCountDistinctStringDifferentialRows(rows, func(row SQLRow) string {
		return row["group"].(string)
	}, func(row SQLRow) (string, error) {
		return row["value"].(string), nil
	})
	if err != nil {
		t.Fatalf("GroupCountDistinctStringDifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"count_distinct": int64(1)}},
		{Key: "a", Time: 3, Diff: -1, Row: Row{"count_distinct": int64(1)}},
		{Key: "a", Time: 3, Diff: 1, Row: Row{"count_distinct": int64(2)}},
		{Key: "a", Time: 4, Diff: -1, Row: Row{"count_distinct": int64(2)}},
		{Key: "a", Time: 4, Diff: 1, Row: Row{"count_distinct": int64(1)}},
		{Key: "a", Time: 5, Diff: -1, Row: Row{"count_distinct": int64(1)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
}

func TestGroupCountDistinctStringDifferentialRowsPreservesEmptyValuesAndAtomicErrors(t *testing.T) {
	valueError := errors.New("value callback failed")
	rows := []DifferentialRow{
		{Key: "empty", Time: 1, Diff: 2, Row: Row{"group": "a", "value": ""}},
		{Key: "other", Time: 2, Diff: 1, Row: Row{"group": "a", "value": "other"}},
		{Key: "empty", Time: 3, Diff: -1, Row: Row{"group": "a", "value": ""}},
	}
	got, err := GroupCountDistinctStringDifferentialRows(rows, distinctCountStringGroupKey, distinctCountStringValue)
	if err != nil {
		t.Fatalf("empty value error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"count_distinct": int64(1)}},
		{Key: "a", Time: 2, Diff: -1, Row: Row{"count_distinct": int64(1)}},
		{Key: "a", Time: 2, Diff: 1, Row: Row{"count_distinct": int64(2)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("empty value output = %#v, want %#v", got, want)
	}

	failedRows := append([]DifferentialRow(nil), rows...)
	got, err = GroupCountDistinctStringDifferentialRows(failedRows, distinctCountStringGroupKey, func(row SQLRow) (string, error) {
		if row["value"] == "other" {
			return "", valueError
		}
		return row["value"].(string), nil
	})
	if !errors.Is(err, valueError) {
		t.Fatalf("callback error = %v, want %v", err, valueError)
	}
	if got != nil {
		t.Fatalf("callback error output = %#v, want nil", got)
	}
}

func TestGroupCountDistinctStringDifferentialRowsRejectsInvalidMultiplicity(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "x", Time: 1, Diff: 1, Row: Row{"group": "a", "value": "x"}},
		{Key: "y", Time: 2, Diff: 1, Row: Row{"group": "a", "value": "y"}},
		{Key: "underflow", Time: 3, Diff: -2, Row: Row{"group": "a", "value": "x"}},
	}
	got, err := GroupCountDistinctStringDifferentialRows(rows, distinctCountStringGroupKey, distinctCountStringValue)
	if !errors.Is(err, ErrDifferentialGroupByDistinctValueMultiplicity) {
		t.Fatalf("error = %v, want distinct value multiplicity", err)
	}
	if got != nil {
		t.Fatalf("output = %#v, want nil", got)
	}
}

func TestGroupCountDistinctStringDifferentialRowsRequiresCallbacks(t *testing.T) {
	if _, err := GroupCountDistinctStringDifferentialRows(nil, nil, distinctCountStringValue); !errors.Is(err, ErrDifferentialGroupByKeyRequired) {
		t.Fatalf("key error = %v, want ErrDifferentialGroupByKeyRequired", err)
	}
	if _, err := GroupCountDistinctStringDifferentialRows(nil, distinctCountStringGroupKey, nil); !errors.Is(err, ErrDifferentialGroupByValueRequired) {
		t.Fatalf("value error = %v, want ErrDifferentialGroupByValueRequired", err)
	}
}

func distinctCountStringGroupKey(row SQLRow) string {
	return row["group"].(string)
}

func distinctCountStringValue(row SQLRow) (string, error) {
	return row["value"].(string), nil
}
