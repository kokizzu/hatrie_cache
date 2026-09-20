package hatSql

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
)

func TestGroupCountDistinctInt64DifferentialRowsEmitsDistinctTransitions(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "r1", Time: 1, Diff: 2, Row: Row{"group": "a", "value": int64(2)}},
		{Key: "r2", Time: 2, Diff: 1, Row: Row{"group": "a", "value": int64(2)}},
		{Key: "r3", Time: 3, Diff: 1, Row: Row{"group": "a", "value": int64(5)}},
		{Key: "r1", Time: 4, Diff: -2, Row: Row{"group": "a", "value": int64(2)}},
		{Key: "r3", Time: 5, Diff: -1, Row: Row{"group": "a", "value": int64(5)}},
	}
	got, err := GroupCountDistinctInt64DifferentialRows(rows, distinctCountGroupKey, distinctCountValue)
	if err != nil {
		t.Fatalf("GroupCountDistinctInt64DifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"count_distinct": int64(1)}},
		{Key: "a", Time: 3, Diff: -1, Row: Row{"count_distinct": int64(1)}},
		{Key: "a", Time: 3, Diff: 1, Row: Row{"count_distinct": int64(2)}},
		{Key: "a", Time: 5, Diff: -1, Row: Row{"count_distinct": int64(2)}},
		{Key: "a", Time: 5, Diff: 1, Row: Row{"count_distinct": int64(1)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
	if rows[0].Row["value"] != int64(2) {
		t.Fatal("GroupCountDistinctInt64DifferentialRows() mutated input rows")
	}
}

func TestGroupCountDistinctInt64DifferentialRowsPreservesGroupsAndValues(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "ignored", Time: 1, Diff: 0, Row: Row{"group": "ignored", "value": int64(9)}},
		{Key: "b1", Time: 2, Diff: 2, Row: Row{"group": "b", "value": int64(1)}},
		{Key: "b2", Time: 3, Diff: 1, Row: Row{"group": "b", "value": int64(1)}},
		{Key: "a1", Time: 4, Diff: 1, Row: Row{"group": "a", "value": int64(math.MinInt64)}},
		{Key: "a2", Time: 5, Diff: 1, Row: Row{"group": "a", "value": int64(math.MinInt64)}},
		{Key: "a1", Time: 6, Diff: -1, Row: Row{"group": "a", "value": int64(math.MinInt64)}},
		{Key: "a2", Time: 7, Diff: -1, Row: Row{"group": "a", "value": int64(math.MinInt64)}},
	}
	got, err := GroupCountDistinctInt64DifferentialRows(rows, distinctCountGroupKey, distinctCountValue)
	if err != nil {
		t.Fatalf("GroupCountDistinctInt64DifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "b", Time: 2, Diff: 1, Row: Row{"count_distinct": int64(1)}},
		{Key: "a", Time: 4, Diff: 1, Row: Row{"count_distinct": int64(1)}},
		{Key: "a", Time: 7, Diff: -1, Row: Row{"count_distinct": int64(1)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
}

func TestGroupCountDistinctInt64DifferentialRowsRejectsInvalidInputAtomically(t *testing.T) {
	valueError := errors.New("value callback failed")
	tests := map[string]struct {
		rows  []DifferentialRow
		value DifferentialInt64ValueFunc
		want  error
	}{
		"negative group count": {
			rows:  []DifferentialRow{{Diff: -1, Row: Row{"group": "a", "value": int64(1)}}},
			value: distinctCountValue,
			want:  ErrDifferentialGroupByNegativeCount,
		},
		"negative distinct multiplicity": {
			rows: []DifferentialRow{
				{Diff: 1, Row: Row{"group": "a", "value": int64(1)}},
				{Diff: 1, Row: Row{"group": "a", "value": int64(2)}},
				{Diff: -2, Row: Row{"group": "a", "value": int64(1)}},
			},
			value: distinctCountValue,
			want:  ErrDifferentialGroupByDistinctValueMultiplicity,
		},
		"count overflow": {
			rows: []DifferentialRow{
				{Diff: math.MaxInt64, Row: Row{"group": "a", "value": int64(1)}},
				{Diff: 1, Row: Row{"group": "a", "value": int64(1)}},
			},
			value: distinctCountValue,
			want:  ErrDifferentialGroupByCountOverflow,
		},
		"value callback": {
			rows:  []DifferentialRow{{Diff: 1, Row: Row{"group": "a", "value": int64(1)}}},
			value: func(SQLRow) (int64, error) { return 0, valueError },
			want:  valueError,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := GroupCountDistinctInt64DifferentialRows(test.rows, distinctCountGroupKey, test.value)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if got != nil {
				t.Fatalf("got partial output = %#v, want nil", got)
			}
		})
	}
}

func TestGroupCountDistinctInt64DifferentialRowsRequiresCallbacks(t *testing.T) {
	if _, err := GroupCountDistinctInt64DifferentialRows(nil, nil, distinctCountValue); !errors.Is(err, ErrDifferentialGroupByKeyRequired) {
		t.Fatalf("key error = %v, want ErrDifferentialGroupByKeyRequired", err)
	}
	if _, err := GroupCountDistinctInt64DifferentialRows(nil, distinctCountGroupKey, nil); !errors.Is(err, ErrDifferentialGroupByValueRequired) {
		t.Fatalf("value error = %v, want ErrDifferentialGroupByValueRequired", err)
	}
}

func ExampleGroupCountDistinctInt64DifferentialRows() {
	rows := []DifferentialRow{
		{Key: "one", Time: 1, Diff: 1, Row: Row{"team": "red", "value": int64(2)}},
		{Key: "two", Time: 2, Diff: 1, Row: Row{"team": "red", "value": int64(2)}},
		{Key: "three", Time: 3, Diff: 1, Row: Row{"team": "red", "value": int64(5)}},
	}
	updates, err := GroupCountDistinctInt64DifferentialRows(rows, func(row SQLRow) string {
		return row["team"].(string)
	}, func(row SQLRow) (int64, error) {
		return row["value"].(int64), nil
	})
	if err != nil {
		panic(err)
	}
	for _, update := range updates {
		fmt.Printf("%s %d %v\n", update.Key, update.Diff, update.Row["count_distinct"])
	}
	// Output:
	// red 1 1
	// red -1 1
	// red 1 2
}

func distinctCountGroupKey(row SQLRow) string {
	return row["group"].(string)
}

func distinctCountValue(row SQLRow) (int64, error) {
	return row["value"].(int64), nil
}
