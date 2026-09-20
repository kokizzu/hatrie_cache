package hatSql

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
)

func TestGroupSumDistinctInt64DifferentialRowsEmitsDistinctTransitions(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "r1", Time: 1, Diff: 1, Row: Row{"group": "a", "value": int64(2)}},
		{Key: "r2", Time: 2, Diff: 1, Row: Row{"group": "a", "value": int64(2)}},
		{Key: "r3", Time: 3, Diff: 1, Row: Row{"group": "a", "value": int64(5)}},
		{Key: "r1", Time: 4, Diff: -1, Row: Row{"group": "a", "value": int64(2)}},
		{Key: "r2", Time: 5, Diff: -1, Row: Row{"group": "a", "value": int64(2)}},
		{Key: "r3", Time: 6, Diff: -1, Row: Row{"group": "a", "value": int64(5)}},
	}
	got, err := GroupSumDistinctInt64DifferentialRows(rows, distinctSumGroupKey, distinctSumValue)
	if err != nil {
		t.Fatalf("GroupSumDistinctInt64DifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"sum": int64(2)}},
		{Key: "a", Time: 3, Diff: -1, Row: Row{"sum": int64(2)}},
		{Key: "a", Time: 3, Diff: 1, Row: Row{"sum": int64(7)}},
		{Key: "a", Time: 5, Diff: -1, Row: Row{"sum": int64(7)}},
		{Key: "a", Time: 5, Diff: 1, Row: Row{"sum": int64(5)}},
		{Key: "a", Time: 6, Diff: -1, Row: Row{"sum": int64(5)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
	if rows[0].Row["value"] != int64(2) {
		t.Fatal("GroupSumDistinctInt64DifferentialRows() mutated input rows")
	}
}

func TestGroupSumDistinctInt64DifferentialRowsPreservesGroupsAndWeights(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "ignored", Time: 1, Diff: 0, Row: Row{"group": "ignored", "value": int64(9)}},
		{Key: "b1", Time: 2, Diff: 2, Row: Row{"group": "b", "value": int64(1)}},
		{Key: "b2", Time: 3, Diff: 1, Row: Row{"group": "b", "value": int64(1)}},
		{Key: "a1", Time: 4, Diff: 1, Row: Row{"group": "a", "value": int64(-3)}},
	}
	got, err := GroupSumDistinctInt64DifferentialRows(rows, distinctSumGroupKey, distinctSumValue)
	if err != nil {
		t.Fatalf("GroupSumDistinctInt64DifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "b", Time: 2, Diff: 1, Row: Row{"sum": int64(1)}},
		{Key: "a", Time: 4, Diff: 1, Row: Row{"sum": int64(-3)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
}

func TestGroupSumDistinctInt64DifferentialRowsHandlesMinimumIntValue(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "min", Time: 1, Diff: 1, Row: Row{"group": "a", "value": int64(math.MinInt64)}},
		{Key: "min", Time: 2, Diff: -1, Row: Row{"group": "a", "value": int64(math.MinInt64)}},
	}
	got, err := GroupSumDistinctInt64DifferentialRows(rows, distinctSumGroupKey, distinctSumValue)
	if err != nil {
		t.Fatalf("GroupSumDistinctInt64DifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"sum": int64(math.MinInt64)}},
		{Key: "a", Time: 2, Diff: -1, Row: Row{"sum": int64(math.MinInt64)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
}

func TestGroupSumDistinctInt64DifferentialRowsRejectsInvalidInputAtomically(t *testing.T) {
	valueError := errors.New("value callback failed")
	tests := map[string]struct {
		rows  []DifferentialRow
		value DifferentialInt64ValueFunc
		want  error
	}{
		"negative group count": {
			rows:  []DifferentialRow{{Diff: -1, Row: Row{"group": "a", "value": int64(1)}}},
			value: distinctSumValue,
			want:  ErrDifferentialGroupByNegativeCount,
		},
		"negative distinct multiplicity": {
			rows: []DifferentialRow{
				{Diff: 1, Row: Row{"group": "a", "value": int64(1)}},
				{Diff: 1, Row: Row{"group": "a", "value": int64(2)}},
				{Diff: -2, Row: Row{"group": "a", "value": int64(1)}},
			},
			value: distinctSumValue,
			want:  ErrDifferentialGroupByDistinctValueMultiplicity,
		},
		"count overflow": {
			rows: []DifferentialRow{
				{Diff: math.MaxInt64, Row: Row{"group": "a", "value": int64(1)}},
				{Diff: 1, Row: Row{"group": "a", "value": int64(1)}},
			},
			value: distinctSumValue,
			want:  ErrDifferentialGroupByCountOverflow,
		},
		"value callback": {
			rows:  []DifferentialRow{{Diff: 1, Row: Row{"group": "a", "value": int64(1)}}},
			value: func(SQLRow) (int64, error) { return 0, valueError },
			want:  valueError,
		},
		"sum overflow": {
			rows: []DifferentialRow{
				{Diff: 1, Row: Row{"group": "a", "value": int64(math.MaxInt64)}},
				{Diff: 1, Row: Row{"group": "a", "value": int64(1)}},
			},
			value: distinctSumValue,
			want:  ErrDifferentialGroupBySumOverflow,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := GroupSumDistinctInt64DifferentialRows(test.rows, distinctSumGroupKey, test.value)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if got != nil {
				t.Fatalf("got partial output = %#v, want nil", got)
			}
		})
	}
}

func TestGroupSumDistinctInt64DifferentialRowsRequiresCallbacks(t *testing.T) {
	if _, err := GroupSumDistinctInt64DifferentialRows(nil, nil, distinctSumValue); !errors.Is(err, ErrDifferentialGroupByKeyRequired) {
		t.Fatalf("key error = %v, want ErrDifferentialGroupByKeyRequired", err)
	}
	if _, err := GroupSumDistinctInt64DifferentialRows(nil, distinctSumGroupKey, nil); !errors.Is(err, ErrDifferentialGroupByValueRequired) {
		t.Fatalf("value error = %v, want ErrDifferentialGroupByValueRequired", err)
	}
}

func ExampleGroupSumDistinctInt64DifferentialRows() {
	rows := []DifferentialRow{
		{Key: "one", Time: 1, Diff: 1, Row: Row{"team": "red", "value": int64(2)}},
		{Key: "two", Time: 2, Diff: 1, Row: Row{"team": "red", "value": int64(2)}},
		{Key: "three", Time: 3, Diff: 1, Row: Row{"team": "red", "value": int64(5)}},
	}
	updates, err := GroupSumDistinctInt64DifferentialRows(rows, func(row SQLRow) string {
		return row["team"].(string)
	}, func(row SQLRow) (int64, error) {
		return row["value"].(int64), nil
	})
	if err != nil {
		panic(err)
	}
	for _, update := range updates {
		fmt.Printf("%s %d %v\n", update.Key, update.Diff, update.Row["sum"])
	}
	// Output:
	// red 1 2
	// red -1 2
	// red 1 7
}

func distinctSumGroupKey(row SQLRow) string {
	return row["group"].(string)
}

func distinctSumValue(row SQLRow) (int64, error) {
	return row["value"].(int64), nil
}
