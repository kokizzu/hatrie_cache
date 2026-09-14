package hatSql

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
)

var errDifferentialAverageTestValue = errors.New("average value rejected")

func TestGroupAverageInt64DifferentialRowsEmitsExactTransitions(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "r1", Time: 1, Diff: 2, Row: Row{"group": "a", "value": int64(3)}},
		{Key: "r2", Time: 2, Diff: 1, Row: Row{"group": "a", "value": int64(4)}},
		{Key: "r1", Time: 3, Diff: -1, Row: Row{"group": "a", "value": int64(3)}},
		{Key: "r1", Time: 4, Diff: -1, Row: Row{"group": "a", "value": int64(3)}},
		{Key: "r2", Time: 5, Diff: -1, Row: Row{"group": "a", "value": int64(4)}},
	}
	got, err := GroupAverageInt64DifferentialRows(rows, differentialTestGroupKey, differentialTestInt64Value)
	if err != nil {
		t.Fatalf("GroupAverageInt64DifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"avg": float64(3)}},
		{Key: "a", Time: 2, Diff: -1, Row: Row{"avg": float64(3)}},
		{Key: "a", Time: 2, Diff: 1, Row: Row{"avg": float64(10) / 3}},
		{Key: "a", Time: 3, Diff: -1, Row: Row{"avg": float64(10) / 3}},
		{Key: "a", Time: 3, Diff: 1, Row: Row{"avg": float64(7) / 2}},
		{Key: "a", Time: 4, Diff: -1, Row: Row{"avg": float64(7) / 2}},
		{Key: "a", Time: 4, Diff: 1, Row: Row{"avg": float64(4)}},
		{Key: "a", Time: 5, Diff: -1, Row: Row{"avg": float64(4)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
	if rows[0].Row["value"] != int64(3) {
		t.Fatal("GroupAverageInt64DifferentialRows() mutated input rows")
	}
}

func TestGroupAverageInt64DifferentialRowsPreservesZeroAveragePresence(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "one", Time: 1, Diff: 1, Row: Row{"group": "a", "value": int64(-5)}},
		{Key: "two", Time: 2, Diff: 1, Row: Row{"group": "a", "value": int64(5)}},
		{Key: "one", Time: 3, Diff: -1, Row: Row{"group": "a", "value": int64(-5)}},
		{Key: "two", Time: 4, Diff: -1, Row: Row{"group": "a", "value": int64(5)}},
	}
	got, err := GroupAverageInt64DifferentialRows(rows, differentialTestGroupKey, differentialTestInt64Value)
	if err != nil {
		t.Fatalf("GroupAverageInt64DifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"avg": float64(-5)}},
		{Key: "a", Time: 2, Diff: -1, Row: Row{"avg": float64(-5)}},
		{Key: "a", Time: 2, Diff: 1, Row: Row{"avg": float64(0)}},
		{Key: "a", Time: 3, Diff: -1, Row: Row{"avg": float64(0)}},
		{Key: "a", Time: 3, Diff: 1, Row: Row{"avg": float64(5)}},
		{Key: "a", Time: 4, Diff: -1, Row: Row{"avg": float64(5)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
}

func TestGroupAverageInt64DifferentialRowsRejectsInvalidStateAtomically(t *testing.T) {
	tests := map[string]struct {
		rows  []DifferentialRow
		value DifferentialInt64ValueFunc
		want  error
	}{
		"negative count": {
			rows:  []DifferentialRow{{Key: "r", Diff: -1, Row: Row{"group": "a", "value": int64(3)}}},
			value: differentialTestInt64Value,
			want:  ErrDifferentialGroupByNegativeCount,
		},
		"product overflow": {
			rows:  []DifferentialRow{{Key: "r", Diff: math.MaxInt64, Row: Row{"group": "a", "value": int64(2)}}},
			value: differentialTestInt64Value,
			want:  ErrDifferentialGroupBySumOverflow,
		},
		"sum overflow": {
			rows: []DifferentialRow{
				{Key: "r1", Diff: 1, Row: Row{"group": "a", "value": int64(math.MaxInt64)}},
				{Key: "r2", Diff: 1, Row: Row{"group": "b", "value": int64(1)}},
				{Key: "r3", Diff: 1, Row: Row{"group": "a", "value": int64(1)}},
			},
			value: differentialTestInt64Value,
			want:  ErrDifferentialGroupBySumOverflow,
		},
		"value callback": {
			rows: []DifferentialRow{
				{Key: "r1", Diff: 1, Row: Row{"group": "a", "value": int64(1)}},
				{Key: "r2", Diff: 1, Row: Row{"group": "b", "value": int64(2)}},
			},
			value: func(row SQLRow) (int64, error) {
				if row["value"] == int64(2) {
					return 0, errDifferentialAverageTestValue
				}
				return row["value"].(int64), nil
			},
			want: errDifferentialAverageTestValue,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := GroupAverageInt64DifferentialRows(test.rows, differentialTestGroupKey, test.value)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if got != nil {
				t.Fatalf("got partial output = %#v, want nil", got)
			}
		})
	}
}

func TestGroupAverageInt64DifferentialRowsRequiresCallbacks(t *testing.T) {
	if _, err := GroupAverageInt64DifferentialRows(nil, nil, differentialTestInt64Value); !errors.Is(err, ErrDifferentialGroupByKeyRequired) {
		t.Fatalf("nil group key error = %v, want ErrDifferentialGroupByKeyRequired", err)
	}
	if _, err := GroupAverageInt64DifferentialRows(nil, differentialTestGroupKey, nil); !errors.Is(err, ErrDifferentialGroupByValueRequired) {
		t.Fatalf("nil value callback error = %v, want ErrDifferentialGroupByValueRequired", err)
	}
	got, err := GroupAverageInt64DifferentialRows(nil, differentialTestGroupKey, differentialTestInt64Value)
	if err != nil {
		t.Fatalf("empty input error = %v", err)
	}
	if got != nil {
		t.Fatalf("empty input = %#v, want nil", got)
	}
}

func ExampleGroupAverageInt64DifferentialRows() {
	rows := []DifferentialRow{
		{Key: "one", Time: 1, Diff: 1, Row: Row{"team": "red", "points": int64(3)}},
		{Key: "two", Time: 2, Diff: 1, Row: Row{"team": "red", "points": int64(4)}},
	}
	updates, err := GroupAverageInt64DifferentialRows(rows,
		func(row SQLRow) string { return row["team"].(string) },
		func(row SQLRow) (int64, error) { return row["points"].(int64), nil },
	)
	if err != nil {
		panic(err)
	}
	for _, update := range updates {
		fmt.Printf("%s %d %.2f\n", update.Key, update.Diff, update.Row["avg"])
	}
	// Output:
	// red 1 3.00
	// red -1 3.00
	// red 1 3.50
}
