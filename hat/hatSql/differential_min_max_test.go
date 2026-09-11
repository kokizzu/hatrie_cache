package hatSql

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestGroupMinMaxInt64DifferentialRowsEmitsOnlyVisibleChanges(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "one", Time: 1, Diff: 2, Row: Row{"group": "a", "value": int64(5)}},
		{Key: "two", Time: 2, Diff: 1, Row: Row{"group": "a", "value": int64(9)}},
		{Key: "three", Time: 3, Diff: 1, Row: Row{"group": "a", "value": int64(3)}},
		{Key: "one", Time: 4, Diff: -1, Row: Row{"group": "a", "value": int64(5)}},
		{Key: "two", Time: 5, Diff: -1, Row: Row{"group": "a", "value": int64(9)}},
		{Key: "three", Time: 6, Diff: -1, Row: Row{"group": "a", "value": int64(3)}},
		{Key: "one", Time: 7, Diff: -1, Row: Row{"group": "a", "value": int64(5)}},
	}
	got, err := GroupMinMaxInt64DifferentialRows(rows,
		func(row SQLRow) string { return row["group"].(string) },
		func(row SQLRow) (int64, error) { return row["value"].(int64), nil },
	)
	if err != nil {
		t.Fatalf("GroupMinMaxInt64DifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"min": int64(5), "max": int64(5)}},
		{Key: "a", Time: 2, Diff: -1, Row: Row{"min": int64(5), "max": int64(5)}},
		{Key: "a", Time: 2, Diff: 1, Row: Row{"min": int64(5), "max": int64(9)}},
		{Key: "a", Time: 3, Diff: -1, Row: Row{"min": int64(5), "max": int64(9)}},
		{Key: "a", Time: 3, Diff: 1, Row: Row{"min": int64(3), "max": int64(9)}},
		{Key: "a", Time: 5, Diff: -1, Row: Row{"min": int64(3), "max": int64(9)}},
		{Key: "a", Time: 5, Diff: 1, Row: Row{"min": int64(3), "max": int64(5)}},
		{Key: "a", Time: 6, Diff: -1, Row: Row{"min": int64(3), "max": int64(5)}},
		{Key: "a", Time: 6, Diff: 1, Row: Row{"min": int64(5), "max": int64(5)}},
		{Key: "a", Time: 7, Diff: -1, Row: Row{"min": int64(5), "max": int64(5)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
}

func TestGroupMinMaxInt64DifferentialRowsPreservesWeightedDuplicates(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "one", Time: 1, Diff: 2, Row: Row{"group": "a", "value": int64(4)}},
		{Key: "two", Time: 2, Diff: 3, Row: Row{"group": "a", "value": int64(2)}},
		{Key: "three", Time: 3, Diff: -2, Row: Row{"group": "a", "value": int64(2)}},
		{Key: "four", Time: 4, Diff: -2, Row: Row{"group": "a", "value": int64(4)}},
	}
	got, err := GroupMinMaxInt64DifferentialRows(rows,
		func(row SQLRow) string { return row["group"].(string) },
		func(row SQLRow) (int64, error) { return row["value"].(int64), nil },
	)
	if err != nil {
		t.Fatalf("GroupMinMaxInt64DifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"min": int64(4), "max": int64(4)}},
		{Key: "a", Time: 2, Diff: -1, Row: Row{"min": int64(4), "max": int64(4)}},
		{Key: "a", Time: 2, Diff: 1, Row: Row{"min": int64(2), "max": int64(4)}},
		{Key: "a", Time: 4, Diff: -1, Row: Row{"min": int64(2), "max": int64(4)}},
		{Key: "a", Time: 4, Diff: 1, Row: Row{"min": int64(2), "max": int64(2)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
}

func TestGroupMinMaxInt64DifferentialRowsRejectsInvalidUpdatesAtomically(t *testing.T) {
	tests := []struct {
		name string
		rows []DifferentialRow
		want error
	}{
		{
			name: "negative group count",
			rows: []DifferentialRow{{Key: "one", Diff: -1, Row: Row{"group": "a", "value": int64(1)}}},
			want: ErrDifferentialGroupByNegativeCount,
		},
		{
			name: "negative value multiplicity",
			rows: []DifferentialRow{
				{Key: "one", Diff: 1, Row: Row{"group": "a", "value": int64(1)}},
				{Key: "two", Diff: 1, Row: Row{"group": "a", "value": int64(2)}},
				{Key: "three", Diff: -2, Row: Row{"group": "a", "value": int64(1)}},
			},
			want: ErrDifferentialGroupByValueMultiplicity,
		},
		{
			name: "value callback",
			rows: []DifferentialRow{{Key: "one", Diff: 1, Row: Row{"group": "a"}}},
			want: errMinMaxValue,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := func(row SQLRow) (int64, error) {
				if test.name == "value callback" {
					return 0, errMinMaxValue
				}
				return row["value"].(int64), nil
			}
			got, err := GroupMinMaxInt64DifferentialRows(test.rows,
				func(row SQLRow) string { return row["group"].(string) }, value)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if got != nil {
				t.Fatalf("got partial output = %#v, want nil", got)
			}
		})
	}
}

var errMinMaxValue = errors.New("value callback failed")

func TestGroupMinMaxInt64DifferentialRowsHandlesExtremesAndOverflow(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "min", Diff: 1, Row: Row{"group": "a", "value": int64(math.MinInt64)}},
		{Key: "max", Diff: 1, Row: Row{"group": "a", "value": int64(math.MaxInt64)}},
	}
	got, err := GroupMinMaxInt64DifferentialRows(rows,
		func(row SQLRow) string { return row["group"].(string) },
		func(row SQLRow) (int64, error) { return row["value"].(int64), nil },
	)
	if err != nil {
		t.Fatalf("GroupMinMaxInt64DifferentialRows() error = %v", err)
	}
	want := Row{"min": int64(math.MinInt64), "max": int64(math.MaxInt64)}
	if !reflect.DeepEqual(got[len(got)-1].Row, want) {
		t.Fatalf("last aggregate = %#v, want %#v", got[len(got)-1].Row, want)
	}

	overflow := []DifferentialRow{
		{Key: "one", Diff: math.MaxInt64, Row: Row{"group": "a", "value": int64(1)}},
		{Key: "two", Diff: 1, Row: Row{"group": "a", "value": int64(1)}},
	}
	if _, err := GroupMinMaxInt64DifferentialRows(overflow,
		func(row SQLRow) string { return row["group"].(string) },
		func(row SQLRow) (int64, error) { return row["value"].(int64), nil },
	); !errors.Is(err, ErrDifferentialGroupByCountOverflow) {
		t.Fatalf("overflow error = %v, want ErrDifferentialGroupByCountOverflow", err)
	}
}

func TestGroupMinMaxInt64DifferentialRowsRequiresCallbacks(t *testing.T) {
	if _, err := GroupMinMaxInt64DifferentialRows(nil, nil, nil); !errors.Is(err, ErrDifferentialGroupByKeyRequired) {
		t.Fatalf("nil key error = %v, want ErrDifferentialGroupByKeyRequired", err)
	}
	if _, err := GroupMinMaxInt64DifferentialRows(nil, func(SQLRow) string { return "a" }, nil); !errors.Is(err, ErrDifferentialGroupByValueRequired) {
		t.Fatalf("nil value error = %v, want ErrDifferentialGroupByValueRequired", err)
	}
}
