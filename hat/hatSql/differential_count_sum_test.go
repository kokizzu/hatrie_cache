package hatSql

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"testing"
)

var errDifferentialCountSumTestValue = errors.New("count-sum value rejected")

func TestGroupCountSumInt64DifferentialRowsEmitsCombinedTransitions(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "r1", Time: 1, Diff: 2, Row: Row{"group": "a", "value": int64(3)}},
		{Key: "r2", Time: 2, Diff: 1, Row: Row{"group": "a", "value": int64(4)}},
		{Key: "r1", Time: 3, Diff: -1, Row: Row{"group": "a", "value": int64(3)}},
		{Key: "r1", Time: 4, Diff: -1, Row: Row{"group": "a", "value": int64(3)}},
		{Key: "r2", Time: 5, Diff: -1, Row: Row{"group": "a", "value": int64(4)}},
	}
	got, err := GroupCountSumInt64DifferentialRows(rows, differentialTestGroupKey, differentialTestInt64Value)
	if err != nil {
		t.Fatalf("GroupCountSumInt64DifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"count": int64(2), "sum": int64(6)}},
		{Key: "a", Time: 2, Diff: -1, Row: Row{"count": int64(2), "sum": int64(6)}},
		{Key: "a", Time: 2, Diff: 1, Row: Row{"count": int64(3), "sum": int64(10)}},
		{Key: "a", Time: 3, Diff: -1, Row: Row{"count": int64(3), "sum": int64(10)}},
		{Key: "a", Time: 3, Diff: 1, Row: Row{"count": int64(2), "sum": int64(7)}},
		{Key: "a", Time: 4, Diff: -1, Row: Row{"count": int64(2), "sum": int64(7)}},
		{Key: "a", Time: 4, Diff: 1, Row: Row{"count": int64(1), "sum": int64(4)}},
		{Key: "a", Time: 5, Diff: -1, Row: Row{"count": int64(1), "sum": int64(4)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
	if rows[0].Row["value"] != int64(3) {
		t.Fatal("GroupCountSumInt64DifferentialRows() mutated input rows")
	}
}

func TestGroupCountSumInt64DifferentialRowsKeepsZeroSumGroups(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "one", Time: 1, Diff: 1, Row: Row{"group": "a", "value": int64(-5)}},
		{Key: "two", Time: 2, Diff: 1, Row: Row{"group": "a", "value": int64(5)}},
		{Key: "one", Time: 3, Diff: -1, Row: Row{"group": "a", "value": int64(-5)}},
		{Key: "two", Time: 4, Diff: -1, Row: Row{"group": "a", "value": int64(5)}},
	}
	got, err := GroupCountSumInt64DifferentialRows(rows, differentialTestGroupKey, differentialTestInt64Value)
	if err != nil {
		t.Fatalf("GroupCountSumInt64DifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"count": int64(1), "sum": int64(-5)}},
		{Key: "a", Time: 2, Diff: -1, Row: Row{"count": int64(1), "sum": int64(-5)}},
		{Key: "a", Time: 2, Diff: 1, Row: Row{"count": int64(2), "sum": int64(0)}},
		{Key: "a", Time: 3, Diff: -1, Row: Row{"count": int64(2), "sum": int64(0)}},
		{Key: "a", Time: 3, Diff: 1, Row: Row{"count": int64(1), "sum": int64(5)}},
		{Key: "a", Time: 4, Diff: -1, Row: Row{"count": int64(1), "sum": int64(5)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
}

func TestGroupCountSumInt64DifferentialRowsRejectsInvalidStateAtomically(t *testing.T) {
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
					return 0, errDifferentialCountSumTestValue
				}
				return row["value"].(int64), nil
			},
			want: errDifferentialCountSumTestValue,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := GroupCountSumInt64DifferentialRows(test.rows, differentialTestGroupKey, test.value)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if got != nil {
				t.Fatalf("got partial output = %#v, want nil", got)
			}
		})
	}
}

func TestGroupCountSumInt64DifferentialRowsRequiresCallbacks(t *testing.T) {
	if _, err := GroupCountSumInt64DifferentialRows(nil, nil, differentialTestInt64Value); !errors.Is(err, ErrDifferentialGroupByKeyRequired) {
		t.Fatalf("nil group key error = %v, want ErrDifferentialGroupByKeyRequired", err)
	}
	if _, err := GroupCountSumInt64DifferentialRows(nil, differentialTestGroupKey, nil); !errors.Is(err, ErrDifferentialGroupByValueRequired) {
		t.Fatalf("nil value callback error = %v, want ErrDifferentialGroupByValueRequired", err)
	}
	got, err := GroupCountSumInt64DifferentialRows(nil, differentialTestGroupKey, differentialTestInt64Value)
	if err != nil {
		t.Fatalf("empty input error = %v", err)
	}
	if got != nil {
		t.Fatalf("empty input = %#v, want nil", got)
	}
}

func ExampleGroupCountSumInt64DifferentialRows() {
	rows := []DifferentialRow{
		{Key: "one", Time: 1, Diff: 1, Row: Row{"team": "red", "points": int64(3)}},
		{Key: "two", Time: 2, Diff: 1, Row: Row{"team": "red", "points": int64(4)}},
	}
	updates, err := GroupCountSumInt64DifferentialRows(rows,
		func(row SQLRow) string { return row["team"].(string) },
		func(row SQLRow) (int64, error) { return row["points"].(int64), nil },
	)
	if err != nil {
		panic(err)
	}
	for _, update := range updates {
		fmt.Printf("%s %d %v %v\n", update.Key, update.Diff, update.Row["count"], update.Row["sum"])
	}
	// Output:
	// red 1 1 3
	// red -1 1 3
	// red 1 2 7
}

func BenchmarkGroupCountSumInt64DifferentialRows(b *testing.B) {
	rows := make([]DifferentialRow, 1024)
	for index := range rows {
		rows[index] = DifferentialRow{
			Key:  fmt.Sprintf("row-%d", index),
			Time: uint64(index),
			Diff: 1,
			Row:  Row{"group": strconv.Itoa(index % 256), "value": int64(index)},
		}
	}
	for _, test := range []struct {
		name string
		fn   func() error
	}{
		{
			name: "separate_count_sum",
			fn: func() error {
				countRows, err := GroupCountDifferentialRows(rows, differentialTestGroupKey)
				if err != nil {
					return err
				}
				sumRows, err := GroupSumInt64DifferentialRows(rows, differentialTestGroupKey, differentialTestInt64Value)
				if err != nil {
					return err
				}
				differentialCountSumBenchmarkSink += len(countRows) + len(sumRows)
				return nil
			},
		},
		{
			name: "combined",
			fn: func() error {
				result, err := GroupCountSumInt64DifferentialRows(rows, differentialTestGroupKey, differentialTestInt64Value)
				differentialCountSumBenchmarkSink += len(result)
				return err
			},
		},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := test.fn(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

var differentialCountSumBenchmarkSink int
