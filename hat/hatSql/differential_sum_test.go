package hatSql

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"testing"
)

var errDifferentialTestValue = errors.New("value rejected")

func TestGroupSumInt64DifferentialRowsEmitsWeightedRetractions(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "r1", Time: 1, Diff: 2, Row: Row{"group": "a", "value": int64(3)}},
		{Key: "r2", Time: 2, Diff: 1, Row: Row{"group": "a", "value": int64(4)}},
		{Key: "r1", Time: 3, Diff: -1, Row: Row{"group": "a", "value": int64(3)}},
		{Key: "r1", Time: 4, Diff: -1, Row: Row{"group": "a", "value": int64(3)}},
		{Key: "r2", Time: 5, Diff: -1, Row: Row{"group": "a", "value": int64(4)}},
	}
	got, err := GroupSumInt64DifferentialRows(rows, differentialTestGroupKey, differentialTestInt64Value)
	if err != nil {
		t.Fatalf("GroupSumInt64DifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"sum": int64(6)}},
		{Key: "a", Time: 2, Diff: -1, Row: Row{"sum": int64(6)}},
		{Key: "a", Time: 2, Diff: 1, Row: Row{"sum": int64(10)}},
		{Key: "a", Time: 3, Diff: -1, Row: Row{"sum": int64(10)}},
		{Key: "a", Time: 3, Diff: 1, Row: Row{"sum": int64(7)}},
		{Key: "a", Time: 4, Diff: -1, Row: Row{"sum": int64(7)}},
		{Key: "a", Time: 4, Diff: 1, Row: Row{"sum": int64(4)}},
		{Key: "a", Time: 5, Diff: -1, Row: Row{"sum": int64(4)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
	if rows[0].Row["value"] != int64(3) {
		t.Fatal("GroupSumInt64DifferentialRows() mutated input rows")
	}
}

func TestGroupSumInt64DifferentialRowsTracksPresenceSeparatelyFromSum(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "one", Time: 1, Diff: 1, Row: Row{"group": "a", "value": int64(-5)}},
		{Key: "two", Time: 2, Diff: 1, Row: Row{"group": "a", "value": int64(5)}},
		{Key: "one", Time: 3, Diff: -1, Row: Row{"group": "a", "value": int64(-5)}},
		{Key: "two", Time: 4, Diff: -1, Row: Row{"group": "a", "value": int64(5)}},
	}
	got, err := GroupSumInt64DifferentialRows(rows, differentialTestGroupKey, differentialTestInt64Value)
	if err != nil {
		t.Fatalf("GroupSumInt64DifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"sum": int64(-5)}},
		{Key: "a", Time: 2, Diff: -1, Row: Row{"sum": int64(-5)}},
		{Key: "a", Time: 2, Diff: 1, Row: Row{"sum": int64(0)}},
		{Key: "a", Time: 3, Diff: -1, Row: Row{"sum": int64(0)}},
		{Key: "a", Time: 3, Diff: 1, Row: Row{"sum": int64(5)}},
		{Key: "a", Time: 4, Diff: -1, Row: Row{"sum": int64(5)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
}

func TestGroupSumInt64DifferentialRowsRejectsInvalidStateAndErrorsAtomically(t *testing.T) {
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
		"accumulator overflow": {
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
					return 0, errDifferentialTestValue
				}
				return row["value"].(int64), nil
			},
			want: errDifferentialTestValue,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := GroupSumInt64DifferentialRows(test.rows, differentialTestGroupKey, test.value)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if got != nil {
				t.Fatalf("got partial output = %#v, want nil", got)
			}
		})
	}
}

func TestGroupSumInt64DifferentialRowsRequiresCallbacks(t *testing.T) {
	if _, err := GroupSumInt64DifferentialRows(nil, nil, differentialTestInt64Value); !errors.Is(err, ErrDifferentialGroupByKeyRequired) {
		t.Fatalf("nil group key error = %v, want ErrDifferentialGroupByKeyRequired", err)
	}
	if _, err := GroupSumInt64DifferentialRows(nil, differentialTestGroupKey, nil); !errors.Is(err, ErrDifferentialGroupByValueRequired) {
		t.Fatalf("nil value callback error = %v, want ErrDifferentialGroupByValueRequired", err)
	}
	got, err := GroupSumInt64DifferentialRows(nil, differentialTestGroupKey, differentialTestInt64Value)
	if err != nil {
		t.Fatalf("empty input error = %v", err)
	}
	if got != nil {
		t.Fatalf("empty input = %#v, want nil", got)
	}
}

func TestMultiplyDifferentialInt64ChecksSignedBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		left  int64
		right int64
		want  int64
		ok    bool
	}{
		{name: "max times one", left: math.MaxInt64, right: 1, want: math.MaxInt64, ok: true},
		{name: "min times one", left: math.MinInt64, right: 1, want: math.MinInt64, ok: true},
		{name: "negative product", left: -2, right: 3, want: -6, ok: true},
		{name: "positive product", left: -2, right: -3, want: 6, ok: true},
		{name: "min times negative one", left: math.MinInt64, right: -1},
		{name: "negative one times min", left: -1, right: math.MinInt64},
		{name: "positive overflow", left: math.MaxInt64, right: 2},
		{name: "negative overflow", left: math.MinInt64, right: 2},
		{name: "both negative overflow", left: math.MinInt64, right: -2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := multiplyDifferentialInt64(test.left, test.right)
			if ok != test.ok || (ok && got != test.want) {
				t.Fatalf("multiplyDifferentialInt64(%d, %d) = (%d, %v), want (%d, %v)", test.left, test.right, got, ok, test.want, test.ok)
			}
		})
	}
}

func ExampleGroupSumInt64DifferentialRows() {
	rows := []DifferentialRow{
		{Key: "one", Time: 1, Diff: 1, Row: Row{"team": "red", "points": int64(3)}},
		{Key: "two", Time: 2, Diff: 1, Row: Row{"team": "red", "points": int64(4)}},
	}
	updates, err := GroupSumInt64DifferentialRows(rows,
		func(row SQLRow) string { return row["team"].(string) },
		func(row SQLRow) (int64, error) { return row["points"].(int64), nil },
	)
	if err != nil {
		panic(err)
	}
	for _, update := range updates {
		fmt.Printf("%s %d %v\n", update.Key, update.Diff, update.Row["sum"])
	}
	// Output:
	// red 1 3
	// red -1 3
	// red 1 7
}

func BenchmarkGroupSumInt64DifferentialRows(b *testing.B) {
	rows := make([]DifferentialRow, 1024)
	for index := range rows {
		rows[index] = DifferentialRow{
			Key:  fmt.Sprintf("row-%d", index),
			Time: uint64(index),
			Diff: 1,
			Row:  Row{"group": strconv.Itoa(index % 256), "value": int64(index)},
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := GroupSumInt64DifferentialRows(rows, differentialTestGroupKey, differentialTestInt64Value); err != nil {
			b.Fatal(err)
		}
	}
}

func differentialTestGroupKey(row SQLRow) string {
	return row["group"].(string)
}

func differentialTestInt64Value(row SQLRow) (int64, error) {
	return row["value"].(int64), nil
}
