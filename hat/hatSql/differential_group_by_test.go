package hatSql

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"testing"
)

func TestGroupCountDifferentialRowsEmitsRetractionsAndInsertions(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "r1", Time: 1, Diff: 1, Row: Row{"group": "a"}},
		{Key: "r2", Time: 2, Diff: 1, Row: Row{"group": "a"}},
		{Key: "r1", Time: 3, Diff: -1, Row: Row{"group": "a"}},
		{Key: "r2", Time: 4, Diff: -1, Row: Row{"group": "a"}},
	}
	got, err := GroupCountDifferentialRows(rows, func(row SQLRow) string {
		return row["group"].(string)
	})
	if err != nil {
		t.Fatalf("GroupCountDifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"count": int64(1)}},
		{Key: "a", Time: 2, Diff: -1, Row: Row{"count": int64(1)}},
		{Key: "a", Time: 2, Diff: 1, Row: Row{"count": int64(2)}},
		{Key: "a", Time: 3, Diff: -1, Row: Row{"count": int64(2)}},
		{Key: "a", Time: 3, Diff: 1, Row: Row{"count": int64(1)}},
		{Key: "a", Time: 4, Diff: -1, Row: Row{"count": int64(1)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
	if rows[0].Row["group"] != "a" {
		t.Fatal("GroupCountDifferentialRows() mutated input rows")
	}
}

func TestGroupCountDifferentialRowsSupportsMultipleGroupsAndZeroDiff(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "ignored", Time: 1, Diff: 0, Row: Row{"group": "ignored"}},
		{Key: "b1", Time: 2, Diff: 2, Row: Row{"group": "b"}},
		{Key: "a1", Time: 3, Diff: 1, Row: Row{"group": "a"}},
	}
	got, err := GroupCountDifferentialRows(rows, func(row SQLRow) string {
		return row["group"].(string)
	})
	if err != nil {
		t.Fatalf("GroupCountDifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "b", Time: 2, Diff: 1, Row: Row{"count": int64(2)}},
		{Key: "a", Time: 3, Diff: 1, Row: Row{"count": int64(1)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
}

func TestGroupCountDifferentialRowsRejectsInvalidCounts(t *testing.T) {
	tests := map[string]struct {
		rows []DifferentialRow
		want error
	}{
		"negative count": {
			rows: []DifferentialRow{{Key: "r", Diff: -1, Row: Row{"group": "a"}}},
			want: ErrDifferentialGroupByNegativeCount,
		},
		"overflow": {
			rows: []DifferentialRow{
				{Key: "r1", Diff: math.MaxInt64, Row: Row{"group": "a"}},
				{Key: "r2", Diff: 1, Row: Row{"group": "a"}},
			},
			want: ErrDifferentialGroupByCountOverflow,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := GroupCountDifferentialRows(test.rows, func(row SQLRow) string {
				return row["group"].(string)
			})
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestGroupCountDifferentialRowsRequiresKey(t *testing.T) {
	if _, err := GroupCountDifferentialRows(nil, nil); !errors.Is(err, ErrDifferentialGroupByKeyRequired) {
		t.Fatalf("error = %v, want ErrDifferentialGroupByKeyRequired", err)
	}
}

func TestGroupCountDifferentialRowsReturnsNoPartialOutputOnError(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "r1", Time: 1, Diff: 1, Row: Row{"group": "a"}},
		{Key: "r2", Time: 2, Diff: -1, Row: Row{"group": "b"}},
	}
	got, err := GroupCountDifferentialRows(rows, func(row SQLRow) string {
		return row["group"].(string)
	})
	if !errors.Is(err, ErrDifferentialGroupByNegativeCount) {
		t.Fatalf("error = %v, want ErrDifferentialGroupByNegativeCount", err)
	}
	if got != nil {
		t.Fatalf("got partial output = %#v, want nil", got)
	}
}

func TestGroupCountDifferentialRowsHandlesEmptyInput(t *testing.T) {
	got, err := GroupCountDifferentialRows(nil, func(SQLRow) string { return "a" })
	if err != nil {
		t.Fatalf("GroupCountDifferentialRows() error = %v", err)
	}
	if got != nil {
		t.Fatalf("got = %#v, want nil", got)
	}
}

func ExampleGroupCountDifferentialRows() {
	rows := []DifferentialRow{
		{Key: "one", Time: 1, Diff: 1, Row: Row{"team": "red"}},
		{Key: "two", Time: 2, Diff: 1, Row: Row{"team": "red"}},
	}
	updates, err := GroupCountDifferentialRows(rows, func(row SQLRow) string {
		return row["team"].(string)
	})
	if err != nil {
		panic(err)
	}
	for _, update := range updates {
		fmt.Printf("%s %d %v\n", update.Key, update.Diff, update.Row["count"])
	}
	// Output:
	// red 1 1
	// red -1 1
	// red 1 2
}

func BenchmarkGroupCountDifferentialRows(b *testing.B) {
	rows := make([]DifferentialRow, 1024)
	for index := range rows {
		rows[index] = DifferentialRow{
			Key:  fmt.Sprintf("row-%d", index),
			Time: uint64(index),
			Diff: 1,
			Row:  Row{"group": strconv.Itoa(index % 256)},
		}
	}
	key := func(row SQLRow) string {
		return row["group"].(string)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := GroupCountDifferentialRows(rows, key); err != nil {
			b.Fatal(err)
		}
	}
}
