package hatSql

import (
	"errors"
	"testing"
)

func TestM038DifferentialFilterSingleRowPreservesBehaviorAndOwnership(t *testing.T) {
	input := []DifferentialRow{{
		Key:  "source",
		Time: 9,
		Diff: -3,
		Row:  Row{"value": int64(7)},
	}}
	callbackCalls := 0
	got, err := FilterDifferentialRows(input, func(row Row) (bool, error) {
		callbackCalls++
		row["value"] = int64(99)
		return true, nil
	})
	if err != nil {
		t.Fatalf("selected filter error = %v", err)
	}
	if callbackCalls != 1 {
		t.Fatalf("callback calls = %d, want 1", callbackCalls)
	}
	if len(got) != 1 || got[0].Key != "source" || got[0].Time != 9 || got[0].Diff != -3 || got[0].Row["value"] != int64(99) {
		t.Fatalf("selected result = %#v", got)
	}
	if input[0].Row["value"] != int64(7) {
		t.Fatalf("filter callback mutated input row: %#v", input[0].Row)
	}

	rejected, err := FilterDifferentialRows(input, func(row Row) (bool, error) {
		row["value"] = int64(11)
		return false, nil
	})
	if err != nil {
		t.Fatalf("rejected filter error = %v", err)
	}
	if rejected != nil {
		t.Fatalf("rejected result = %#v, want nil", rejected)
	}
	if input[0].Row["value"] != int64(7) {
		t.Fatalf("rejected callback mutated input row: %#v", input[0].Row)
	}

	zeroCalls := 0
	zero, err := FilterDifferentialRows([]DifferentialRow{{Key: "zero", Diff: 0, Row: Row{"value": int64(1)}}}, func(Row) (bool, error) {
		zeroCalls++
		return true, nil
	})
	if err != nil {
		t.Fatalf("zero-weight filter error = %v", err)
	}
	if zero != nil || zeroCalls != 0 {
		t.Fatalf("zero-weight result/calls = %#v/%d, want nil/0", zero, zeroCalls)
	}
}

func TestM038DifferentialFilterSingleRowRetainsErrors(t *testing.T) {
	if _, err := FilterDifferentialRows([]DifferentialRow{{Row: Row{"value": int64(1)}}}, func(Row) (bool, error) {
		return true, nil
	}); !errors.Is(err, ErrDifferentialRowKeyRequired) {
		t.Fatalf("missing key error = %v", err)
	}
	want := errors.New("filter failed")
	if _, err := FilterDifferentialRows([]DifferentialRow{{Key: "source", Diff: 1, Row: Row{"value": int64(1)}}}, func(Row) (bool, error) {
		return false, want
	}); !errors.Is(err, want) {
		t.Fatalf("callback error = %v, want %v", err, want)
	}
}

func BenchmarkM038DifferentialFilterSingleRow(b *testing.B) {
	input := []DifferentialRow{{Key: "source", Time: 9, Diff: 1, Row: Row{"value": int64(7)}}}
	for _, test := range []struct {
		name     string
		keep     DifferentialFilterFunc
		wantRows int
	}{
		{name: "selected", keep: func(Row) (bool, error) { return true, nil }, wantRows: 1},
		{name: "rejected", keep: func(Row) (bool, error) { return false, nil }, wantRows: 0},
		{name: "zero-weight", keep: func(Row) (bool, error) { return true, nil }, wantRows: 0},
	} {
		for _, implementation := range []struct {
			name   string
			filter func([]DifferentialRow, DifferentialFilterFunc) []DifferentialRow
		}{
			{name: "legacy", filter: legacyM038Filter},
			{name: "optimized", filter: func(rows []DifferentialRow, keep DifferentialFilterFunc) []DifferentialRow {
				got, err := FilterDifferentialRows(rows, keep)
				if err != nil {
					return nil
				}
				return got
			}},
		} {
			b.Run(test.name+"/"+implementation.name, func(b *testing.B) {
				rows := input
				if test.name == "zero-weight" {
					rows = []DifferentialRow{{Key: "zero", Diff: 0, Row: Row{"value": int64(7)}}}
				}
				b.ReportAllocs()
				for index := 0; index < b.N; index++ {
					got := implementation.filter(rows, test.keep)
					if len(got) != test.wantRows {
						b.Fatalf("result = %#v", got)
					}
				}
			})
		}
	}
}

func legacyM038Filter(rows []DifferentialRow, keep DifferentialFilterFunc) []DifferentialRow {
	filtered := make([]DifferentialRow, 0, len(rows))
	for _, update := range rows {
		if update.Key == "" || update.Diff == 0 {
			continue
		}
		row := cloneDifferentialRow(update.Row)
		selected, _ := keep(row)
		if selected {
			update.Row = row
			filtered = append(filtered, update)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}
