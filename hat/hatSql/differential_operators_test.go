package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestFilterDifferentialRowsPreservesSignedMultiplicityAndOwnsRows(t *testing.T) {
	input := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 2, Row: Row{"kind": "keep"}},
		{Key: "b", Time: 1, Diff: -3, Row: Row{"kind": "drop"}},
	}
	got, err := FilterDifferentialRows(input, func(row Row) (bool, error) {
		return row["kind"] == "keep", nil
	})
	if err != nil {
		t.Fatalf("FilterDifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{{Key: "a", Time: 1, Diff: 2, Row: Row{"kind": "keep"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("FilterDifferentialRows() = %#v, want %#v", got, want)
	}
	got[0].Row["kind"] = "changed"
	if input[0].Row["kind"] != "keep" {
		t.Fatal("FilterDifferentialRows() returned a row alias")
	}
}

func TestMapDifferentialRowsConsolidatesSignedMultiplicity(t *testing.T) {
	input := []DifferentialRow{
		{Key: "a", Time: 4, Diff: 2, Row: Row{"value": "one"}},
		{Key: "b", Time: 4, Diff: -1, Row: Row{"value": "two"}},
	}
	got, err := MapDifferentialRows(input, func(row Row) (string, Row, error) {
		return "same", Row{"value": row["value"]}, nil
	})
	if err != nil {
		t.Fatalf("MapDifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{{Key: "same", Time: 4, Diff: 1, Row: Row{"value": "one"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MapDifferentialRows() = %#v, want %#v", got, want)
	}
	got[0].Row["value"] = "changed"
	if input[0].Row["value"] != "one" {
		t.Fatal("MapDifferentialRows() returned a row alias")
	}
}

func TestMapDifferentialRowsReturnsNoPartialOutputOnCallbackError(t *testing.T) {
	wantErr := errors.New("map failed")
	input := []DifferentialRow{{Key: "a", Diff: 1, Row: Row{"value": "one"}}}
	got, err := MapDifferentialRows(input, func(Row) (string, Row, error) {
		return "", nil, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("MapDifferentialRows() error = %v, want %v", err, wantErr)
	}
	if got != nil {
		t.Fatalf("MapDifferentialRows() output = %#v, want nil", got)
	}
}

func TestFlatMapDifferentialRowsPreservesExpansionMultiplicity(t *testing.T) {
	input := []DifferentialRow{{Key: "source", Time: 2, Diff: -2, Row: Row{"value": "x"}}}
	got, err := FlatMapDifferentialRows(input, func(row Row) ([]DifferentialFlatMapResult, error) {
		return []DifferentialFlatMapResult{
			{Key: "one", Row: Row{"value": row["value"], "part": int64(1)}},
			{Key: "two", Row: Row{"value": row["value"], "part": int64(2)}},
		}, nil
	})
	if err != nil {
		t.Fatalf("FlatMapDifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "one", Time: 2, Diff: -2, Row: Row{"value": "x", "part": int64(1)}},
		{Key: "two", Time: 2, Diff: -2, Row: Row{"value": "x", "part": int64(2)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("FlatMapDifferentialRows() = %#v, want %#v", got, want)
	}
}

func TestUnionDifferentialRowsPreservesDuplicateMultiplicity(t *testing.T) {
	got, err := UnionDifferentialRows(
		[]DifferentialRow{{Key: "same", Time: 1, Diff: 2, Row: Row{"value": "x"}}},
		[]DifferentialRow{{Key: "same", Time: 1, Diff: -1, Row: Row{"value": "x"}}},
	)
	if err != nil {
		t.Fatalf("UnionDifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{{Key: "same", Time: 1, Diff: 1, Row: Row{"value": "x"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("UnionDifferentialRows() = %#v, want %#v", got, want)
	}
}

func TestJoinDifferentialRowsMultipliesSignedWeightsAndUsesLatestTime(t *testing.T) {
	left := []DifferentialRow{{Key: "l", Time: 3, Diff: 2, Row: Row{"id": int64(7)}}}
	right := []DifferentialRow{{Key: "r", Time: 8, Diff: -3, Row: Row{"id": int64(7)}}}
	got, err := JoinDifferentialRows(left, right,
		func(left, right Row) (bool, error) {
			return left["id"] == right["id"], nil
		},
		func(left, right Row) (string, Row, error) {
			return "joined", Row{"id": left["id"], "right": right["id"]}, nil
		},
	)
	if err != nil {
		t.Fatalf("JoinDifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{{Key: "joined", Time: 8, Diff: -6, Row: Row{"id": int64(7), "right": int64(7)}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("JoinDifferentialRows() = %#v, want %#v", got, want)
	}
}

func TestJoinDifferentialRowsRejectsWeightOverflow(t *testing.T) {
	left := []DifferentialRow{{Key: "l", Diff: 1 << 62, Row: Row{"id": 1}}}
	right := []DifferentialRow{{Key: "r", Diff: 4, Row: Row{"id": 1}}}
	got, err := JoinDifferentialRows(left, right,
		func(Row, Row) (bool, error) { return true, nil },
		func(Row, Row) (string, Row, error) { return "joined", Row{}, nil },
	)
	if !errors.Is(err, ErrDifferentialJoinWeightOverflow) {
		t.Fatalf("JoinDifferentialRows() error = %v, want weight overflow", err)
	}
	if got != nil {
		t.Fatalf("JoinDifferentialRows() output = %#v, want nil", got)
	}
}

func TestJoinDifferentialRowsReturnsNoPartialOutputOnCallbackError(t *testing.T) {
	wantErr := errors.New("join failed")
	left := []DifferentialRow{{Key: "l", Diff: 1, Row: Row{"id": 1}}}
	right := []DifferentialRow{{Key: "r", Diff: 1, Row: Row{"id": 1}}}
	got, err := JoinDifferentialRows(left, right,
		func(Row, Row) (bool, error) { return true, nil },
		func(Row, Row) (string, Row, error) { return "", nil, wantErr },
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("JoinDifferentialRows() error = %v, want %v", err, wantErr)
	}
	if got != nil {
		t.Fatalf("JoinDifferentialRows() output = %#v, want nil", got)
	}
}
