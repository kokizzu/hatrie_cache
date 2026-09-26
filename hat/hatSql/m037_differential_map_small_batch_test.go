package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestMZ037DifferentialMapSmallBatchPreservesWeightAndOwnership(t *testing.T) {
	input := []DifferentialRow{{
		Key:  "source",
		Time: 7,
		Diff: -3,
		Row:  Row{"value": "original", "payload": []byte("bytes")},
	}}

	got, err := MapDifferentialRows(input, func(row Row) (string, Row, error) {
		row["observed"] = true
		payload := append([]byte(nil), row["payload"].([]byte)...)
		return "mapped", Row{"value": row["value"], "payload": payload}, nil
	})
	if err != nil {
		t.Fatalf("MapDifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{{
		Key:  "mapped",
		Time: 7,
		Diff: -3,
		Row:  Row{"value": "original", "payload": []byte("bytes")},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MapDifferentialRows() = %#v, want %#v", got, want)
	}
	if _, exists := input[0].Row["observed"]; exists {
		t.Fatal("map callback mutated the input row")
	}
	got[0].Row["value"] = "changed"
	got[0].Row["payload"].([]byte)[0] = 'X'
	if input[0].Row["value"] != "original" || string(input[0].Row["payload"].([]byte)) != "bytes" {
		t.Fatal("mapped output aliases the input row")
	}
}

func TestMZ037DifferentialMapSmallBatchPreservesZeroAndDuplicateSemantics(t *testing.T) {
	called := false
	mapRow := func(row Row) (string, Row, error) {
		called = true
		return "mapped", Row{"value": row["value"]}, nil
	}

	zero, err := MapDifferentialRows([]DifferentialRow{{Key: "zero", Diff: 0, Row: Row{"value": 1}}}, mapRow)
	if err != nil {
		t.Fatalf("zero MapDifferentialRows() error = %v", err)
	}
	if zero != nil || called {
		t.Fatalf("zero result = %#v, callback called = %v; want nil and false", zero, called)
	}

	duplicate, err := MapDifferentialRows([]DifferentialRow{
		{Key: "first", Time: 1, Diff: 2, Row: Row{"value": 1}},
		{Key: "second", Time: 1, Diff: -1, Row: Row{"value": 2}},
	}, mapRow)
	if err != nil {
		t.Fatalf("duplicate MapDifferentialRows() error = %v", err)
	}
	if len(duplicate) != 1 || duplicate[0].Key != "mapped" || duplicate[0].Diff != 1 {
		t.Fatalf("duplicate result = %#v, want one mapped row with diff 1", duplicate)
	}
}

func TestMZ037DifferentialMapSmallBatchPropagatesCallbackErrors(t *testing.T) {
	want := errors.New("map failed")
	got, err := MapDifferentialRows(
		[]DifferentialRow{{Key: "source", Diff: 1, Row: Row{"value": 1}}},
		func(Row) (string, Row, error) { return "", nil, want },
	)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if got != nil {
		t.Fatalf("result = %#v, want nil", got)
	}
}

var benchmarkMZ037DifferentialMapSink []DifferentialRow

func BenchmarkMZ037DifferentialMapSmallBatch(b *testing.B) {
	input := []DifferentialRow{{Key: "source", Time: 7, Diff: -3, Row: Row{"value": int64(1)}}}
	mapRow := func(row Row) (string, Row, error) {
		return "mapped", Row{"value": row["value"]}, nil
	}

	b.Run("single-map", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			result, err := MapDifferentialRows(input, mapRow)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkMZ037DifferentialMapSink = result
		}
	})

	input[0].Diff = 0
	b.Run("single-zero", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			result, err := MapDifferentialRows(input, mapRow)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkMZ037DifferentialMapSink = result
		}
	})
}
