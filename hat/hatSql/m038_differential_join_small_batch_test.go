package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestMZ038DifferentialJoinSmallBatchPreservesSignedMultiplicity(t *testing.T) {
	left := []DifferentialRow{{
		Key:  "left",
		Time: 3,
		Diff: -2,
		Row:  Row{"id": "same", "left": "L"},
	}}
	right := []DifferentialRow{{
		Key:  "right",
		Time: 5,
		Diff: 3,
		Row:  Row{"id": "same", "right": "R"},
	}}

	got, err := JoinDifferentialRows(left, right,
		func(left, right Row) (bool, error) {
			left["observed"] = true
			return left["id"] == right["id"], nil
		},
		func(left, right Row) (string, Row, error) {
			return "joined", Row{"left": left["left"], "right": right["right"]}, nil
		},
	)
	if err != nil {
		t.Fatalf("JoinDifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{{
		Key:  "joined",
		Time: 5,
		Diff: -6,
		Row:  Row{"left": "L", "right": "R"},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("JoinDifferentialRows() = %#v, want %#v", got, want)
	}
	if _, exists := left[0].Row["observed"]; exists {
		t.Fatal("join callback mutated the input row")
	}
}

func TestMZ038DifferentialJoinSmallBatchPreservesEmptyAndZeroSemantics(t *testing.T) {
	called := false
	match := func(Row, Row) (bool, error) {
		called = true
		return true, nil
	}
	project := func(Row, Row) (string, Row, error) {
		called = true
		return "joined", Row{}, nil
	}

	for name, input := range map[string][2][]DifferentialRow{
		"empty-left": {
			nil,
			{{Key: "right", Time: 1, Diff: 1, Row: Row{"id": "same"}}},
		},
		"empty-right": {
			{{Key: "left", Time: 1, Diff: 1, Row: Row{"id": "same"}}},
			nil,
		},
		"zero-left": {
			{{Key: "left", Time: 1, Diff: 0, Row: Row{"id": "same"}}},
			{{Key: "right", Time: 1, Diff: 1, Row: Row{"id": "same"}}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			called = false
			got, err := JoinDifferentialRows(input[0], input[1], match, project)
			if err != nil {
				t.Fatalf("JoinDifferentialRows() error = %v", err)
			}
			if got != nil {
				t.Fatalf("JoinDifferentialRows() = %#v, want nil", got)
			}
			if called {
				t.Fatal("callbacks called for an empty effective input")
			}
		})
	}
}

func TestMZ038DifferentialJoinSmallBatchPropagatesCallbackErrors(t *testing.T) {
	want := errors.New("match failed")
	got, err := JoinDifferentialRows(
		[]DifferentialRow{{Key: "left", Diff: 1, Row: Row{"id": "same"}}},
		[]DifferentialRow{{Key: "right", Diff: 1, Row: Row{"id": "same"}}},
		func(Row, Row) (bool, error) { return false, want },
		func(Row, Row) (string, Row, error) { return "", nil, nil },
	)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if got != nil {
		t.Fatalf("result = %#v, want nil", got)
	}
}

var benchmarkMZ038DifferentialJoinSink []DifferentialRow

func BenchmarkMZ038DifferentialJoinSmallBatch(b *testing.B) {
	left := []DifferentialRow{{Key: "left", Time: 3, Diff: -2, Row: Row{"id": "same"}}}
	right := []DifferentialRow{{Key: "right", Time: 5, Diff: 3, Row: Row{"id": "same"}}}
	match := func(left, right Row) (bool, error) { return left["id"] == right["id"], nil }
	project := func(left, right Row) (string, Row, error) {
		return "joined", Row{"left": left["id"], "right": right["id"]}, nil
	}

	b.Run("single-match", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			result, err := JoinDifferentialRows(left, right, match, project)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkMZ038DifferentialJoinSink = result
		}
	})

	right[0].Row["id"] = "different"
	b.Run("single-non-match", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			result, err := JoinDifferentialRows(left, right, match, project)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkMZ038DifferentialJoinSink = result
		}
	})
}
