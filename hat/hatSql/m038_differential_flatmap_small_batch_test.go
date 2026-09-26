package hatSql

import (
	"errors"
	"testing"
)

func TestMZ038DifferentialFlatMapSmallBatchPreservesWeightAndOwnership(t *testing.T) {
	input := []DifferentialRow{{
		Key:  "source",
		Time: 7,
		Diff: -3,
		Row:  Row{"value": int64(11)},
	}}
	returned := Row{"value": int64(99)}

	got, err := FlatMapDifferentialRows(input, func(row Row) ([]DifferentialFlatMapResult, error) {
		row["value"] = int64(99)
		return []DifferentialFlatMapResult{{Key: "mapped", Row: returned}}, nil
	})
	if err != nil {
		t.Fatalf("FlatMapDifferentialRows() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(result) = %d, want 1", len(got))
	}
	if got[0].Key != "mapped" || got[0].Time != 7 || got[0].Diff != -3 {
		t.Fatalf("result metadata = %+v, want mapped/7/-3", got[0])
	}
	if got[0].Row["value"] != int64(99) {
		t.Fatalf("result value = %v, want 99", got[0].Row["value"])
	}
	if input[0].Row["value"] != int64(11) {
		t.Fatalf("input row was mutated: %v", input[0].Row["value"])
	}
	got[0].Row["value"] = int64(123)
	if returned["value"] != int64(99) {
		t.Fatal("result row aliases callback output")
	}
}

func TestMZ038DifferentialFlatMapSmallBatchPreservesZeroAndFallbackSemantics(t *testing.T) {
	called := false
	zero, err := FlatMapDifferentialRows([]DifferentialRow{{Key: "zero", Diff: 0}}, func(Row) ([]DifferentialFlatMapResult, error) {
		called = true
		return []DifferentialFlatMapResult{{Key: "unexpected"}}, nil
	})
	if err != nil {
		t.Fatalf("zero-diff flat-map error = %v", err)
	}
	if zero != nil {
		t.Fatalf("zero-diff result = %#v, want nil", zero)
	}
	if called {
		t.Fatal("zero-diff callback was called")
	}

	got, err := FlatMapDifferentialRows([]DifferentialRow{
		{Key: "left", Time: 3, Diff: 2, Row: Row{"value": int64(1)}},
		{Key: "right", Time: 3, Diff: -1, Row: Row{"value": int64(2)}},
	}, func(Row) ([]DifferentialFlatMapResult, error) {
		return []DifferentialFlatMapResult{{Key: "same", Row: Row{"value": int64(7)}}}, nil
	})
	if err != nil {
		t.Fatalf("duplicate fallback error = %v", err)
	}
	if len(got) != 1 || got[0].Key != "same" || got[0].Diff != 1 {
		t.Fatalf("duplicate fallback result = %#v, want same/+1", got)
	}

	multiple, err := FlatMapDifferentialRows([]DifferentialRow{{Key: "source", Time: 4, Diff: 1}}, func(Row) ([]DifferentialFlatMapResult, error) {
		return []DifferentialFlatMapResult{
			{Key: "first", Row: Row{"value": int64(1)}},
			{Key: "second", Row: Row{"value": int64(2)}},
		}, nil
	})
	if err != nil {
		t.Fatalf("multi-output fallback error = %v", err)
	}
	if len(multiple) != 2 || multiple[0].Key != "first" || multiple[1].Key != "second" {
		t.Fatalf("multi-output fallback result = %#v", multiple)
	}
}

func TestMZ038DifferentialFlatMapSmallBatchPropagatesErrors(t *testing.T) {
	want := errors.New("stop")
	got, err := FlatMapDifferentialRows([]DifferentialRow{{Key: "source", Diff: 1}}, func(Row) ([]DifferentialFlatMapResult, error) {
		return nil, want
	})
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want wrapped %v", err, want)
	}
	if got != nil {
		t.Fatalf("result = %#v, want nil on error", got)
	}
}

func BenchmarkMZ038DifferentialFlatMapSmallBatch(b *testing.B) {
	rows := []DifferentialRow{{Key: "source", Time: 7, Diff: -3, Row: Row{"value": int64(11)}}}
	b.Run("single-one-result", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			got, err := FlatMapDifferentialRows(rows, func(Row) ([]DifferentialFlatMapResult, error) {
				return []DifferentialFlatMapResult{{Key: "mapped", Row: Row{"value": int64(99)}}}, nil
			})
			if err != nil || len(got) != 1 {
				b.Fatalf("unexpected result: %#v, %v", got, err)
			}
		}
	})
	b.Run("single-empty-result", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			got, err := FlatMapDifferentialRows(rows, func(Row) ([]DifferentialFlatMapResult, error) {
				return nil, nil
			})
			if err != nil || got != nil {
				b.Fatalf("unexpected result: %#v, %v", got, err)
			}
		}
	})
	b.Run("single-zero", func(b *testing.B) {
		zero := []DifferentialRow{{Key: "zero", Diff: 0, Row: Row{"value": int64(11)}}}
		for i := 0; i < b.N; i++ {
			got, err := FlatMapDifferentialRows(zero, func(Row) ([]DifferentialFlatMapResult, error) {
				return []DifferentialFlatMapResult{{Key: "unexpected"}}, nil
			})
			if err != nil || got != nil {
				b.Fatalf("unexpected result: %#v, %v", got, err)
			}
		}
	})
}
