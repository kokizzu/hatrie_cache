package hatSql

import "testing"

func TestMZ038DifferentialFlatMapDistinctOutputsPreservesRowsAndFallback(t *testing.T) {
	input := []DifferentialRow{{
		Key:  "source",
		Time: 7,
		Diff: -3,
		Row:  Row{"value": int64(11)},
	}}

	got, err := FlatMapDifferentialRows(input, func(Row) ([]DifferentialFlatMapResult, error) {
		return []DifferentialFlatMapResult{
			{Key: "left", Row: Row{"value": int64(1)}},
			{Key: "right", Row: Row{"value": int64(2)}},
		}, nil
	})
	if err != nil {
		t.Fatalf("FlatMapDifferentialRows() error = %v", err)
	}
	if len(got) != 2 || got[0].Key != "left" || got[1].Key != "right" {
		t.Fatalf("result = %#v, want left/right in order", got)
	}
	if got[0].Time != 7 || got[0].Diff != -3 || got[1].Time != 7 || got[1].Diff != -3 {
		t.Fatalf("result weights/timestamps = %#v, want both 7/-3", got)
	}
	got[0].Row["value"] = int64(99)
	if input[0].Row["value"] != int64(11) {
		t.Fatal("flat-map result aliases input row")
	}

	duplicate, err := FlatMapDifferentialRows(input, func(Row) ([]DifferentialFlatMapResult, error) {
		return []DifferentialFlatMapResult{
			{Key: "same", Row: Row{"value": int64(1)}},
			{Key: "same", Row: Row{"value": int64(2)}},
		}, nil
	})
	if err != nil {
		t.Fatalf("duplicate flat-map error = %v", err)
	}
	if len(duplicate) != 1 || duplicate[0].Key != "same" || duplicate[0].Diff != -6 {
		t.Fatalf("duplicate result = %#v, want same/-6", duplicate)
	}

	overflow, err := FlatMapDifferentialRows([]DifferentialRow{{Key: "source", Time: 8, Diff: int64(1<<63 - 1)}}, func(Row) ([]DifferentialFlatMapResult, error) {
		return []DifferentialFlatMapResult{
			{Key: "same", Row: Row{"value": int64(1)}},
			{Key: "same", Row: Row{"value": int64(2)}},
		}, nil
	})
	if err == nil {
		t.Fatalf("overflow result = %#v, want an error", overflow)
	}
	if overflow != nil {
		t.Fatalf("overflow result = %#v, want nil", overflow)
	}
}

func BenchmarkMZ038DifferentialFlatMapDistinctOutputs(b *testing.B) {
	rows := []DifferentialRow{{Key: "source", Time: 7, Diff: -3, Row: Row{"value": int64(11)}}}
	b.Run("single-two-distinct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			got, err := FlatMapDifferentialRows(rows, func(Row) ([]DifferentialFlatMapResult, error) {
				return []DifferentialFlatMapResult{
					{Key: "left", Row: Row{"value": int64(1)}},
					{Key: "right", Row: Row{"value": int64(2)}},
				}, nil
			})
			if err != nil || len(got) != 2 {
				b.Fatalf("unexpected result: %#v, %v", got, err)
			}
		}
	})
	b.Run("single-two-same", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			got, err := FlatMapDifferentialRows(rows, func(Row) ([]DifferentialFlatMapResult, error) {
				return []DifferentialFlatMapResult{
					{Key: "same", Row: Row{"value": int64(1)}},
					{Key: "same", Row: Row{"value": int64(2)}},
				}, nil
			})
			if err != nil || len(got) != 1 || got[0].Diff != -6 {
				b.Fatalf("unexpected result: %#v, %v", got, err)
			}
		}
	})
}
