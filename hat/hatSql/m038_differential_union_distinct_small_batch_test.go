package hatSql

import "testing"

func TestMZ038DifferentialUnionDistinctSmallBatchPreservesRowsAndOwnership(t *testing.T) {
	input := []DifferentialRow{
		{Key: "left", Time: 2, Diff: 2, Row: Row{"value": int64(1)}},
		{Key: "right", Time: 3, Diff: -1, Row: Row{"value": int64(2)}},
	}

	got, err := UnionDifferentialRows(input)
	if err != nil {
		t.Fatalf("UnionDifferentialRows() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(result) = %d, want 2", len(got))
	}
	if got[0].Key != "left" || got[0].Time != 2 || got[0].Diff != 2 || got[1].Key != "right" || got[1].Time != 3 || got[1].Diff != -1 {
		t.Fatalf("result = %#v, want input order and weights", got)
	}
	got[0].Row["value"] = int64(99)
	if input[0].Row["value"] != int64(1) {
		t.Fatal("result row aliases input row")
	}
}

func TestMZ038DifferentialUnionDistinctSmallBatchKeepsConsolidationFallback(t *testing.T) {
	duplicate, err := UnionDifferentialRows([]DifferentialRow{
		{Key: "same", Time: 4, Diff: 3, Row: Row{"value": int64(1)}},
		{Key: "same", Time: 4, Diff: -2, Row: Row{"value": int64(2)}},
	})
	if err != nil {
		t.Fatalf("duplicate union error = %v", err)
	}
	if len(duplicate) != 1 || duplicate[0].Key != "same" || duplicate[0].Time != 4 || duplicate[0].Diff != 1 {
		t.Fatalf("duplicate result = %#v, want same/4/+1", duplicate)
	}

	zero, err := UnionDifferentialRows([]DifferentialRow{
		{Key: "zero", Time: 5, Diff: 0, Row: Row{"value": int64(1)}},
		{Key: "value", Time: 5, Diff: 1, Row: Row{"value": int64(2)}},
	})
	if err != nil {
		t.Fatalf("zero union error = %v", err)
	}
	if len(zero) != 1 || zero[0].Key != "value" || zero[0].Diff != 1 {
		t.Fatalf("zero result = %#v, want value/+1", zero)
	}
}

func BenchmarkMZ038DifferentialUnionDistinctSmallBatch(b *testing.B) {
	distinct := []DifferentialRow{
		{Key: "left", Time: 2, Diff: 2, Row: Row{"value": int64(1)}},
		{Key: "right", Time: 3, Diff: -1, Row: Row{"value": int64(2)}},
	}
	same := []DifferentialRow{
		{Key: "same", Time: 2, Diff: 2, Row: Row{"value": int64(1)}},
		{Key: "same", Time: 2, Diff: -1, Row: Row{"value": int64(2)}},
	}
	b.Run("two-distinct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			got, err := UnionDifferentialRows(distinct)
			if err != nil || len(got) != 2 {
				b.Fatalf("unexpected result: %#v, %v", got, err)
			}
		}
	})
	b.Run("two-same", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			got, err := UnionDifferentialRows(same)
			if err != nil || len(got) != 1 || got[0].Diff != 1 {
				b.Fatalf("unexpected result: %#v, %v", got, err)
			}
		}
	})
}
