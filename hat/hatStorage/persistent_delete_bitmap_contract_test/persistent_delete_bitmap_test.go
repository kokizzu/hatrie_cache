package persistent_delete_bitmap_contract_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

func TestPersistentDeleteBitmapLifecycleAndRoundTrip(t *testing.T) {
	bitmap := hatStorage.NewPersistentDeleteBitmap(130)
	if bitmap.RowCount() != 130 || bitmap.DeletedCount() != 0 {
		t.Fatalf("initial bitmap rows=%d deleted=%d", bitmap.RowCount(), bitmap.DeletedCount())
	}
	if !bitmap.Mark(0) || !bitmap.Mark(64) || !bitmap.Mark(129) || bitmap.Mark(64) || bitmap.Mark(130) {
		t.Fatal("mark result mismatch")
	}
	if !bitmap.Contains(0) || !bitmap.Contains(64) || !bitmap.Contains(129) || bitmap.Contains(1) {
		t.Fatal("contains result mismatch")
	}
	if bitmap.DeletedCount() != 3 {
		t.Fatalf("deleted count=%d", bitmap.DeletedCount())
	}

	data, err := bitmap.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := hatStorage.DecodePersistentDeleteBitmap(data)
	if err != nil {
		t.Fatal(err)
	}
	if restored.RowCount() != 130 || restored.DeletedCount() != 3 {
		t.Fatalf("restored rows=%d deleted=%d", restored.RowCount(), restored.DeletedCount())
	}
	var rows []uint32
	if !restored.VisitDeleted(func(row uint32) bool {
		rows = append(rows, row)
		return true
	}) {
		t.Fatal("visit unexpectedly stopped")
	}
	if !reflect.DeepEqual(rows, []uint32{0, 64, 129}) {
		t.Fatalf("visited rows=%v", rows)
	}
	if !bitmap.Unmark(64) || bitmap.Unmark(64) || bitmap.Contains(64) {
		t.Fatal("unmark result mismatch")
	}
}

func TestPersistentDeleteBitmapRejectsCorruptionAndPreservesBounds(t *testing.T) {
	bitmap := hatStorage.NewPersistentDeleteBitmap(17)
	bitmap.Mark(3)
	data, err := bitmap.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), data...)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := hatStorage.DecodePersistentDeleteBitmap(corrupt); !errors.Is(err, hatStorage.ErrPersistentDeleteBitmapCorrupt) {
		t.Fatalf("checksum error=%v", err)
	}
	if _, err := hatStorage.DecodePersistentDeleteBitmap(data[:len(data)-1]); !errors.Is(err, hatStorage.ErrPersistentDeleteBitmapCorrupt) {
		t.Fatalf("truncated error=%v", err)
	}
	if !bitmap.Mark(16) || bitmap.Mark(17) || bitmap.Contains(17) {
		t.Fatal("row bound was not enforced")
	}
	if _, err := hatStorage.DecodePersistentDeleteBitmap(nil); !errors.Is(err, hatStorage.ErrPersistentDeleteBitmapCorrupt) {
		t.Fatalf("empty input error=%v", err)
	}
}

func TestPersistentDeleteBitmapAdaptiveEncodingNeverExceedsDenseBaseline(t *testing.T) {
	patterns := [][]uint32{chu06SparseRows(), chu06DenseRows(), chu06RandomRows()}
	for _, rows := range patterns {
		bitmap := hatStorage.NewPersistentDeleteBitmap(chu06BenchmarkRows)
		for _, row := range rows {
			bitmap.Mark(row)
		}
		data, err := bitmap.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		baseline := buildBaselineDeleteBitmap(rows).encode()
		if len(data) > len(baseline) {
			t.Fatalf("candidate bytes=%d baseline bytes=%d rows=%d", len(data), len(baseline), len(rows))
		}
		if _, err := hatStorage.DecodePersistentDeleteBitmap(data); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPersistentDeleteBitmapEncodingSizes(t *testing.T) {
	patterns := []struct {
		name string
		rows []uint32
	}{
		{name: "sparse", rows: chu06SparseRows()},
		{name: "dense", rows: chu06DenseRows()},
		{name: "random", rows: chu06RandomRows()},
	}
	for _, pattern := range patterns {
		bitmap := buildCandidateDeleteBitmap(pattern.rows)
		data, err := bitmap.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		baseline := buildBaselineDeleteBitmap(pattern.rows).encode()
		t.Logf("%s deleted=%d candidate_bytes=%d baseline_bytes=%d", pattern.name, len(pattern.rows), len(data), len(baseline))
	}
}

func TestPersistentDeleteBitmapVisitCanStop(t *testing.T) {
	bitmap := hatStorage.NewPersistentDeleteBitmap(10)
	bitmap.Mark(1)
	bitmap.Mark(2)
	if bitmap.VisitDeleted(func(uint32) bool { return false }) {
		t.Fatal("visit did not report early stop")
	}
	if bitmap.VisitDeleted(nil) {
		t.Fatal("nil visit unexpectedly completed")
	}
}

func TestPersistentDeleteBitmapUnmarshalBinary(t *testing.T) {
	source := hatStorage.NewPersistentDeleteBitmap(32)
	source.Mark(4)
	source.Mark(31)
	data, err := source.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	target := hatStorage.NewPersistentDeleteBitmap(1)
	if err := target.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	if target.RowCount() != 32 || !target.Contains(4) || !target.Contains(31) || target.Contains(0) {
		t.Fatal("unmarshal state mismatch")
	}
}

var chu06CandidateEncoded []byte

func buildCandidateDeleteBitmap(rows []uint32) *hatStorage.PersistentDeleteBitmap {
	bitmap := hatStorage.NewPersistentDeleteBitmap(chu06BenchmarkRows)
	for _, row := range rows {
		bitmap.Mark(row)
	}
	return bitmap
}

func BenchmarkCHU06CandidateSparseEncode(b *testing.B) {
	bitmap := buildCandidateDeleteBitmap(chu06SparseRows())
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		chu06CandidateEncoded, _ = bitmap.MarshalBinary()
	}
}

func BenchmarkCHU06CandidateDenseEncode(b *testing.B) {
	bitmap := buildCandidateDeleteBitmap(chu06DenseRows())
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		chu06CandidateEncoded, _ = bitmap.MarshalBinary()
	}
}

func BenchmarkCHU06CandidateRandomEncode(b *testing.B) {
	bitmap := buildCandidateDeleteBitmap(chu06RandomRows())
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		chu06CandidateEncoded, _ = bitmap.MarshalBinary()
	}
}
