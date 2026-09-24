package hatDataStructure

import (
	"encoding/binary"
	"testing"
)

func TestCH005SparseDeleteBitmapUsesDeltaEncoding(t *testing.T) {
	bitmap, err := NewPersistentDeleteBitmap(1 << 20)
	if err != nil {
		t.Fatalf("NewPersistentDeleteBitmap() error = %v", err)
	}
	for _, row := range []uint64{3, 700000} {
		if changed, err := bitmap.Delete(row); err != nil || !changed {
			t.Fatalf("Delete(%d) = changed %t, error %v", row, changed, err)
		}
	}
	encoded, err := bitmap.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	if len(encoded) >= 4096 {
		t.Fatalf("sparse snapshot length = %d, want less than 4096 bytes", len(encoded))
	}
	if len(encoded) <= len(persistentDeleteBitmapMagic) || encoded[len(persistentDeleteBitmapMagic)] != 2 {
		t.Fatalf("snapshot version = %d, want adaptive version 2", encoded[len(persistentDeleteBitmapMagic)])
	}
	if cap(encoded) != len(encoded) {
		t.Fatalf("snapshot capacity = %d, want exact length %d", cap(encoded), len(encoded))
	}
	restored, err := DecodePersistentDeleteBitmap(encoded)
	if err != nil {
		t.Fatalf("DecodePersistentDeleteBitmap() error = %v", err)
	}
	if restored.Rows() != bitmap.Rows() || restored.Deleted() != bitmap.Deleted() {
		t.Fatalf("restored counts = rows %d/deleted %d, want rows %d/deleted %d", restored.Rows(), restored.Deleted(), bitmap.Rows(), bitmap.Deleted())
	}
	for _, row := range []uint64{3, 700000} {
		if !restored.Contains(row) {
			t.Fatalf("restored bitmap does not contain deleted row %d", row)
		}
	}
	if restored.Contains(4) {
		t.Fatal("restored bitmap contains live row 4")
	}
}

func TestCH005AdaptiveDeleteBitmapReadsLegacyDenseSnapshot(t *testing.T) {
	bitmap, err := NewPersistentDeleteBitmap(130)
	if err != nil {
		t.Fatalf("NewPersistentDeleteBitmap() error = %v", err)
	}
	for _, row := range []uint64{0, 64, 129} {
		if _, err := bitmap.Delete(row); err != nil {
			t.Fatalf("Delete(%d) error = %v", row, err)
		}
	}
	legacy := append([]byte(persistentDeleteBitmapMagic), persistentDeleteBitmapLegacyVersion)
	legacy = persistentDeleteBitmapAppendUvarint(legacy, bitmap.rows)
	legacy = persistentDeleteBitmapAppendUvarint(legacy, bitmap.deleted)
	legacy = persistentDeleteBitmapAppendUvarint(legacy, uint64(len(bitmap.words)))
	var wordBytes [8]byte
	for _, word := range bitmap.words {
		binary.LittleEndian.PutUint64(wordBytes[:], word)
		legacy = append(legacy, wordBytes[:]...)
	}
	legacy = persistentDeleteBitmapAppendChecksum(legacy)
	restored, err := DecodePersistentDeleteBitmap(legacy)
	if err != nil {
		t.Fatalf("DecodePersistentDeleteBitmap(legacy) error = %v", err)
	}
	for _, row := range []uint64{0, 64, 129} {
		if !restored.Contains(row) {
			t.Fatalf("legacy restore does not contain deleted row %d", row)
		}
	}
}

func BenchmarkCH005AdaptiveDeleteBitmap(b *testing.B) {
	bitmap, err := NewPersistentDeleteBitmap(1 << 20)
	if err != nil {
		b.Fatal(err)
	}
	for _, row := range []uint64{3, 700000, 900000, 1000000} {
		if _, err := bitmap.Delete(row); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for range b.N {
		encoded, err := bitmap.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		restored, err := DecodePersistentDeleteBitmap(encoded)
		if err != nil || restored.Deleted() != bitmap.Deleted() {
			b.Fatalf("round trip error = %v, restored deleted = %d", err, restored.Deleted())
		}
	}
}
