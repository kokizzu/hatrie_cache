package hatDataStructure

import (
	"bytes"
	"errors"
	"testing"
)

func TestPersistentDeleteBitmapRoundTrip(t *testing.T) {
	bitmap, err := NewPersistentDeleteBitmap(130)
	if err != nil {
		t.Fatalf("NewPersistentDeleteBitmap: %v", err)
	}
	for _, row := range []uint64{0, 63, 64, 129} {
		changed, err := bitmap.Delete(row)
		if err != nil || !changed {
			t.Fatalf("Delete(%d) = %v/%v, want true/nil", row, changed, err)
		}
	}
	changed, err := bitmap.Delete(64)
	if err != nil || changed {
		t.Fatalf("duplicate Delete(64) = %v/%v, want false/nil", changed, err)
	}
	if bitmap.Deleted() != 4 || bitmap.Live() != 126 {
		t.Fatalf("counts = %d/%d, want 4/126", bitmap.Deleted(), bitmap.Live())
	}
	encoded, err := bitmap.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	decoded, err := DecodePersistentDeleteBitmap(encoded)
	if err != nil {
		t.Fatalf("DecodePersistentDeleteBitmap: %v", err)
	}
	if decoded.Rows() != 130 || decoded.Deleted() != 4 || decoded.Live() != 126 {
		t.Fatalf("decoded counts = %d/%d/%d, want 130/4/126", decoded.Rows(), decoded.Deleted(), decoded.Live())
	}
	for row := uint64(0); row < decoded.Rows(); row++ {
		want := row == 0 || row == 63 || row == 64 || row == 129
		if decoded.Contains(row) != want {
			t.Fatalf("Contains(%d) = %v, want %v", row, decoded.Contains(row), want)
		}
	}
	reencoded, err := decoded.MarshalBinary()
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if !bytes.Equal(encoded, reencoded) {
		t.Fatal("round-trip encoding is not deterministic")
	}
}

func TestPersistentDeleteBitmapUndeleteAndBounds(t *testing.T) {
	bitmap, err := NewPersistentDeleteBitmap(2)
	if err != nil {
		t.Fatalf("NewPersistentDeleteBitmap: %v", err)
	}
	if _, err := bitmap.Delete(2); !errors.Is(err, ErrPersistentDeleteBitmapOutOfRange) {
		t.Fatalf("out-of-range Delete error = %v", err)
	}
	if _, err := bitmap.Undelete(2); !errors.Is(err, ErrPersistentDeleteBitmapOutOfRange) {
		t.Fatalf("out-of-range Undelete error = %v", err)
	}
	if changed, err := bitmap.Delete(1); err != nil || !changed {
		t.Fatalf("Delete(1) = %v/%v, want true/nil", changed, err)
	}
	if changed, err := bitmap.Undelete(1); err != nil || !changed {
		t.Fatalf("Undelete(1) = %v/%v, want true/nil", changed, err)
	}
	if changed, err := bitmap.Undelete(1); err != nil || changed {
		t.Fatalf("duplicate Undelete(1) = %v/%v, want false/nil", changed, err)
	}
}

func TestPersistentDeleteBitmapRejectsCorruptionAndTrailingData(t *testing.T) {
	bitmap, err := NewPersistentDeleteBitmap(65)
	if err != nil {
		t.Fatalf("NewPersistentDeleteBitmap: %v", err)
	}
	if _, err := bitmap.Delete(64); err != nil {
		t.Fatalf("Delete(64): %v", err)
	}
	encoded, err := bitmap.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	corrupted := append([]byte(nil), encoded...)
	corrupted[len(corrupted)/2] ^= 0x40
	if _, err := DecodePersistentDeleteBitmap(corrupted); !errors.Is(err, ErrPersistentDeleteBitmapInvalid) {
		t.Fatalf("corrupt decode error = %v, want invalid", err)
	}
	trailing := append(append([]byte(nil), encoded...), 1)
	if _, err := DecodePersistentDeleteBitmap(trailing); !errors.Is(err, ErrPersistentDeleteBitmapInvalid) {
		t.Fatalf("trailing decode error = %v, want invalid", err)
	}
	if _, err := DecodePersistentDeleteBitmap(encoded[:len(encoded)-1]); !errors.Is(err, ErrPersistentDeleteBitmapInvalid) {
		t.Fatalf("truncated decode error = %v, want invalid", err)
	}
}

func TestPersistentDeleteBitmapRejectsInvalidRows(t *testing.T) {
	if _, err := NewPersistentDeleteBitmap(^uint64(0)); !errors.Is(err, ErrPersistentDeleteBitmapInvalid) {
		t.Fatalf("oversized bitmap error = %v, want invalid", err)
	}
}

func TestPersistentDeleteBitmapEmptyAndAtomicRestore(t *testing.T) {
	empty, err := NewPersistentDeleteBitmap(0)
	if err != nil {
		t.Fatalf("NewPersistentDeleteBitmap(0): %v", err)
	}
	encoded, err := empty.MarshalBinary()
	if err != nil {
		t.Fatalf("empty MarshalBinary: %v", err)
	}
	decoded, err := DecodePersistentDeleteBitmap(encoded)
	if err != nil || decoded.Rows() != 0 || decoded.Deleted() != 0 {
		t.Fatalf("empty decode = %#v/%v, want zero bitmap", decoded, err)
	}

	target, err := NewPersistentDeleteBitmap(8)
	if err != nil {
		t.Fatalf("NewPersistentDeleteBitmap(8): %v", err)
	}
	if _, err := target.Delete(1); err != nil {
		t.Fatalf("Delete(1): %v", err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[0] ^= 1
	if err := target.UnmarshalBinary(corrupt); !errors.Is(err, ErrPersistentDeleteBitmapInvalid) {
		t.Fatalf("invalid UnmarshalBinary error = %v, want invalid", err)
	}
	if !target.Contains(1) || target.Rows() != 8 || target.Deleted() != 1 {
		t.Fatal("failed UnmarshalBinary changed the receiver")
	}
}

func TestPersistentDeleteBitmapPackedSize(t *testing.T) {
	bitmap, err := NewPersistentDeleteBitmap(100_000)
	if err != nil {
		t.Fatalf("NewPersistentDeleteBitmap: %v", err)
	}
	for row := uint64(0); row < bitmap.Rows(); row += 3 {
		if _, err := bitmap.Delete(row); err != nil {
			t.Fatalf("Delete(%d): %v", row, err)
		}
	}
	encoded, err := bitmap.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	// Version 2 adds one encoding tag while retaining the same dense payload
	// for this high-cardinality fixture.
	if len(encoded) != 12_523 {
		t.Fatalf("encoded length = %d, want 12523", len(encoded))
	}
}
