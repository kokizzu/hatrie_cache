package hatDataStructure_test

import (
	"errors"
	"testing"

	hatDataStructure "hatrie_cache/hat/hatDataStructure"
)

func TestNullableBitmapTracksNullBitsAndResizes(t *testing.T) {
	bitmap, err := hatDataStructure.NewNullableBitmap(65)
	if err != nil {
		t.Fatalf("NewNullableBitmap() error = %v", err)
	}
	if bitmap.Len() != 65 || bitmap.CountNulls() != 0 {
		t.Fatalf("new bitmap length/count = %d/%d", bitmap.Len(), bitmap.CountNulls())
	}
	for _, index := range []int{0, 63, 64} {
		if err := bitmap.SetNull(index); err != nil {
			t.Fatalf("SetNull(%d) error = %v", index, err)
		}
	}
	if bitmap.CountNulls() != 3 {
		t.Fatalf("CountNulls() = %d, want 3", bitmap.CountNulls())
	}
	for _, test := range []struct {
		index  int
		isNull bool
	}{
		{0, true},
		{1, false},
		{63, true},
		{64, true},
	} {
		got, err := bitmap.IsNull(test.index)
		if err != nil {
			t.Fatalf("IsNull(%d) error = %v", test.index, err)
		}
		if got != test.isNull {
			t.Fatalf("IsNull(%d) = %v, want %v", test.index, got, test.isNull)
		}
	}

	if err := bitmap.SetValid(63); err != nil {
		t.Fatalf("SetValid() error = %v", err)
	}
	if bitmap.CountNulls() != 2 {
		t.Fatalf("CountNulls() after SetValid = %d, want 2", bitmap.CountNulls())
	}
	if err := bitmap.Resize(130); err != nil {
		t.Fatalf("Resize(grow) error = %v", err)
	}
	if bitmap.Len() != 130 || bitmap.CountNulls() != 2 {
		t.Fatalf("grown bitmap length/count = %d/%d", bitmap.Len(), bitmap.CountNulls())
	}
	if got, err := bitmap.IsNull(129); err != nil || got {
		t.Fatalf("new row IsNull(129) = %v, error %v, want false", got, err)
	}
	if err := bitmap.SetNull(129); err != nil {
		t.Fatalf("SetNull(129) error = %v", err)
	}
	if err := bitmap.Resize(64); err != nil {
		t.Fatalf("Resize(shrink) error = %v", err)
	}
	if bitmap.Len() != 64 || bitmap.CountNulls() != 1 {
		t.Fatalf("shrunk bitmap length/count = %d/%d, want 64/1", bitmap.Len(), bitmap.CountNulls())
	}
}

func TestNullableBitmapRejectsInvalidLengthsAndIndexes(t *testing.T) {
	if _, err := hatDataStructure.NewNullableBitmap(-1); !errors.Is(err, hatDataStructure.ErrNullableBitmapInvalid) {
		t.Fatalf("NewNullableBitmap(-1) error = %v, want ErrNullableBitmapInvalid", err)
	}
	bitmap, err := hatDataStructure.NewNullableBitmap(1)
	if err != nil {
		t.Fatalf("NewNullableBitmap() error = %v", err)
	}
	for name, index := range map[string]int{"negative": -1, "at length": 1} {
		t.Run(name, func(t *testing.T) {
			if _, err := bitmap.IsNull(index); !errors.Is(err, hatDataStructure.ErrNullableBitmapIndexOutOfRange) {
				t.Fatalf("IsNull(%d) error = %v, want index error", index, err)
			}
			if err := bitmap.SetNull(index); !errors.Is(err, hatDataStructure.ErrNullableBitmapIndexOutOfRange) {
				t.Fatalf("SetNull(%d) error = %v, want index error", index, err)
			}
		})
	}
}

func BenchmarkNullableBitmapSetNull(b *testing.B) {
	bitmap, err := hatDataStructure.NewNullableBitmap(b.N)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := range b.N {
		if err := bitmap.SetNull(index); err != nil {
			b.Fatal(err)
		}
	}
}
