package hatDataStructure

import "testing"

func TestRoaringBitmapLookupBoundaries(t *testing.T) {
	var bitmap RoaringBitmap
	for index := uint32(0); index < 64; index++ {
		bitmap.Add(index<<roaringBitmapContainerBits | index*3)
	}
	for index := uint32(0); index < 64; index++ {
		value := index<<roaringBitmapContainerBits | index*3
		if !bitmap.Contains(value) {
			t.Fatalf("Contains(%d) = false for inserted value", value)
		}
		if bitmap.Contains(value + 1) {
			t.Fatalf("Contains(%d) = true for missing value", value+1)
		}
	}
	if bitmap.Contains(65 << roaringBitmapContainerBits) {
		t.Fatal("Contains() = true beyond the last container")
	}
}
