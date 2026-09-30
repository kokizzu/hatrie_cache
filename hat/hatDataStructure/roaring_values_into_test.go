package hatDataStructure

import "testing"

func TestRoaringBitmapValuesIntoMatchesValuesAndReusesDestination(t *testing.T) {
	bitmap := NewRoaringBitmap()
	bitmap.Add(0, 1, 65535, 65536, 1<<31)

	destination := make([]uint32, 0, int(bitmap.Count()))
	backing := &destination[:cap(destination)][0]
	got := bitmap.ValuesInto(destination)
	if want := bitmap.Values(); !equalUint32Slices(got, want) {
		t.Fatalf("ValuesInto() = %v, want %v", got, want)
	}
	if &got[:cap(got)][0] != backing {
		t.Fatal("ValuesInto() did not reuse the destination backing array")
	}

	bitmap.Add(2)
	got = bitmap.ValuesInto(got)
	if want := bitmap.Values(); !equalUint32Slices(got, want) {
		t.Fatalf("ValuesInto() after mutation = %v, want %v", got, want)
	}

	var empty []uint32
	if got := bitmap.ValuesInto(empty); !equalUint32Slices(got, bitmap.Values()) {
		t.Fatalf("ValuesInto(nil) = %v, want %v", got, bitmap.Values())
	}
}

func equalUint32Slices(left, right []uint32) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
