package hatPartition

import "testing"

func TestValidateAndIndex(t *testing.T) {
	for _, count := range []int{0, 2, 4, 256} {
		if err := Validate(count); err != nil {
			t.Fatalf("Validate(%d) error = %v", count, err)
		}
	}
	for _, count := range []int{-1, 1, 3, 512} {
		if err := Validate(count); err == nil {
			t.Fatalf("Validate(%d) error = nil, want error", count)
		}
	}
	if first, second := Index("tenant:42", 16), Index("tenant:42", 16); first != second || first < 0 || first >= 16 {
		t.Fatalf("Index() = %d/%d, want stable index in [0,16)", first, second)
	}
}
