package hatMerkle_test

import (
	"testing"

	"hatrie_cache/hat/hatMerkle"
)

func TestBucketMaskRoundTripsAndSelectsKeys(t *testing.T) {
	var mask hatMerkle.BucketMask
	for _, bucket := range []int{0, 17, hatMerkle.BucketCount - 1} {
		mask[bucket/64] |= uint64(1) << uint(bucket%64)
	}
	if mask.Empty() {
		t.Fatal("Empty() = true for populated mask")
	}
	if !mask.Contains(17) || mask.Contains(-1) || mask.Contains(hatMerkle.BucketCount) {
		t.Fatalf("Contains() returned unexpected result")
	}

	encoded := hatMerkle.EncodeBucketMask(mask)
	decoded, err := hatMerkle.DecodeBucketMask(encoded)
	if err != nil {
		t.Fatalf("DecodeBucketMask() error = %v", err)
	}
	if decoded != mask {
		t.Fatalf("decoded mask = %#v, want %#v", decoded, mask)
	}
	if mask.ContainsKey("customer:42") != mask.Contains(hatMerkle.BucketForKey("customer:42")) {
		t.Fatal("ContainsKey() did not use BucketForKey()")
	}
}

func TestDecodeBucketMaskRejectsMalformedInput(t *testing.T) {
	if _, err := hatMerkle.DecodeBucketMask("not-a-mask"); err == nil {
		t.Fatal("DecodeBucketMask() error = nil, want rejection")
	}
}
