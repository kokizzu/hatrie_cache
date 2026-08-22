package hatHash_test

import (
	"testing"

	"hatrie_cache/hat/hatHash"
)

func TestFNV64VariantsAreUsableByImporters(t *testing.T) {
	if got, want := hatHash.FNV1a64(nil), uint64(14695981039346656037); got != want {
		t.Fatalf("FNV1a64() = %d, want %d", got, want)
	}
	if got, want := hatHash.FNV1_64(nil), uint64(14695981039346656037); got != want {
		t.Fatalf("FNV1_64() = %d, want %d", got, want)
	}
	if hatHash.FNV1a64JSONString("key") == hatHash.FNV1a64([]byte("key")) {
		t.Fatal("FNV1a64JSONString() must include JSON string quotes")
	}
}
