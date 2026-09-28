package hatHash_test

import (
	"encoding/binary"
	"testing"

	"hatrie_cache/hat/hatHash"
)

func TestFNV1a64Uint64MatchesCanonicalBigEndian(t *testing.T) {
	values := []uint64{0, 1, 0xff, 0x0102030405060708, ^uint64(0)}
	var encoded [8]byte
	for _, value := range values {
		binary.BigEndian.PutUint64(encoded[:], value)
		if got, want := hatHash.FNV1a64Uint64(value), hatHash.FNV1a64(encoded[:]); got != want {
			t.Fatalf("FNV1a64Uint64(%#x) = %#x, want %#x", value, got, want)
		}
	}
}

func TestFNV1a64Int64UsesTwosComplementEncoding(t *testing.T) {
	var encoded [8]byte
	for _, value := range []int64{0, 1, -1, -123456789, 123456789} {
		binary.BigEndian.PutUint64(encoded[:], uint64(value))
		if got, want := hatHash.FNV1a64Int64(value), hatHash.FNV1a64(encoded[:]); got != want {
			t.Fatalf("FNV1a64Int64(%d) = %#x, want %#x", value, got, want)
		}
	}
}
