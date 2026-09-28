package hatHash_test

import (
	"encoding/binary"
	"testing"

	"hatrie_cache/hat/hatHash"
)

var hashKeyFastpathSink uint64

func BenchmarkFNV1a64Uint64EncodedBaseline(b *testing.B) {
	var encoded [8]byte
	value := uint64(0x1020304050607080)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		binary.BigEndian.PutUint64(encoded[:], value+uint64(index))
		hashKeyFastpathSink = hatHash.FNV1a64(encoded[:])
	}
}

func BenchmarkFNV1a64Uint64Fastpath(b *testing.B) {
	value := uint64(0x1020304050607080)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		hashKeyFastpathSink = hatHash.FNV1a64Uint64(value + uint64(index))
	}
}

func BenchmarkFNV1a64Int64Fastpath(b *testing.B) {
	value := int64(0x1020304050607080)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		hashKeyFastpathSink = hatHash.FNV1a64Int64(value + int64(index))
	}
}
