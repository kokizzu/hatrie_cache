package hatDataStructure

import (
	"bytes"
	"testing"
	"time"
)

func BenchmarkTTLRecompressionDecision(b *testing.B) {
	policy := TTLRecompressionPolicy{RecompressAfter: time.Hour, DeleteAfter: 24 * time.Hour}
	createdAt := time.Unix(100, 0)
	now := createdAt.Add(2 * time.Hour)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		_ = policy.Decide(createdAt, now)
	}
}

func BenchmarkTTLRecompressionKeepFastPath(b *testing.B) {
	compressor, err := NewTupleCompressor(TupleCompressionOptions{Algorithm: TupleCompressionNone})
	if err != nil {
		b.Fatal(err)
	}
	frame, err := compressor.Compress([]byte("payload"))
	if err != nil {
		b.Fatal(err)
	}
	createdAt := time.Unix(100, 0)
	policy := TTLRecompressionPolicy{RecompressAfter: time.Hour}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, _, err := compressor.RecompressIfDue(frame, createdAt, createdAt.Add(time.Minute), policy, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTTLRecompressionRewrite(b *testing.B) {
	hot, err := NewTupleCompressor(TupleCompressionOptions{Algorithm: TupleCompressionNone})
	if err != nil {
		b.Fatal(err)
	}
	cold, err := NewTupleCompressor(DefaultTupleCompressionOptions())
	if err != nil {
		b.Fatal(err)
	}
	payload := bytes.Repeat([]byte("recompression-benchmark-payload-"), 1024)
	frame, err := hot.Compress(payload)
	if err != nil {
		b.Fatal(err)
	}
	createdAt := time.Unix(100, 0)
	policy := TTLRecompressionPolicy{RecompressAfter: time.Hour}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, _, err := hot.RecompressIfDue(frame, createdAt, createdAt.Add(2*time.Hour), policy, cold); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTTLRecompressionManualRewrite(b *testing.B) {
	hot, err := NewTupleCompressor(TupleCompressionOptions{Algorithm: TupleCompressionNone})
	if err != nil {
		b.Fatal(err)
	}
	cold, err := NewTupleCompressor(DefaultTupleCompressionOptions())
	if err != nil {
		b.Fatal(err)
	}
	payload := bytes.Repeat([]byte("recompression-benchmark-payload-"), 1024)
	frame, err := hot.Compress(payload)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		decoded, err := hot.Decompress(frame)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := cold.Compress(decoded); err != nil {
			b.Fatal(err)
		}
	}
}
