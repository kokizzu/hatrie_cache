package hatDataStructure

import (
	"bytes"
	"testing"
)

func BenchmarkTT045TupleCompressionZSTD(b *testing.B) {
	compressor, err := NewTupleCompressor(DefaultTupleCompressionOptions())
	if err != nil {
		b.Fatal(err)
	}
	input := bytes.Repeat([]byte("tenant=42;region=ap-southeast-1;status=ready;"), 2048)
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoded, err := compressor.Compress(input)
		if err != nil {
			b.Fatal(err)
		}
		if len(encoded) == 0 {
			b.Fatal("empty compressed tuple")
		}
	}
}

func BenchmarkTT045TupleDecompressionZSTD(b *testing.B) {
	compressor, err := NewTupleCompressor(DefaultTupleCompressionOptions())
	if err != nil {
		b.Fatal(err)
	}
	input := bytes.Repeat([]byte("tenant=42;region=ap-southeast-1;status=ready;"), 2048)
	encoded, err := compressor.Compress(input)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoded, err := compressor.Decompress(encoded)
		if err != nil {
			b.Fatal(err)
		}
		if len(decoded) != len(input) {
			b.Fatal("wrong decompressed tuple size")
		}
	}
}

func BenchmarkTT045TupleRawCopyBaseline(b *testing.B) {
	input := bytes.Repeat([]byte("tenant=42;region=ap-southeast-1;status=ready;"), 2048)
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		output := append([]byte(nil), input...)
		if len(output) != len(input) {
			b.Fatal("wrong copied tuple size")
		}
	}
}

func BenchmarkTT045TupleCompressionRandomFallback(b *testing.B) {
	compressor, err := NewTupleCompressor(DefaultTupleCompressionOptions())
	if err != nil {
		b.Fatal(err)
	}
	input := make([]byte, 64*1024)
	state := uint32(0x9e3779b9)
	for index := range input {
		state = state*1664525 + 1013904223
		input[index] = byte(state >> 24)
	}
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoded, err := compressor.Compress(input)
		if err != nil {
			b.Fatal(err)
		}
		if len(encoded) == 0 {
			b.Fatal("empty fallback tuple")
		}
	}
}
