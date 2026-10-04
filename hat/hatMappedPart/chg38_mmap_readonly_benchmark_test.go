package hatMappedPart

import (
	"os"
	"testing"
)

var mmapReadOnlyBenchmarkSink byte

func BenchmarkMappedReadOnlyPartBaselineReadFile(b *testing.B) {
	path := benchmarkMappedReadOnlyPartFile(b, 1<<20)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		data, err := os.ReadFile(path)
		if err != nil {
			b.Fatal(err)
		}
		mmapReadOnlyBenchmarkSink ^= sampleMappedReadOnlyPart(data)
	}
}

func BenchmarkMappedReadOnlyPartOpenOnceRead(b *testing.B) {
	path := benchmarkMappedReadOnlyPartFile(b, 1<<20)
	part, err := OpenMappedReadOnlyPart(path, MappedReadOnlyPartOptions{
		MaxBytes:     1 << 20,
		ExpectedSize: 1 << 20,
		VerifySize:   true,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer part.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		mmapReadOnlyBenchmarkSink ^= sampleMappedReadOnlyPart(part.Bytes())
	}
}

func BenchmarkMappedReadOnlyPartOpenClose(b *testing.B) {
	path := benchmarkMappedReadOnlyPartFile(b, 1<<20)
	options := MappedReadOnlyPartOptions{
		MaxBytes:     1 << 20,
		ExpectedSize: 1 << 20,
		VerifySize:   true,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		part, err := OpenMappedReadOnlyPart(path, options)
		if err != nil {
			b.Fatal(err)
		}
		mmapReadOnlyBenchmarkSink ^= sampleMappedReadOnlyPart(part.Bytes())
		if err := part.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func sampleMappedReadOnlyPart(data []byte) byte {
	var sample byte
	stride := len(data) / 16
	for offset := 0; offset < len(data); offset += stride {
		sample ^= data[offset]
	}
	return sample
}

func benchmarkMappedReadOnlyPartFile(b *testing.B, size int) string {
	b.Helper()
	path := b.TempDir() + "/part.bin"
	data := make([]byte, size)
	for index := range data {
		data[index] = byte(index * 31)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		b.Fatal(err)
	}
	return path
}
