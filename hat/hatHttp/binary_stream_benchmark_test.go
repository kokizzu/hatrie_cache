package hatHttp

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

func BenchmarkBinaryStreamFraming(b *testing.B) {
	for _, chunks := range []int{1, 16, 64} {
		chunks := chunks
		b.Run(fmt.Sprintf("framed_%d_chunks", chunks), func(b *testing.B) {
			benchmarkBinaryStreamFramed(b, chunks)
		})
		b.Run(fmt.Sprintf("raw_%d_chunks", chunks), func(b *testing.B) {
			benchmarkBinaryStreamRaw(b, chunks)
		})
	}
}

func benchmarkBinaryStreamFramed(b *testing.B, chunks int) {
	payload := bytes.Repeat([]byte("x"), 1024)
	totalPayload := int64(len(payload) * chunks)
	writer := &binaryStreamCountingWriter{}
	b.ReportAllocs()
	b.SetBytes(totalPayload)
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		stream, err := NewBinaryStreamWriter(writer, BinaryStreamOptions{MaxChunkBytes: len(payload)})
		if err != nil {
			b.Fatal(err)
		}
		for chunk := 0; chunk < chunks; chunk++ {
			if err := stream.WriteChunk(context.Background(), payload); err != nil {
				b.Fatal(err)
			}
		}
		if err := stream.End(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(writer.bytes/int64(b.N)), "wire-bytes/op")
}

func benchmarkBinaryStreamRaw(b *testing.B, chunks int) {
	payload := bytes.Repeat([]byte("x"), 1024)
	totalPayload := int64(len(payload) * chunks)
	writer := &binaryStreamCountingWriter{}
	b.ReportAllocs()
	b.SetBytes(totalPayload)
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for chunk := 0; chunk < chunks; chunk++ {
			if _, err := writer.Write(payload); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportMetric(float64(writer.bytes/int64(b.N)), "wire-bytes/op")
}

type binaryStreamCountingWriter struct {
	bytes int64
}

func (writer *binaryStreamCountingWriter) Write(data []byte) (int, error) {
	writer.bytes += int64(len(data))
	return len(data), nil
}
