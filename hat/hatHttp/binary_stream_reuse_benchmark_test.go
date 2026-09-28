package hatHttp

import (
	"bytes"
	"context"
	"io"
	"testing"
)

type binaryStreamReuseReader struct {
	data   []byte
	offset int
}

func (reader *binaryStreamReuseReader) Read(data []byte) (int, error) {
	if reader.offset >= len(reader.data) {
		return 0, io.EOF
	}
	count := copy(data, reader.data[reader.offset:])
	reader.offset += count
	return count, nil
}

func binaryStreamReuseFixture() []byte {
	var encoded bytes.Buffer
	writer, _ := NewBinaryStreamWriter(&encoded, BinaryStreamOptions{MaxChunkBytes: 256})
	for index := 0; index < 64; index++ {
		payload := bytes.Repeat([]byte{byte(index)}, 256)
		_ = writer.WriteChunk(context.Background(), payload)
	}
	_ = writer.End(context.Background())
	return encoded.Bytes()
}

var binaryStreamReuseSink int

func BenchmarkBinaryStreamReaderNextAllocating(b *testing.B) {
	fixture := binaryStreamReuseFixture()
	b.SetBytes(int64(len(fixture)))
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		reader, _ := NewBinaryStreamReader(&binaryStreamReuseReader{data: fixture}, BinaryStreamOptions{MaxChunkBytes: 256})
		frames := 0
		for {
			frame, err := reader.Next(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			frames += len(frame.Payload)
			if frame.Kind == BinaryStreamFrameEnd {
				break
			}
		}
		binaryStreamReuseSink = frames
	}
}

func BenchmarkBinaryStreamReaderNextInto(b *testing.B) {
	fixture := binaryStreamReuseFixture()
	b.SetBytes(int64(len(fixture)))
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		reader, _ := NewBinaryStreamReader(&binaryStreamReuseReader{data: fixture}, BinaryStreamOptions{MaxChunkBytes: 256})
		payload := make([]byte, 256)
		frames := 0
		for {
			frame, err := reader.NextInto(context.Background(), payload)
			if err != nil {
				b.Fatal(err)
			}
			frames += len(frame.Payload)
			if frame.Kind == BinaryStreamFrameEnd {
				break
			}
		}
		binaryStreamReuseSink = frames
	}
}
