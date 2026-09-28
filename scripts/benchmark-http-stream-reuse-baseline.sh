#!/usr/bin/env bash
set -euo pipefail

baseline_file="hat/hatHttp/binary_stream_reuse_baseline_temp_test.go"
trap 'rm -f "$baseline_file"' EXIT
cat > "$baseline_file" <<'EOF'
package hatHttp

import (
	"bytes"
	"context"
	"io"
	"testing"
)

type binaryStreamBaselineReader struct {
	data   []byte
	offset int
}

func (reader *binaryStreamBaselineReader) Read(data []byte) (int, error) {
	if reader.offset >= len(reader.data) {
		return 0, io.EOF
	}
	count := copy(data, reader.data[reader.offset:])
	reader.offset += count
	return count, nil
}

func binaryStreamBaselineFixture() []byte {
	var encoded bytes.Buffer
	writer, _ := NewBinaryStreamWriter(&encoded, BinaryStreamOptions{MaxChunkBytes: 256})
	for index := 0; index < 64; index++ {
		payload := bytes.Repeat([]byte{byte(index)}, 256)
		_ = writer.WriteChunk(context.Background(), payload)
	}
	_ = writer.End(context.Background())
	return encoded.Bytes()
}

var binaryStreamBaselineSink int

func BenchmarkBinaryStreamReaderNextAllocatingBaseline(b *testing.B) {
	fixture := binaryStreamBaselineFixture()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		reader, _ := NewBinaryStreamReader(&binaryStreamBaselineReader{data: fixture}, BinaryStreamOptions{MaxChunkBytes: 256})
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
		binaryStreamBaselineSink = frames
	}
}
EOF
go test ./hat/hatHttp/binary_stream.go "$baseline_file" -run '^$' -bench '^BenchmarkBinaryStreamReaderNextAllocatingBaseline$' -benchmem -count=1
