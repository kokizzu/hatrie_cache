package hatHttp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

func TestBinaryStreamReaderNextIntoReusesCallerBuffer(t *testing.T) {
	var encoded bytes.Buffer
	writer, err := NewBinaryStreamWriter(&encoded, BinaryStreamOptions{MaxChunkBytes: 8})
	if err != nil {
		t.Fatalf("NewBinaryStreamWriter() error = %v", err)
	}
	if err := writer.WriteChunk(context.Background(), []byte("one")); err != nil {
		t.Fatalf("WriteChunk(one) error = %v", err)
	}
	if err := writer.WriteChunk(context.Background(), []byte("two")); err != nil {
		t.Fatalf("WriteChunk(two) error = %v", err)
	}
	if err := writer.End(context.Background()); err != nil {
		t.Fatalf("End() error = %v", err)
	}

	reader, err := NewBinaryStreamReader(bytes.NewReader(encoded.Bytes()), BinaryStreamOptions{MaxChunkBytes: 8})
	if err != nil {
		t.Fatalf("NewBinaryStreamReader() error = %v", err)
	}
	buffer := make([]byte, 8)
	frame, err := reader.NextInto(context.Background(), buffer)
	if err != nil {
		t.Fatalf("NextInto(one) error = %v", err)
	}
	if got, want := string(frame.Payload), "one"; got != want {
		t.Fatalf("first payload = %q, want %q", got, want)
	}
	if len(frame.Payload) == 0 || &frame.Payload[0] != &buffer[0] {
		t.Fatal("first payload does not reuse caller buffer")
	}

	frame, err = reader.NextInto(context.Background(), buffer)
	if err != nil {
		t.Fatalf("NextInto(two) error = %v", err)
	}
	if got, want := string(frame.Payload), "two"; got != want {
		t.Fatalf("second payload = %q, want %q", got, want)
	}
	if frame.Kind != BinaryStreamFrameData || frame.Sequence != 1 {
		t.Fatalf("second frame = %#v, want data sequence 1", frame)
	}

	frame, err = reader.NextInto(context.Background(), buffer)
	if err != nil {
		t.Fatalf("NextInto(end) error = %v", err)
	}
	if frame.Kind != BinaryStreamFrameEnd || len(frame.Payload) != 0 {
		t.Fatalf("terminal frame = %#v, want empty end frame", frame)
	}
	if _, err := reader.NextInto(context.Background(), buffer); !errors.Is(err, io.EOF) {
		t.Fatalf("NextInto(after end) error = %v, want io.EOF", err)
	}
}

func TestBinaryStreamReaderNextIntoRejectsTooSmallBuffer(t *testing.T) {
	var encoded bytes.Buffer
	writer, err := NewBinaryStreamWriter(&encoded, BinaryStreamOptions{MaxChunkBytes: 8})
	if err != nil {
		t.Fatalf("NewBinaryStreamWriter() error = %v", err)
	}
	if err := writer.WriteChunk(context.Background(), []byte("payload")); err != nil {
		t.Fatalf("WriteChunk() error = %v", err)
	}
	if err := writer.End(context.Background()); err != nil {
		t.Fatalf("End() error = %v", err)
	}
	reader, err := NewBinaryStreamReader(bytes.NewReader(encoded.Bytes()), BinaryStreamOptions{MaxChunkBytes: 8})
	if err != nil {
		t.Fatalf("NewBinaryStreamReader() error = %v", err)
	}
	if _, err := reader.NextInto(context.Background(), make([]byte, 2)); !errors.Is(err, ErrBinaryStreamBufferTooSmall) {
		t.Fatalf("NextInto(small buffer) error = %v, want ErrBinaryStreamBufferTooSmall", err)
	}
	if _, err := reader.NextInto(context.Background(), make([]byte, 8)); !errors.Is(err, io.EOF) {
		t.Fatalf("NextInto(after small buffer) error = %v, want io.EOF", err)
	}
}
