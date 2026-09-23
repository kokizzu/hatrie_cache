package hatHttp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBinaryStreamRoundTripSequenceAndEnd(t *testing.T) {
	var wire bytes.Buffer
	options := BinaryStreamOptions{MaxChunkBytes: 32, MaxErrorBytes: 16}
	stream, err := NewBinaryStreamWriter(&wire, options)
	if err != nil {
		t.Fatalf("NewBinaryStreamWriter() error = %v", err)
	}
	ctx := context.Background()
	for _, payload := range [][]byte{[]byte("first"), []byte("second")} {
		if err := stream.WriteChunk(ctx, payload); err != nil {
			t.Fatalf("WriteChunk() error = %v", err)
		}
	}
	if err := stream.End(ctx); err != nil {
		t.Fatalf("End() error = %v", err)
	}

	reader, err := NewBinaryStreamReader(bytes.NewReader(wire.Bytes()), options)
	if err != nil {
		t.Fatalf("NewBinaryStreamReader() error = %v", err)
	}
	for sequence, want := range [][]byte{[]byte("first"), []byte("second")} {
		frame, err := reader.Next(ctx)
		if err != nil {
			t.Fatalf("Next(%d) error = %v", sequence, err)
		}
		if frame.Kind != BinaryStreamFrameData || frame.Sequence != uint64(sequence) || !bytes.Equal(frame.Payload, want) {
			t.Fatalf("frame[%d] = %#v, want data sequence %d payload %q", sequence, frame, sequence, want)
		}
	}
	frame, err := reader.Next(ctx)
	if err != nil {
		t.Fatalf("terminal Next() error = %v", err)
	}
	if frame.Kind != BinaryStreamFrameEnd || frame.Sequence != 2 {
		t.Fatalf("terminal frame = %#v, want end sequence 2", frame)
	}
	if _, err := reader.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after end error = %v, want io.EOF", err)
	}
}

func TestBinaryStreamRejectsOversizeAndKeepsWriterUsable(t *testing.T) {
	var wire bytes.Buffer
	stream, err := NewBinaryStreamWriter(&wire, BinaryStreamOptions{MaxChunkBytes: 3})
	if err != nil {
		t.Fatalf("NewBinaryStreamWriter() error = %v", err)
	}
	if err := stream.WriteChunk(context.Background(), []byte("four")); !errors.Is(err, ErrBinaryStreamChunkTooLarge) {
		t.Fatalf("oversize error = %v, want %v", err, ErrBinaryStreamChunkTooLarge)
	}
	if wire.Len() != 0 {
		t.Fatalf("oversize write emitted %d bytes", wire.Len())
	}
	if err := stream.WriteChunk(context.Background(), []byte("ok")); err != nil {
		t.Fatalf("valid WriteChunk() error = %v", err)
	}
}

func TestBinaryStreamProducerErrorWritesTerminalError(t *testing.T) {
	var wire bytes.Buffer
	wantErr := errors.New("producer failed")
	err := StreamBinary(context.Background(), &wire, BinaryStreamOptions{MaxChunkBytes: 32, MaxErrorBytes: 64}, func(ctx context.Context, stream *BinaryStreamWriter) error {
		if err := stream.WriteChunk(ctx, []byte("before-error")); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("StreamBinary() error = %v, want %v", err, wantErr)
	}
	reader, err := NewBinaryStreamReader(bytes.NewReader(wire.Bytes()), BinaryStreamOptions{MaxChunkBytes: 32, MaxErrorBytes: 64})
	if err != nil {
		t.Fatalf("NewBinaryStreamReader() error = %v", err)
	}
	if frame, err := reader.Next(context.Background()); err != nil || frame.Kind != BinaryStreamFrameData {
		t.Fatalf("data frame = %#v, error = %v", frame, err)
	}
	frame, err := reader.Next(context.Background())
	if err != nil {
		t.Fatalf("error frame read error = %v", err)
	}
	if frame.Kind != BinaryStreamFrameError || frame.Sequence != 1 || frame.Error != wantErr.Error() {
		t.Fatalf("error frame = %#v, want sequence 1 and %q", frame, wantErr)
	}
	if _, err := reader.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after error = %v, want io.EOF", err)
	}
}

func TestBinaryStreamCancellationAndPartialWriteClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var wire bytes.Buffer
	stream, err := NewBinaryStreamWriter(&wire, BinaryStreamOptions{MaxChunkBytes: 16})
	if err != nil {
		t.Fatalf("NewBinaryStreamWriter() error = %v", err)
	}
	if err := stream.WriteChunk(ctx, []byte("blocked")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled WriteChunk() error = %v, want context.Canceled", err)
	}
	if wire.Len() != 0 {
		t.Fatalf("cancelled write emitted %d bytes", wire.Len())
	}

	failing := &binaryStreamFailWriter{limit: 3, err: errors.New("short write")}
	partial, err := NewBinaryStreamWriter(failing, BinaryStreamOptions{MaxChunkBytes: 16})
	if err != nil {
		t.Fatalf("NewBinaryStreamWriter(failing) error = %v", err)
	}
	if err := partial.WriteChunk(context.Background(), []byte("payload")); err == nil {
		t.Fatal("partial WriteChunk() unexpectedly succeeded")
	}
	if err := partial.WriteChunk(context.Background(), []byte("again")); !errors.Is(err, ErrBinaryStreamClosed) {
		t.Fatalf("WriteChunk after partial failure = %v, want %v", err, ErrBinaryStreamClosed)
	}
}

func TestStreamBinaryHTTPResponseFlushesAndSetsContentType(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/stream", nil)
	response := &binaryStreamFlushRecorder{ResponseRecorder: httptest.NewRecorder()}
	err := StreamBinaryHTTPResponse(response, request, BinaryStreamOptions{MaxChunkBytes: 32}, func(ctx context.Context, stream *BinaryStreamWriter) error {
		return stream.WriteChunk(ctx, []byte("http-payload"))
	})
	if err != nil {
		t.Fatalf("StreamBinaryHTTPResponse() error = %v", err)
	}
	if got := response.Header().Get("Content-Type"); got != BinaryStreamContentType {
		t.Fatalf("Content-Type = %q, want %q", got, BinaryStreamContentType)
	}
	if response.flushes < 2 {
		t.Fatalf("flush count = %d, want data and terminal flushes", response.flushes)
	}
	reader, err := NewBinaryStreamReader(response.Body, BinaryStreamOptions{MaxChunkBytes: 32})
	if err != nil {
		t.Fatalf("NewBinaryStreamReader(http) error = %v", err)
	}
	frame, err := reader.Next(context.Background())
	if err != nil || frame.Kind != BinaryStreamFrameData || string(frame.Payload) != "http-payload" {
		t.Fatalf("HTTP data frame = %#v, error = %v", frame, err)
	}
}

func TestBinaryStreamReaderRejectsCorruptChecksum(t *testing.T) {
	var wire bytes.Buffer
	stream, err := NewBinaryStreamWriter(&wire, BinaryStreamOptions{MaxChunkBytes: 16})
	if err != nil {
		t.Fatalf("NewBinaryStreamWriter() error = %v", err)
	}
	if err := stream.WriteChunk(context.Background(), []byte("checksum")); err != nil {
		t.Fatalf("WriteChunk() error = %v", err)
	}
	encoded := wire.Bytes()
	encoded[len(encoded)-1] ^= 0x01
	reader, err := NewBinaryStreamReader(bytes.NewReader(encoded), BinaryStreamOptions{MaxChunkBytes: 16})
	if err != nil {
		t.Fatalf("NewBinaryStreamReader() error = %v", err)
	}
	if _, err := reader.Next(context.Background()); !errors.Is(err, ErrBinaryStreamChecksum) {
		t.Fatalf("corrupt frame error = %v, want %v", err, ErrBinaryStreamChecksum)
	}
}

func TestBinaryStreamReaderRejectsSequenceGapAndTruncation(t *testing.T) {
	var wire bytes.Buffer
	stream, err := NewBinaryStreamWriter(&wire, BinaryStreamOptions{MaxChunkBytes: 16})
	if err != nil {
		t.Fatalf("NewBinaryStreamWriter() error = %v", err)
	}
	if err := stream.WriteChunk(context.Background(), []byte("sequence")); err != nil {
		t.Fatalf("WriteChunk() error = %v", err)
	}
	gap := append([]byte(nil), wire.Bytes()...)
	gap[8] = 1
	reader, err := NewBinaryStreamReader(bytes.NewReader(gap), BinaryStreamOptions{MaxChunkBytes: 16})
	if err != nil {
		t.Fatalf("NewBinaryStreamReader(gap) error = %v", err)
	}
	if _, err := reader.Next(context.Background()); !errors.Is(err, ErrBinaryStreamSequence) {
		t.Fatalf("sequence gap error = %v, want %v", err, ErrBinaryStreamSequence)
	}

	reader, err = NewBinaryStreamReader(bytes.NewReader(wire.Bytes()), BinaryStreamOptions{MaxChunkBytes: 16})
	if err != nil {
		t.Fatalf("NewBinaryStreamReader(truncated) error = %v", err)
	}
	if _, err := reader.Next(context.Background()); err != nil {
		t.Fatalf("truncated data frame error = %v", err)
	}
	if _, err := reader.Next(context.Background()); !errors.Is(err, ErrBinaryStreamTruncated) {
		t.Fatalf("missing terminal frame error = %v, want %v", err, ErrBinaryStreamTruncated)
	}
}

func TestBinaryStreamErrorPayloadIsBounded(t *testing.T) {
	var wire bytes.Buffer
	wantErr := errors.New(strings.Repeat("x", 64))
	_ = StreamBinary(context.Background(), &wire, BinaryStreamOptions{MaxChunkBytes: 16, MaxErrorBytes: 7}, func(context.Context, *BinaryStreamWriter) error {
		return wantErr
	})
	reader, err := NewBinaryStreamReader(bytes.NewReader(wire.Bytes()), BinaryStreamOptions{MaxChunkBytes: 16, MaxErrorBytes: 7})
	if err != nil {
		t.Fatalf("NewBinaryStreamReader() error = %v", err)
	}
	frame, err := reader.Next(context.Background())
	if err != nil {
		t.Fatalf("error frame read error = %v", err)
	}
	if frame.Kind != BinaryStreamFrameError || len(frame.Error) != 7 {
		t.Fatalf("bounded error frame = %#v, want 7-byte error", frame)
	}
}

type binaryStreamFailWriter struct {
	limit   int
	written int
	err     error
}

type binaryStreamFlushRecorder struct {
	*httptest.ResponseRecorder
	flushes int
}

func (recorder *binaryStreamFlushRecorder) Flush() {
	recorder.flushes++
	recorder.ResponseRecorder.Flush()
}

func (writer *binaryStreamFailWriter) Write(data []byte) (int, error) {
	if writer.written >= writer.limit {
		return 0, writer.err
	}
	remaining := writer.limit - writer.written
	if remaining > len(data) {
		remaining = len(data)
	}
	writer.written += remaining
	if remaining < len(data) {
		return remaining, writer.err
	}
	return remaining, nil
}
