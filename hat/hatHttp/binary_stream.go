package hatHttp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
)

const (
	// BinaryStreamContentType identifies the bounded framed binary response
	// format. It is deliberately opt-in; existing JSON and NDJSON endpoints do
	// not change their content type.
	BinaryStreamContentType = "application/vnd.hatrie.binary-stream"

	binaryStreamHeaderSize   = 24
	binaryStreamVersion      = 1
	binaryStreamMaxChunk     = 16 << 20
	binaryStreamMaxError     = 64 << 10
	binaryStreamDefaultChunk = 64 << 10
	binaryStreamDefaultError = 4 << 10
)

var binaryStreamMagic = [...]byte{'H', 'T', 'B', 'S'}
var binaryStreamCRCTable = crc32.MakeTable(crc32.Castagnoli)

var (
	// ErrBinaryStreamChunkTooLarge indicates a data or error payload over the
	// configured bound.
	ErrBinaryStreamChunkTooLarge = errors.New("hatHttp: binary stream chunk is too large")
	// ErrBinaryStreamClosed indicates that a terminal frame was already written
	// or a previous write failed after touching the underlying writer.
	ErrBinaryStreamClosed = errors.New("hatHttp: binary stream is closed")
	// ErrBinaryStreamInvalid indicates malformed frame metadata.
	ErrBinaryStreamInvalid = errors.New("hatHttp: binary stream frame is invalid")
	// ErrBinaryStreamChecksum indicates a frame payload checksum mismatch.
	ErrBinaryStreamChecksum = errors.New("hatHttp: binary stream checksum mismatch")
	// ErrBinaryStreamSequence indicates a frame sequence gap or duplicate.
	ErrBinaryStreamSequence = errors.New("hatHttp: binary stream sequence mismatch")
	// ErrBinaryStreamTruncated indicates that a stream ended before a terminal
	// frame or while a frame was being read.
	ErrBinaryStreamTruncated = errors.New("hatHttp: binary stream is truncated")
	// ErrBinaryStreamOptions indicates an invalid stream bound.
	ErrBinaryStreamOptions = errors.New("hatHttp: invalid binary stream options")
)

// BinaryStreamFrameKind identifies the payload meaning of one frame.
type BinaryStreamFrameKind byte

const (
	BinaryStreamFrameData  BinaryStreamFrameKind = 1
	BinaryStreamFrameEnd   BinaryStreamFrameKind = 2
	BinaryStreamFrameError BinaryStreamFrameKind = 3
)

// BinaryStreamOptions bounds one data frame and one terminal error message.
// Zero values select conservative defaults. Values above the protocol maximum
// are rejected so a peer cannot configure unbounded decoder allocations.
type BinaryStreamOptions struct {
	MaxChunkBytes int
	MaxErrorBytes int
}

type normalizedBinaryStreamOptions struct {
	maxChunkBytes int
	maxErrorBytes int
}

func (options BinaryStreamOptions) normalize() (normalizedBinaryStreamOptions, error) {
	chunkBytes := options.MaxChunkBytes
	if chunkBytes == 0 {
		chunkBytes = binaryStreamDefaultChunk
	}
	if chunkBytes < 1 || chunkBytes > binaryStreamMaxChunk {
		return normalizedBinaryStreamOptions{}, fmt.Errorf("%w: max chunk bytes %d", ErrBinaryStreamOptions, chunkBytes)
	}
	errorBytes := options.MaxErrorBytes
	if errorBytes == 0 {
		errorBytes = binaryStreamDefaultError
	}
	if errorBytes < 1 || errorBytes > binaryStreamMaxError {
		return normalizedBinaryStreamOptions{}, fmt.Errorf("%w: max error bytes %d", ErrBinaryStreamOptions, errorBytes)
	}
	return normalizedBinaryStreamOptions{maxChunkBytes: chunkBytes, maxErrorBytes: errorBytes}, nil
}

// BinaryStreamFrame is one decoded response frame. Error is populated for an
// error frame; Payload remains available for callers that need the exact bytes.
type BinaryStreamFrame struct {
	Kind     BinaryStreamFrameKind
	Sequence uint64
	Payload  []byte
	Error    string
}

// BinaryStreamWriter writes one bounded, ordered binary response. A writer is
// single-producer; callers must not invoke its methods concurrently.
type BinaryStreamWriter struct {
	writer   io.Writer
	options  normalizedBinaryStreamOptions
	sequence uint64
	closed   bool
	flush    func()
	header   [binaryStreamHeaderSize]byte
}

// NewBinaryStreamWriter creates a framed writer over writer.
func NewBinaryStreamWriter(writer io.Writer, options BinaryStreamOptions) (*BinaryStreamWriter, error) {
	return newBinaryStreamWriter(writer, options, nil)
}

func newBinaryStreamWriter(writer io.Writer, options BinaryStreamOptions, flush func()) (*BinaryStreamWriter, error) {
	if writer == nil {
		return nil, errors.New("hatHttp: binary stream writer is nil")
	}
	normalized, err := options.normalize()
	if err != nil {
		return nil, err
	}
	return &BinaryStreamWriter{writer: writer, options: normalized, flush: flush}, nil
}

// WriteChunk writes one data frame and advances its sequence only after the
// complete frame has reached the underlying writer.
func (stream *BinaryStreamWriter) WriteChunk(ctx context.Context, payload []byte) error {
	if err := stream.ready(ctx); err != nil {
		return err
	}
	if len(payload) > stream.options.maxChunkBytes {
		return ErrBinaryStreamChunkTooLarge
	}
	if err := stream.writeFrame(ctx, BinaryStreamFrameData, payload); err != nil {
		stream.closed = true
		return err
	}
	stream.sequence++
	return nil
}

// End writes the successful terminal frame. It is idempotent only through the
// returned ErrBinaryStreamClosed; callers should call it once.
func (stream *BinaryStreamWriter) End(ctx context.Context) error {
	if err := stream.ready(ctx); err != nil {
		return err
	}
	if err := stream.writeFrame(ctx, BinaryStreamFrameEnd, nil); err != nil {
		stream.closed = true
		return err
	}
	stream.closed = true
	return nil
}

// WriteError writes a bounded terminal error frame. The original error is
// returned by StreamBinary; the frame gives a remote reader a deterministic
// partial-stream failure without requiring a second response format.
func (stream *BinaryStreamWriter) WriteError(ctx context.Context, cause error) error {
	if err := stream.ready(ctx); err != nil {
		return err
	}
	message := "binary stream aborted"
	if cause != nil {
		message = cause.Error()
	}
	if len(message) > stream.options.maxErrorBytes {
		message = message[:stream.options.maxErrorBytes]
	}
	if err := stream.writeFrame(ctx, BinaryStreamFrameError, []byte(message)); err != nil {
		stream.closed = true
		return err
	}
	stream.closed = true
	return nil
}

func (stream *BinaryStreamWriter) ready(ctx context.Context) error {
	if stream == nil || stream.writer == nil || stream.closed {
		return ErrBinaryStreamClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return ctx.Err()
}

func (stream *BinaryStreamWriter) writeFrame(ctx context.Context, kind BinaryStreamFrameKind, payload []byte) error {
	if len(payload) > int(^uint32(0)) {
		return ErrBinaryStreamChunkTooLarge
	}
	header := &stream.header
	copy(header[:4], binaryStreamMagic[:])
	header[4] = binaryStreamVersion
	header[5] = byte(kind)
	binary.LittleEndian.PutUint64(header[8:16], stream.sequence)
	binary.LittleEndian.PutUint32(header[16:20], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[20:24], crc32.Checksum(payload, binaryStreamCRCTable))
	if err := writeBinaryStreamAll(ctx, stream.writer, header[:]); err != nil {
		return err
	}
	if err := writeBinaryStreamAll(ctx, stream.writer, payload); err != nil {
		return err
	}
	if stream.flush != nil {
		stream.flush()
	}
	return nil
}

// StreamBinary runs a bounded producer and emits a terminal end or error
// frame. Context cancellation deliberately stops without attempting a frame,
// because the peer connection is normally already closing.
func StreamBinary(ctx context.Context, writer io.Writer, options BinaryStreamOptions, produce func(context.Context, *BinaryStreamWriter) error) error {
	stream, err := newBinaryStreamWriter(writer, options, nil)
	if err != nil {
		return err
	}
	return stream.run(ctx, produce)
}

func (stream *BinaryStreamWriter) run(ctx context.Context, produce func(context.Context, *BinaryStreamWriter) error) error {
	if produce == nil {
		return errors.New("hatHttp: binary stream producer is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := produce(ctx, stream); err != nil {
		if ctx.Err() != nil || stream.closed {
			return err
		}
		if writeErr := stream.WriteError(ctx, err); writeErr != nil {
			return errors.Join(err, writeErr)
		}
		return err
	}
	if stream.closed {
		return nil
	}
	return stream.End(ctx)
}

// StreamBinaryHTTPResponse writes a framed response with a stable content
// type and flushes each complete frame when the server supports Flusher.
func StreamBinaryHTTPResponse(response http.ResponseWriter, request *http.Request, options BinaryStreamOptions, produce func(context.Context, *BinaryStreamWriter) error) error {
	if response == nil {
		return errors.New("hatHttp: binary stream response writer is nil")
	}
	ctx := context.Background()
	if request != nil {
		ctx = request.Context()
	}
	flusher, _ := response.(http.Flusher)
	flush := func() {}
	if flusher != nil {
		flush = flusher.Flush
	}
	stream, err := newBinaryStreamWriter(response, options, flush)
	if err != nil {
		return err
	}
	response.Header().Set("Content-Type", BinaryStreamContentType)
	response.Header().Set("Cache-Control", "no-store")
	response.WriteHeader(http.StatusOK)
	return stream.run(ctx, produce)
}

func writeBinaryStreamAll(ctx context.Context, writer io.Writer, data []byte) error {
	for len(data) > 0 {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		written, err := writer.Write(data)
		if written < 0 || written > len(data) {
			return io.ErrShortWrite
		}
		data = data[written:]
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// BinaryStreamReader validates frame bounds, checksums, and strictly
// increasing sequence numbers before returning a frame.
type BinaryStreamReader struct {
	reader   io.Reader
	options  normalizedBinaryStreamOptions
	sequence uint64
	done     bool
}

// NewBinaryStreamReader creates a bounded decoder over reader.
func NewBinaryStreamReader(reader io.Reader, options BinaryStreamOptions) (*BinaryStreamReader, error) {
	if reader == nil {
		return nil, errors.New("hatHttp: binary stream reader is nil")
	}
	normalized, err := options.normalize()
	if err != nil {
		return nil, err
	}
	return &BinaryStreamReader{reader: reader, options: normalized}, nil
}

// Next reads one frame. io.EOF after a valid terminal frame is the normal end
// condition; truncation or protocol errors permanently stop the reader.
func (reader *BinaryStreamReader) Next(ctx context.Context) (BinaryStreamFrame, error) {
	if reader == nil || reader.reader == nil || reader.done {
		return BinaryStreamFrame{}, io.EOF
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return BinaryStreamFrame{}, err
		}
	}
	var header [binaryStreamHeaderSize]byte
	read, err := io.ReadFull(reader.reader, header[:])
	if err != nil {
		if err == io.EOF && read == 0 {
			if reader.sequence != 0 {
				reader.done = true
				return BinaryStreamFrame{}, ErrBinaryStreamTruncated
			}
			return BinaryStreamFrame{}, io.EOF
		}
		reader.done = true
		return BinaryStreamFrame{}, fmt.Errorf("%w: header", ErrBinaryStreamTruncated)
	}
	if string(header[:4]) != string(binaryStreamMagic[:]) || header[4] != binaryStreamVersion || header[6] != 0 || header[7] != 0 {
		reader.done = true
		return BinaryStreamFrame{}, ErrBinaryStreamInvalid
	}
	kind := BinaryStreamFrameKind(header[5])
	if kind != BinaryStreamFrameData && kind != BinaryStreamFrameEnd && kind != BinaryStreamFrameError {
		reader.done = true
		return BinaryStreamFrame{}, ErrBinaryStreamInvalid
	}
	sequence := binary.LittleEndian.Uint64(header[8:16])
	length := binary.LittleEndian.Uint32(header[16:20])
	checksum := binary.LittleEndian.Uint32(header[20:24])
	if sequence != reader.sequence {
		reader.done = true
		return BinaryStreamFrame{}, ErrBinaryStreamSequence
	}
	maxLength := reader.options.maxChunkBytes
	switch kind {
	case BinaryStreamFrameEnd:
		if length != 0 {
			reader.done = true
			return BinaryStreamFrame{}, ErrBinaryStreamInvalid
		}
	case BinaryStreamFrameError:
		maxLength = reader.options.maxErrorBytes
	}
	if uint64(length) > uint64(maxLength) {
		reader.done = true
		return BinaryStreamFrame{}, ErrBinaryStreamChunkTooLarge
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(reader.reader, payload); err != nil {
		reader.done = true
		return BinaryStreamFrame{}, fmt.Errorf("%w: payload", ErrBinaryStreamTruncated)
	}
	if crc32.Checksum(payload, binaryStreamCRCTable) != checksum {
		reader.done = true
		return BinaryStreamFrame{}, ErrBinaryStreamChecksum
	}
	reader.sequence++
	frame := BinaryStreamFrame{Kind: kind, Sequence: sequence, Payload: payload}
	if kind == BinaryStreamFrameError {
		frame.Error = string(payload)
	}
	if kind == BinaryStreamFrameEnd || kind == BinaryStreamFrameError {
		reader.done = true
	}
	return frame, nil
}
