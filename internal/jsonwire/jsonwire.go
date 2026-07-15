package jsonwire

import (
	"bytes"
	"compress/gzip"
	"io"
	"math"
	"strconv"
	"sync"
	"unicode/utf8"

	json "github.com/goccy/go-json"
)

type Decoder = json.Decoder

var gzipWriterPool = sync.Pool{
	New: func() interface{} {
		writer, err := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
		if err != nil {
			panic(err)
		}
		return writer
	},
}

func AcquireGzipWriter(writer io.Writer) *gzip.Writer {
	gzipWriter := gzipWriterPool.Get().(*gzip.Writer)
	gzipWriter.Reset(writer)
	return gzipWriter
}

func ReleaseGzipWriter(writer *gzip.Writer) {
	writer.Reset(io.Discard)
	gzipWriterPool.Put(writer)
}

func Marshal(value interface{}) ([]byte, error) {
	return json.MarshalWithOption(value, json.DisableHTMLEscape())
}

func NewEncoder(writer io.Writer) *json.Encoder {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	return encoder
}

func NewDecoder(reader io.Reader) *json.Decoder {
	return json.NewDecoder(reader)
}

func RequestBody(value interface{}, estimatedSize int, compressionThreshold int) (io.Reader, string, error) {
	if compressionThreshold > 0 && estimatedSize >= compressionThreshold {
		return StreamingGzipJSONReader(value), "gzip", nil
	}
	data, err := Marshal(value)
	if err != nil {
		return nil, "", err
	}
	return EncodedRequestBody(data, compressionThreshold)
}

func EncodedRequestBody(data []byte, compressionThreshold int) (io.Reader, string, error) {
	if compressionThreshold <= 0 || len(data) < compressionThreshold {
		return bytes.NewReader(data), "", nil
	}
	return StreamingGzipBytesReader(data), "gzip", nil
}

func StreamingGzipJSONReader(value interface{}) io.Reader {
	reader, writer := io.Pipe()
	return &StreamingGzipJSONBody{
		reader: reader,
		writer: writer,
		value:  value,
		done:   make(chan struct{}),
	}
}

func StreamingGzipBytesReader(data []byte) io.Reader {
	reader, writer := io.Pipe()
	return &StreamingGzipBytesBody{
		reader: reader,
		writer: writer,
		data:   data,
		done:   make(chan struct{}),
	}
}

type StreamingGzipBytesBody struct {
	mu       sync.Mutex
	reader   *io.PipeReader
	writer   *io.PipeWriter
	data     []byte
	started  bool
	closed   bool
	closeErr error
	done     chan struct{}
}

func (body *StreamingGzipBytesBody) Read(data []byte) (int, error) {
	body.mu.Lock()
	if body.closed {
		body.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if !body.started {
		body.started = true
		body.write()
	}
	body.mu.Unlock()
	return body.reader.Read(data)
}

func (body *StreamingGzipBytesBody) Close() error {
	body.mu.Lock()
	if body.closed {
		err := body.closeErr
		started := body.started
		done := body.done
		body.mu.Unlock()
		if started {
			<-done
		}
		return err
	}
	body.closed = true
	started := body.started
	done := body.done
	body.mu.Unlock()

	err := body.reader.Close()

	body.mu.Lock()
	body.closeErr = err
	body.mu.Unlock()
	if started {
		<-done
	}
	return err
}

func (body *StreamingGzipBytesBody) write() {
	go func() {
		defer close(body.done)
		gzipWriter := AcquireGzipWriter(body.writer)
		_, writeErr := gzipWriter.Write(body.data)
		closeErr := gzipWriter.Close()
		ReleaseGzipWriter(gzipWriter)
		if writeErr != nil {
			_ = body.writer.CloseWithError(writeErr)
			return
		}
		_ = body.writer.CloseWithError(closeErr)
	}()
}

type StreamingGzipJSONBody struct {
	mu       sync.Mutex
	reader   *io.PipeReader
	writer   *io.PipeWriter
	value    interface{}
	started  bool
	closed   bool
	closeErr error
	done     chan struct{}
}

func (body *StreamingGzipJSONBody) Read(data []byte) (int, error) {
	body.mu.Lock()
	if body.closed {
		body.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if !body.started {
		body.started = true
		body.write()
	}
	body.mu.Unlock()
	return body.reader.Read(data)
}

func (body *StreamingGzipJSONBody) Close() error {
	body.mu.Lock()
	if body.closed {
		err := body.closeErr
		started := body.started
		done := body.done
		body.mu.Unlock()
		if started {
			<-done
		}
		return err
	}
	body.closed = true
	started := body.started
	done := body.done
	body.mu.Unlock()

	err := body.reader.Close()

	body.mu.Lock()
	body.closeErr = err
	body.mu.Unlock()
	if started {
		<-done
	}
	return err
}

func (body *StreamingGzipJSONBody) write() {
	go func() {
		defer close(body.done)
		gzipWriter := AcquireGzipWriter(body.writer)
		encodeErr := NewEncoder(gzipWriter).Encode(body.value)
		closeErr := gzipWriter.Close()
		ReleaseGzipWriter(gzipWriter)
		if encodeErr != nil {
			_ = body.writer.CloseWithError(encodeErr)
			return
		}
		_ = body.writer.CloseWithError(closeErr)
	}()
}

func EstimateJSONValueBytes(value interface{}, threshold int) int {
	switch value := value.(type) {
	case nil:
		return 4
	case string:
		return EstimateJSONStringBytes(value)
	case json.Number:
		return len(value.String())
	case bool:
		if value {
			return 4
		}
		return 5
	case int:
		return estimateJSONIntBytes(int64(value))
	case int8:
		return estimateJSONIntBytes(int64(value))
	case int16:
		return estimateJSONIntBytes(int64(value))
	case int32:
		return estimateJSONIntBytes(int64(value))
	case int64:
		return estimateJSONIntBytes(value)
	case uint:
		return estimateJSONUintBytes(uint64(value))
	case uint8:
		return estimateJSONUintBytes(uint64(value))
	case uint16:
		return estimateJSONUintBytes(uint64(value))
	case uint32:
		return estimateJSONUintBytes(uint64(value))
	case uint64:
		return estimateJSONUintBytes(value)
	case float32:
		return estimateJSONFloatBytes(float64(value), 32)
	case float64:
		return estimateJSONFloatBytes(value, 64)
	case []interface{}:
		estimate := 2
		for idx, item := range value {
			if idx > 0 {
				estimate = AddEstimate(estimate, 1, threshold)
				if estimate >= threshold {
					return threshold
				}
			}
			estimate = AddEstimate(estimate, EstimateJSONValueBytes(item, threshold), threshold)
			if estimate >= threshold {
				return threshold
			}
		}
		return estimate
	case []string:
		estimate := 2
		for idx, item := range value {
			if idx > 0 {
				estimate = AddEstimate(estimate, 1, threshold)
				if estimate >= threshold {
					return threshold
				}
			}
			estimate = AddEstimate(estimate, EstimateJSONStringBytes(item), threshold)
			if estimate >= threshold {
				return threshold
			}
		}
		return estimate
	case map[string]interface{}:
		estimate := 2
		idx := 0
		for key, item := range value {
			if idx > 0 {
				estimate = AddEstimate(estimate, 1, threshold)
				if estimate >= threshold {
					return threshold
				}
			}
			idx++
			estimate = AddEstimate(estimate, EstimateJSONStringBytes(key)+1, threshold)
			if estimate >= threshold {
				return threshold
			}
			estimate = AddEstimate(estimate, EstimateJSONValueBytes(item, threshold), threshold)
			if estimate >= threshold {
				return threshold
			}
		}
		return estimate
	case map[string]string:
		estimate := 2
		idx := 0
		for key, item := range value {
			if idx > 0 {
				estimate = AddEstimate(estimate, 1, threshold)
				if estimate >= threshold {
					return threshold
				}
			}
			idx++
			estimate = AddEstimate(estimate, EstimateJSONStringBytes(key)+1, threshold)
			if estimate >= threshold {
				return threshold
			}
			estimate = AddEstimate(estimate, EstimateJSONStringBytes(item), threshold)
			if estimate >= threshold {
				return threshold
			}
		}
		return estimate
	default:
		return 0
	}
}

func estimateJSONIntBytes(value int64) int {
	var scratch [20]byte
	return len(strconv.AppendInt(scratch[:0], value, 10))
}

func estimateJSONUintBytes(value uint64) int {
	var scratch [20]byte
	return len(strconv.AppendUint(scratch[:0], value, 10))
}

func estimateJSONFloatBytes(value float64, bitSize int) int {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	var scratch [32]byte
	return len(strconv.AppendFloat(scratch[:0], value, 'g', -1, bitSize))
}

func EstimateJSONStringBytes(value string) int {
	estimate := 2
	for _, char := range value {
		switch char {
		case '\\', '"', '\b', '\f', '\n', '\r', '\t':
			estimate += 2
		default:
			if char < 0x20 {
				estimate += 6
			} else {
				size := utf8.RuneLen(char)
				if size < 0 {
					size = len("\ufffd")
				}
				estimate += size
			}
		}
	}
	return estimate
}

func AddEstimate(total, value, threshold int) int {
	if threshold <= 0 {
		return total
	}
	if value >= threshold || total >= threshold-value {
		return threshold
	}
	return total + value
}
