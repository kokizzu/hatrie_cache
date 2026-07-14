package jsonwire

import (
	"bytes"
	"compress/gzip"
	"io"
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
	var compressed bytes.Buffer
	writer := AcquireGzipWriter(&compressed)
	_, writeErr := writer.Write(data)
	closeErr := writer.Close()
	ReleaseGzipWriter(writer)
	if writeErr != nil {
		return nil, "", writeErr
	}
	if closeErr != nil {
		return nil, "", closeErr
	}
	return bytes.NewReader(compressed.Bytes()), "gzip", nil
}

func StreamingGzipJSONReader(value interface{}) io.Reader {
	reader, writer := io.Pipe()
	return &StreamingGzipJSONBody{
		reader: reader,
		writer: writer,
		value:  value,
	}
}

type StreamingGzipJSONBody struct {
	start  sync.Once
	reader *io.PipeReader
	writer *io.PipeWriter
	value  interface{}
}

func (body *StreamingGzipJSONBody) Read(data []byte) (int, error) {
	body.start.Do(body.write)
	return body.reader.Read(data)
}

func (body *StreamingGzipJSONBody) Close() error {
	return body.reader.Close()
}

func (body *StreamingGzipJSONBody) write() {
	go func() {
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
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return 20
	case float32:
		return 15
	case float64:
		return 24
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
