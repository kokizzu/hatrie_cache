package jsonwire

import (
	"compress/gzip"
	"io"
	"sync"

	json "github.com/goccy/go-json"
)

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
		encodeErr := json.NewEncoder(gzipWriter).Encode(body.value)
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
		return len(value) + 2
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
		for _, item := range value {
			estimate = AddEstimate(estimate, EstimateJSONValueBytes(item, threshold), threshold)
			if estimate >= threshold {
				return threshold
			}
		}
		return estimate
	case []string:
		estimate := 2
		for _, item := range value {
			estimate = AddEstimate(estimate, len(item)+2, threshold)
			if estimate >= threshold {
				return threshold
			}
		}
		return estimate
	case map[string]interface{}:
		estimate := 2
		for key, item := range value {
			estimate = AddEstimate(estimate, len(key)+EstimateJSONValueBytes(item, threshold), threshold)
			if estimate >= threshold {
				return threshold
			}
		}
		return estimate
	case map[string]string:
		estimate := 2
		for key, item := range value {
			estimate = AddEstimate(estimate, len(key)+len(item)+2, threshold)
			if estimate >= threshold {
				return threshold
			}
		}
		return estimate
	default:
		return 0
	}
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
