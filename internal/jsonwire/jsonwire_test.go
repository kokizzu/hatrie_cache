package jsonwire

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestStreamingGzipJSONReaderEncodesValue(t *testing.T) {
	value := map[string]interface{}{
		"command": "SETSTR",
		"key":     "session:1",
		"value":   strings.Repeat("x", 1024),
	}
	body := StreamingGzipJSONReader(value)

	reader, err := gzip.NewReader(body)
	if err != nil {
		t.Fatalf("NewReader(streaming body) error = %v", err)
	}
	defer reader.Close()

	var decoded map[string]interface{}
	if err := json.NewDecoder(reader).Decode(&decoded); err != nil {
		t.Fatalf("Decode(streaming body) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, value) {
		t.Fatalf("decoded = %#v, want %#v", decoded, value)
	}
}

func TestAcquireGzipWriterCompressesAndReleases(t *testing.T) {
	var compressed bytes.Buffer
	writer := AcquireGzipWriter(&compressed)
	if _, err := writer.Write([]byte("payload")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	ReleaseGzipWriter(writer)

	reader, err := gzip.NewReader(bytes.NewReader(compressed.Bytes()))
	if err != nil {
		t.Fatalf("NewReader(compressed) error = %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll(compressed) error = %v", err)
	}
	if string(data) != "payload" {
		t.Fatalf("decompressed = %q, want payload", data)
	}
}

func TestEstimateJSONValueBytesCapsNestedValues(t *testing.T) {
	threshold := 128
	value := []interface{}{
		map[string]interface{}{
			"name": strings.Repeat("x", threshold),
		},
	}
	if got := EstimateJSONValueBytes(value, threshold); got != threshold {
		t.Fatalf("EstimateJSONValueBytes() = %d, want threshold %d", got, threshold)
	}
	if got := AddEstimate(120, 12, threshold); got != threshold {
		t.Fatalf("AddEstimate(capped) = %d, want threshold %d", got, threshold)
	}
}
