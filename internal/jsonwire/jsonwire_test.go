package jsonwire

import (
	"bytes"
	"compress/gzip"
	stdjson "encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	gojson "github.com/goccy/go-json"
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
	if err := stdjson.NewDecoder(reader).Decode(&decoded); err != nil {
		t.Fatalf("Decode(streaming body) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, value) {
		t.Fatalf("decoded = %#v, want %#v", decoded, value)
	}
}

func TestStreamingGzipJSONReaderCloseClosesPipe(t *testing.T) {
	body := StreamingGzipJSONReader(map[string]interface{}{"ok": true})
	closer, ok := body.(io.Closer)
	if !ok {
		t.Fatalf("StreamingGzipJSONReader() body = %T, want io.Closer", body)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	buffer := make([]byte, 1)
	if _, err := body.Read(buffer); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Read(closed body) error = %v, want io.ErrClosedPipe", err)
	}
}

func TestStreamingGzipJSONReaderPropagatesEncodeErrors(t *testing.T) {
	body := StreamingGzipJSONReader(map[string]interface{}{"bad": func() {}})
	if _, err := io.ReadAll(body); err == nil {
		t.Fatal("ReadAll(unsupported value) error = nil, want encode error")
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

func TestEstimateJSONValueBytesScalars(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value interface{}
		want  int
	}{
		{name: "nil", value: nil, want: 4},
		{name: "string", value: "abc", want: 5},
		{name: "number", value: gojson.Number("123.5"), want: 5},
		{name: "true", value: true, want: 4},
		{name: "false", value: false, want: 5},
		{name: "int", value: int64(-7), want: 20},
		{name: "uint", value: uint64(7), want: 20},
		{name: "float32", value: float32(1.25), want: 15},
		{name: "float64", value: float64(1.25), want: 24},
		{name: "unsupported", value: struct{}{}, want: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := EstimateJSONValueBytes(tt.value, 1024); got != tt.want {
				t.Fatalf("EstimateJSONValueBytes(%s) = %d, want %d", tt.name, got, tt.want)
			}
		})
	}
}

func TestEstimateJSONValueBytesCollectionsWithoutCap(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value interface{}
		want  int
	}{
		{name: "slice", value: []interface{}{"a", gojson.Number("12"), true}, want: 11},
		{name: "string slice", value: []string{"a", "bb"}, want: 9},
		{name: "map", value: map[string]interface{}{"n": gojson.Number("12"), "b": false}, want: 11},
		{name: "string map", value: map[string]string{"a": "b"}, want: 6},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := EstimateJSONValueBytes(tt.value, 1024); got != tt.want {
				t.Fatalf("EstimateJSONValueBytes(%s) = %d, want %d", tt.name, got, tt.want)
			}
		})
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
	if got := AddEstimate(7, 5, threshold); got != 12 {
		t.Fatalf("AddEstimate(uncapped) = %d, want 12", got)
	}
	if got := AddEstimate(120, 12, threshold); got != threshold {
		t.Fatalf("AddEstimate(capped) = %d, want threshold %d", got, threshold)
	}
	if got := AddEstimate(120, 12, 0); got != 120 {
		t.Fatalf("AddEstimate(disabled threshold) = %d, want total 120", got)
	}
}
