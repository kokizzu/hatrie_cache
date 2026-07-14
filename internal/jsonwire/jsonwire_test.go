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

func TestMarshalDisablesHTMLEscape(t *testing.T) {
	data, err := Marshal(struct {
		Value string `json:"value"`
	}{
		Value: "<tag>&",
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if got, want := string(data), `{"value":"<tag>&"}`; got != want {
		t.Fatalf("Marshal() = %q, want %q", got, want)
	}

	var decoded map[string]string
	if err := stdjson.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("encoding/json Unmarshal() error = %v", err)
	}
	if decoded["value"] != "<tag>&" {
		t.Fatalf("decoded value = %q, want <tag>&", decoded["value"])
	}
}

func TestNewEncoderDisablesHTMLEscape(t *testing.T) {
	var out bytes.Buffer
	if err := NewEncoder(&out).Encode(struct {
		Value string `json:"value"`
	}{
		Value: "<tag>&",
	}); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if got, want := strings.TrimSpace(out.String()), `{"value":"<tag>&"}`; got != want {
		t.Fatalf("Encode() = %q, want %q", got, want)
	}
}

func TestRequestBodyLeavesSmallPayloadPlain(t *testing.T) {
	body, contentEncoding, err := RequestBody(struct {
		Value string `json:"value"`
	}{
		Value: "<tag>&",
	}, 0, 1024)
	if err != nil {
		t.Fatalf("RequestBody() error = %v", err)
	}
	if contentEncoding != "" {
		t.Fatalf("Content-Encoding = %q, want empty", contentEncoding)
	}
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("ReadAll(plain body) error = %v", err)
	}
	if got, want := string(data), `{"value":"<tag>&"}`; got != want {
		t.Fatalf("plain body = %q, want %q", got, want)
	}
}

func TestRequestBodyCompressesActualLargePayload(t *testing.T) {
	payload := struct {
		Value string `json:"value"`
	}{
		Value: strings.Repeat("<tag>&", 16),
	}
	body, contentEncoding, err := RequestBody(payload, 0, 16)
	if err != nil {
		t.Fatalf("RequestBody() error = %v", err)
	}
	if contentEncoding != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", contentEncoding)
	}

	reader, err := gzip.NewReader(body)
	if err != nil {
		t.Fatalf("NewReader(compressed body) error = %v", err)
	}
	defer reader.Close()

	var decoded struct {
		Value string `json:"value"`
	}
	if err := stdjson.NewDecoder(reader).Decode(&decoded); err != nil {
		t.Fatalf("Decode(compressed body) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, payload) {
		t.Fatalf("decoded body = %#v, want %#v", decoded, payload)
	}
}

func TestRequestBodyStreamsEstimatedLargePayload(t *testing.T) {
	body, contentEncoding, err := RequestBody(struct {
		Value string `json:"value"`
	}{
		Value: strings.Repeat("x", 32),
	}, 32, 16)
	if err != nil {
		t.Fatalf("RequestBody() error = %v", err)
	}
	if contentEncoding != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", contentEncoding)
	}
	if _, ok := body.(*StreamingGzipJSONBody); !ok {
		t.Fatalf("RequestBody() body = %T, want streaming gzip body", body)
	}
}

func TestRequestBodyReportsMarshalErrors(t *testing.T) {
	body, contentEncoding, err := RequestBody(map[string]interface{}{"bad": func() {}}, 0, 1024)
	if err == nil {
		t.Fatal("RequestBody(unsupported) error = nil, want error")
	}
	if body != nil {
		t.Fatalf("RequestBody(unsupported) body = %T, want nil", body)
	}
	if contentEncoding != "" {
		t.Fatalf("Content-Encoding = %q, want empty", contentEncoding)
	}
}

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
		{name: "escaped string", value: "a\"b\n", want: 8},
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
		{name: "slice", value: []interface{}{"a", gojson.Number("12"), true}, want: 13},
		{name: "string slice", value: []string{"a", "bb"}, want: 10},
		{name: "map", value: map[string]interface{}{"n": gojson.Number("12"), "b": false}, want: 18},
		{name: "string map", value: map[string]string{"a": "b"}, want: 9},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := EstimateJSONValueBytes(tt.value, 1024); got != tt.want {
				t.Fatalf("EstimateJSONValueBytes(%s) = %d, want %d", tt.name, got, tt.want)
			}
		})
	}
}

func TestEstimateJSONValueBytesDoesNotUndercountSupportedValues(t *testing.T) {
	for _, value := range []interface{}{
		"a\"b\n",
		[]interface{}{"a", gojson.Number("12"), true},
		[]string{"a", "bb"},
		map[string]interface{}{"n": gojson.Number("12"), "b": false},
		map[string]string{"a": "b"},
		map[string]interface{}{"escaped\"key": "line\nbreak"},
	} {
		data, err := Marshal(value)
		if err != nil {
			t.Fatalf("Marshal(%#v) error = %v", value, err)
		}
		if got := EstimateJSONValueBytes(value, 1024); got < len(data) {
			t.Fatalf("EstimateJSONValueBytes(%#v) = %d, below marshaled size %d (%s)", value, got, len(data), data)
		}
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
