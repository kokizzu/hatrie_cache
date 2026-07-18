package hatriecache

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

type commandWireTrackingBody struct {
	reader *strings.Reader
	read   bool
	closed bool
}

func (body *commandWireTrackingBody) Read(data []byte) (int, error) {
	body.read = true
	return body.reader.Read(data)
}

func (body *commandWireTrackingBody) Close() error {
	body.closed = true
	return nil
}

type discardCommandWireResponseWriter struct {
	header http.Header
	status int
	bytes  int
}

func newDiscardCommandWireResponseWriter() *discardCommandWireResponseWriter {
	return &discardCommandWireResponseWriter{header: http.Header{}}
}

func (writer *discardCommandWireResponseWriter) reset() {
	for key := range writer.header {
		delete(writer.header, key)
	}
	writer.status = 0
	writer.bytes = 0
}

func (writer *discardCommandWireResponseWriter) Header() http.Header {
	return writer.header
}

func (writer *discardCommandWireResponseWriter) Write(data []byte) (int, error) {
	writer.bytes += len(data)
	return len(data), nil
}

func (writer *discardCommandWireResponseWriter) WriteHeader(status int) {
	writer.status = status
}

func TestParseCommandWireFormatAliases(t *testing.T) {
	tests := []struct {
		value string
		want  CommandWireFormat
	}{
		{value: "", want: CommandWireFormatProtobuf},
		{value: " protobuf ", want: CommandWireFormatProtobuf},
		{value: "proto", want: CommandWireFormatProtobuf},
		{value: "pb", want: CommandWireFormatProtobuf},
		{value: "json", want: CommandWireFormatJSON},
		{value: "JSON", want: CommandWireFormatJSON},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			got, err := ParseCommandWireFormat(tt.value)
			if err != nil {
				t.Fatalf("ParseCommandWireFormat(%q) error = %v", tt.value, err)
			}
			if got != tt.want {
				t.Fatalf("ParseCommandWireFormat(%q) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
	if _, err := ParseCommandWireFormat("msgpack"); err == nil {
		t.Fatal("ParseCommandWireFormat(msgpack) error = nil, want error")
	}
}

func TestCommandWireFormatFromContentTypeAliases(t *testing.T) {
	tests := []struct {
		value string
		want  CommandWireFormat
	}{
		{value: "", want: CommandWireFormatJSON},
		{value: "application/json; charset=utf-8", want: CommandWireFormatJSON},
		{value: " Application/JSON ; charset=utf-8", want: CommandWireFormatJSON},
		{value: "text/json", want: CommandWireFormatJSON},
		{value: "application/x-protobuf", want: CommandWireFormatProtobuf},
		{value: "Application/X-Protobuf ; proto=cache", want: CommandWireFormatProtobuf},
		{value: "application/protobuf", want: CommandWireFormatProtobuf},
		{value: "application/octet-stream", want: CommandWireFormatProtobuf},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			got, ok := commandWireFormatFromContentType(tt.value)
			if !ok {
				t.Fatalf("commandWireFormatFromContentType(%q) ok = false, want true", tt.value)
			}
			if got != tt.want {
				t.Fatalf("commandWireFormatFromContentType(%q) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
	if got, ok := commandWireFormatFromContentType("application/xml"); ok || got != "" {
		t.Fatalf("commandWireFormatFromContentType(application/xml) = %q/%v, want unsupported", got, ok)
	}
}

func TestCommandWireFormatFromAcceptRespectsQuality(t *testing.T) {
	tests := []struct {
		name     string
		accept   string
		fallback CommandWireFormat
		want     CommandWireFormat
		wantOK   bool
	}{
		{
			name:     "empty uses fallback",
			fallback: CommandWireFormatProtobuf,
			want:     CommandWireFormatProtobuf,
			wantOK:   true,
		},
		{
			name:     "higher quality json beats protobuf",
			accept:   "application/x-protobuf;q=0.1, application/json;q=0.9",
			fallback: CommandWireFormatProtobuf,
			want:     CommandWireFormatJSON,
			wantOK:   true,
		},
		{
			name:     "higher quality protobuf beats json",
			accept:   "application/json;q=0.1, application/x-protobuf;q=0.9",
			fallback: CommandWireFormatJSON,
			want:     CommandWireFormatProtobuf,
			wantOK:   true,
		},
		{
			name:     "explicit q zero excludes fallback under wildcard",
			accept:   "application/x-protobuf;q=0, */*;q=1",
			fallback: CommandWireFormatProtobuf,
			want:     CommandWireFormatJSON,
			wantOK:   true,
		},
		{
			name:     "wildcard keeps fallback when not excluded",
			accept:   "application/json;q=0, */*;q=1",
			fallback: CommandWireFormatProtobuf,
			want:     CommandWireFormatProtobuf,
			wantOK:   true,
		},
		{
			name:     "invalid q disables that media range",
			accept:   "application/json;q=bogus, application/x-protobuf;q=0.5",
			fallback: CommandWireFormatJSON,
			want:     CommandWireFormatProtobuf,
			wantOK:   true,
		},
		{
			name:     "equal quality keeps fallback preference",
			accept:   "application/json;q=0.5, application/x-protobuf;q=0.5",
			fallback: CommandWireFormatJSON,
			want:     CommandWireFormatJSON,
			wantOK:   true,
		},
		{
			name:     "media params before q are accepted",
			accept:   "application/json; charset=utf-8; q=0.8, application/x-protobuf;q=0.2",
			fallback: CommandWireFormatProtobuf,
			want:     CommandWireFormatJSON,
			wantOK:   true,
		},
		{
			name:     "valueless media params before q are ignored",
			accept:   "application/json; foo; q=0.8, application/x-protobuf;q=0.2",
			fallback: CommandWireFormatProtobuf,
			want:     CommandWireFormatJSON,
			wantOK:   true,
		},
		{
			name:     "repeated media range keeps highest quality",
			accept:   "application/json;q=0.1, application/json;q=0.9, application/x-protobuf;q=0.5",
			fallback: CommandWireFormatProtobuf,
			want:     CommandWireFormatJSON,
			wantOK:   true,
		},
		{
			name:     "application wildcard matches supported fallback",
			accept:   "application/*;q=0.7",
			fallback: CommandWireFormatJSON,
			want:     CommandWireFormatJSON,
			wantOK:   true,
		},
		{
			name:     "unsupported media range is not acceptable",
			accept:   "application/xml",
			fallback: CommandWireFormatJSON,
			wantOK:   false,
		},
		{
			name:     "q zero only accepted type is not acceptable",
			accept:   "application/x-protobuf;q=0",
			fallback: CommandWireFormatProtobuf,
			wantOK:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := commandWireFormatFromAccept(tt.accept, tt.fallback)
			if ok != tt.wantOK {
				t.Fatalf("commandWireFormatFromAccept(%q, %q) ok = %v, want %v", tt.accept, tt.fallback, ok, tt.wantOK)
			}
			if got != tt.want {
				t.Fatalf("commandWireFormatFromAccept(%q, %q) = %q, want %q", tt.accept, tt.fallback, got, tt.want)
			}
		})
	}
}

func TestDecodeCommandResponseWireRejectsUnsupportedAndOversized(t *testing.T) {
	if _, err := decodeCommandResponseWire(strings.NewReader(`{"ok":true}`), "application/xml", 1<<20); !errors.Is(err, ErrUnsupportedCommandResponseContentType) {
		t.Fatalf("decodeCommandResponseWire(unsupported) error = %v, want ErrUnsupportedCommandResponseContentType", err)
	}

	if _, err := decodeCommandResponseWire(strings.NewReader(`{"ok":true}`), commandWireContentTypeJSON, 4); !errors.Is(err, errReplicationResponseTooLarge) {
		t.Fatalf("decodeCommandResponseWire(oversized JSON) error = %v, want errReplicationResponseTooLarge", err)
	}
	if _, err := decodeCommandResponseWire(bytes.NewReader([]byte{0, 1}), commandWireContentTypeProtobuf, 1); !errors.Is(err, errReplicationResponseTooLarge) {
		t.Fatalf("decodeCommandResponseWire(oversized protobuf) error = %v, want errReplicationResponseTooLarge", err)
	}
}

func TestCommandResponseWireRoundTripsBatchResponses(t *testing.T) {
	original := CacheCommandResponse{
		OK:      false,
		Message: "batch completed with errors",
		Responses: []CacheCommandResponse{
			{OK: true, Message: "stored string"},
			{OK: true, Message: "ok", Value: "ivi"},
			{OK: false, Message: "value must be a 32-bit integer"},
		},
	}
	data, err := proto.Marshal(grpcCommandResponse(original))
	if err != nil {
		t.Fatalf("Marshal(CommandResponse) error = %v", err)
	}
	decoded, err := decodeCommandResponseWire(bytes.NewReader(data), commandWireContentTypeProtobuf, 1<<20)
	if err != nil {
		t.Fatalf("decodeCommandResponseWire(protobuf batch response) error = %v", err)
	}
	if decoded.OK || decoded.Message != original.Message {
		t.Fatalf("decoded batch response = %#v, want aggregate failure message %q", decoded, original.Message)
	}
	if len(decoded.Responses) != len(original.Responses) {
		t.Fatalf("decoded responses len = %d, want %d", len(decoded.Responses), len(original.Responses))
	}
	if !decoded.Responses[1].OK || decoded.Responses[1].Value != "ivi" {
		t.Fatalf("decoded second response = %#v, want ok ivi", decoded.Responses[1])
	}
	if decoded.Responses[2].OK || decoded.Responses[2].Message == "" {
		t.Fatalf("decoded failing response = %#v, want error message", decoded.Responses[2])
	}
}

func TestWriteCommandResponseWireProtobufBatchUsesLowAllocationPath(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts include race detector instrumentation")
	}
	response := CacheCommandResponse{
		OK:      true,
		Message: "batch applied",
		Responses: []CacheCommandResponse{
			{OK: true, Message: "stored string"},
			{OK: true, Message: "ok", Value: "ivi"},
			{OK: true, Message: "stored counter"},
			{OK: true, Message: "ok", Value: "42"},
		},
	}
	request := httptest.NewRequest(http.MethodPost, "/api/commands", nil)
	request.Header.Set("Accept", commandWireContentTypeProtobuf)
	writer := newDiscardCommandWireResponseWriter()

	allocs := testing.AllocsPerRun(1000, func() {
		writer.reset()
		writeCommandResponseWire(writer, request, http.StatusOK, response, CommandWireFormatProtobuf)
		if writer.status != http.StatusOK || writer.bytes == 0 {
			t.Fatalf("writeCommandResponseWire status/bytes = %d/%d, want 200/non-empty", writer.status, writer.bytes)
		}
	})
	if allocs > 3 {
		t.Fatalf("protobuf batch response allocations = %.0f, want <= 3", allocs)
	}
}

func TestReadLimitedCommandWireBoundaryLimits(t *testing.T) {
	data, err := readLimitedCommandWire(strings.NewReader(""), 0)
	if err != nil {
		t.Fatalf("readLimitedCommandWire(empty, zero limit) error = %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("readLimitedCommandWire(empty, zero limit) len = %d, want 0", len(data))
	}

	if _, err := readLimitedCommandWire(strings.NewReader("x"), 0); !errors.Is(err, errReplicationResponseTooLarge) {
		t.Fatalf("readLimitedCommandWire(non-empty, zero limit) error = %v, want errReplicationResponseTooLarge", err)
	}
}

func TestReadLimitedCommandWireRejectsInvalidLimitWithoutRead(t *testing.T) {
	for _, limit := range []int64{-1, maxCommandWireReadLimit + 1} {
		body := &commandWireTrackingBody{reader: strings.NewReader("payload")}
		if _, err := readLimitedCommandWire(body, limit); !errors.Is(err, errCommandWireInvalidLimit) {
			t.Fatalf("readLimitedCommandWire(limit %d) error = %v, want errCommandWireInvalidLimit", limit, err)
		}
		if body.read {
			t.Fatalf("readLimitedCommandWire(limit %d) read from body", limit)
		}
	}
}

func TestDecodeCommandResponseWireRejectsInvalidJSONLimitWithoutRead(t *testing.T) {
	for _, limit := range []int64{-1, maxCommandWireReadLimit + 1} {
		body := &commandWireTrackingBody{reader: strings.NewReader(`{"ok":true}`)}
		if _, err := decodeCommandResponseWire(body, commandWireContentTypeJSON, limit); !errors.Is(err, errCommandWireInvalidLimit) {
			t.Fatalf("decodeCommandResponseWire(JSON limit %d) error = %v, want errCommandWireInvalidLimit", limit, err)
		}
		if body.read {
			t.Fatalf("decodeCommandResponseWire(JSON limit %d) read from body", limit)
		}
	}
}

func TestCacheCommandRequestToProtoConvertsScalarValuesAndPairs(t *testing.T) {
	priority := int64(7)
	ttl := int64(30)
	unix := int64(12345)
	message, err := cacheCommandRequestToProto(CacheCommandRequest{
		Command:     "PUTMAP",
		Key:         "session:1",
		Value:       "ignored",
		Subkey:      "profile",
		Priority:    &priority,
		TTLSeconds:  &ttl,
		UnixSeconds: &unix,
		Values: Slice{
			"alpha",
			int(12),
			int32(-3),
			int64(9001),
			uint(4),
			uint32(5),
			uint64(6),
			float32(1.25),
			float64(2.5),
		},
		Pairs: Map{
			"name":  "ivi",
			"score": uint64(42),
			"ratio": float32(0.75),
		},
	})
	if err != nil {
		t.Fatalf("cacheCommandRequestToProto() error = %v", err)
	}
	if message.GetCommand() != "PUTMAP" || message.GetKey() != "session:1" || message.GetValue() != "ignored" || message.GetSubkey() != "profile" {
		t.Fatalf("basic proto request fields = %#v, want copied command fields", message)
	}
	if message.GetPriority() != priority || message.GetTtlSeconds() != ttl || message.GetUnixSeconds() != unix {
		t.Fatalf("proto numeric pointers = priority %d ttl %d unix %d, want copied values", message.GetPriority(), message.GetTtlSeconds(), message.GetUnixSeconds())
	}
	wantValues := []string{"alpha", "12", "-3", "9001", "4", "5", "6", "1.25", "2.5"}
	if len(message.GetValues()) != len(wantValues) {
		t.Fatalf("proto values len = %d, want %d: %#v", len(message.GetValues()), len(wantValues), message.GetValues())
	}
	for idx, want := range wantValues {
		if got := message.GetValues()[idx]; got != want {
			t.Fatalf("proto value[%d] = %q, want %q", idx, got, want)
		}
	}
	wantPairs := map[string]string{"name": "ivi", "score": "42", "ratio": "0.75"}
	for key, want := range wantPairs {
		if got := message.GetPairs()[key]; got != want {
			t.Fatalf("proto pair[%q] = %q, want %q", key, got, want)
		}
	}
}

func TestCacheCommandRequestToProtoRejectsComplexValues(t *testing.T) {
	if _, err := cacheCommandRequestToProto(CacheCommandRequest{
		Command: "PUSH",
		Key:     "list",
		Values:  Slice{Map{"nested": "value"}},
	}); err == nil {
		t.Fatal("cacheCommandRequestToProto(complex value) error = nil, want error")
	}
	if _, err := cacheCommandRequestToProto(CacheCommandRequest{
		Command: "PUTMAP",
		Key:     "profile",
		Pairs:   Map{"nested": Slice{"value"}},
	}); err == nil {
		t.Fatal("cacheCommandRequestToProto(complex pair) error = nil, want error")
	}
}

func TestCommandRequestBodyRejectsUnsupportedProtobufValues(t *testing.T) {
	body, contentType, contentEncoding, err := commandRequestBody(CacheCommandRequest{
		Command: "PUTMAP",
		Key:     "profile",
		Pairs:   Map{"nested": Map{"value": "x"}},
	}, CommandWireFormatProtobuf, 0, 0)
	if err == nil {
		t.Fatal("commandRequestBody(protobuf complex pair) error = nil, want error")
	}
	if !errors.Is(err, ErrUnsupportedCommandWireProtobufValue) {
		t.Fatalf("commandRequestBody(protobuf complex pair) error = %v, want unsupported protobuf value", err)
	}
	if body != nil || contentType != "" || contentEncoding != "" {
		t.Fatalf("commandRequestBody(protobuf complex pair) = body %T contentType %q contentEncoding %q, want empty outputs", body, contentType, contentEncoding)
	}
}

func TestCommandRequestBodyExportedEncodesProtobufDefault(t *testing.T) {
	request := CacheCommandRequest{
		Command: "SETSTR",
		Key:     "session:1",
		Value:   "value",
		Values:  Slice{"alpha", int64(42)},
		Pairs:   Map{"score": uint64(9)},
	}

	body, contentType, contentEncoding, err := CommandRequestBody(request, DefaultCommandWireFormat, 0, 0)
	if err != nil {
		t.Fatalf("CommandRequestBody(protobuf) error = %v", err)
	}
	if contentType != commandWireContentTypeProtobuf || contentEncoding != "" {
		t.Fatalf("CommandRequestBody(protobuf) content type/encoding = %q/%q, want protobuf/empty", contentType, contentEncoding)
	}
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("ReadAll(protobuf body) error = %v", err)
	}
	decoded, err := decodeCommandRequestProto(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("decodeCommandRequestProto(exported body) error = %v", err)
	}
	if decoded.Command != request.Command || decoded.Key != request.Key || decoded.Value != request.Value {
		t.Fatalf("decoded protobuf basics = %#v, want %#v", decoded, request)
	}
	if len(decoded.Values) != 2 || decoded.Values[0] != "alpha" || decoded.Values[1] != "42" {
		t.Fatalf("decoded protobuf values = %#v, want alpha/42", decoded.Values)
	}
	if decoded.Pairs["score"] != "9" {
		t.Fatalf("decoded protobuf pairs = %#v, want score=9", decoded.Pairs)
	}
}

func TestCommandRequestBodyProtobufScalarUsesLowAllocationPath(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts include race detector instrumentation")
	}
	request := CacheCommandRequest{Command: "INTERNALSET", Key: "session:1", Value: `{"type":"string","string":"value"}`}
	allocs := testing.AllocsPerRun(1000, func() {
		body, contentType, contentEncoding, err := commandRequestBody(request, CommandWireFormatProtobuf, 0, 0)
		if err != nil {
			t.Fatalf("commandRequestBody(protobuf) error = %v", err)
		}
		if contentType != commandWireContentTypeProtobuf || contentEncoding != "" {
			t.Fatalf("commandRequestBody(protobuf) content type/encoding = %q/%q, want protobuf/empty", contentType, contentEncoding)
		}
		if _, err := io.Copy(io.Discard, body); err != nil {
			t.Fatalf("ReadAll(protobuf body) error = %v", err)
		}
		if closer, ok := body.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				t.Fatalf("Close(protobuf body) error = %v", err)
			}
		}
	})
	if allocs > 1 {
		t.Fatalf("protobuf scalar command body allocations = %.0f, want <= 1", allocs)
	}
}

func TestCommandRequestBodyEncodesNativeBatchProtobuf(t *testing.T) {
	request := CacheCommandRequest{
		Command: "INTERNALBATCH",
		Batch: []CacheCommandRequest{
			{Command: "INTERNALSET", Key: "session:1", Value: `{"type":"string","string":"one"}`},
			{Command: "INTERNALDEL", Key: "session:2"},
		},
	}

	body, contentType, contentEncoding, err := CommandRequestBody(request, CommandWireFormatProtobuf, 0, 0)
	if err != nil {
		t.Fatalf("CommandRequestBody(native batch protobuf) error = %v", err)
	}
	if contentType != commandWireContentTypeProtobuf || contentEncoding != "" {
		t.Fatalf("CommandRequestBody(native batch protobuf) content type/encoding = %q/%q, want protobuf/empty", contentType, contentEncoding)
	}
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("ReadAll(native batch protobuf body) error = %v", err)
	}
	decoded, err := decodeCommandRequestProto(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("decodeCommandRequestProto(native batch) error = %v", err)
	}
	if decoded.Command != "INTERNALBATCH" || len(decoded.Values) != 0 || len(decoded.Batch) != 2 {
		t.Fatalf("decoded native batch = %#v, want INTERNALBATCH with two batch entries and no legacy values", decoded)
	}
	if decoded.Batch[0].Command != "INTERNALSET" || decoded.Batch[0].Key != "session:1" || decoded.Batch[0].Value == "" {
		t.Fatalf("decoded batch[0] = %#v, want INTERNALSET snapshot", decoded.Batch[0])
	}
	if decoded.Batch[1].Command != "INTERNALDEL" || decoded.Batch[1].Key != "session:2" {
		t.Fatalf("decoded batch[1] = %#v, want INTERNALDEL", decoded.Batch[1])
	}
}

func TestCommandRequestBodyExportedEncodesJSONFallback(t *testing.T) {
	request := CacheCommandRequest{
		Command: "PUTMAP",
		Key:     "profile",
		Pairs:   Map{"nested": Map{"city": "Singapore"}},
	}

	body, contentType, contentEncoding, err := CommandRequestBody(request, CommandWireFormatJSON, 0, 0)
	if err != nil {
		t.Fatalf("CommandRequestBody(json) error = %v", err)
	}
	if contentType != commandWireContentTypeJSON || contentEncoding != "" {
		t.Fatalf("CommandRequestBody(json) content type/encoding = %q/%q, want JSON/empty", contentType, contentEncoding)
	}
	var decoded CacheCommandRequest
	if err := json.NewDecoder(body).Decode(&decoded); err != nil {
		t.Fatalf("Decode(JSON body) error = %v", err)
	}
	if decoded.Command != request.Command || decoded.Key != request.Key {
		t.Fatalf("decoded JSON basics = %#v, want %#v", decoded, request)
	}
	nested, ok := decoded.Pairs["nested"].(map[string]interface{})
	if !ok || nested["city"] != "Singapore" {
		t.Fatalf("decoded JSON nested pair = %#v, want city Singapore", decoded.Pairs["nested"])
	}
}

func TestCommandRequestBodyExportedRejectsUnsupportedFormat(t *testing.T) {
	body, contentType, contentEncoding, err := CommandRequestBody(CacheCommandRequest{Command: "GET", Key: "key"}, CommandWireFormat("msgpack"), 0, 0)
	if err == nil || !strings.Contains(err.Error(), "unsupported command wire format") {
		t.Fatalf("CommandRequestBody(msgpack) error = %v, want unsupported format", err)
	}
	if body != nil || contentType != "" || contentEncoding != "" {
		t.Fatalf("CommandRequestBody(msgpack) = %T/%q/%q, want nil/empty/empty", body, contentType, contentEncoding)
	}
}

func TestDecodeCommandResponseWireExportedDecodesProtobufAndJSON(t *testing.T) {
	want := CacheCommandResponse{OK: true, Message: "stored", Value: "value"}
	protobufPayload, err := proto.Marshal(grpcCommandResponse(want))
	if err != nil {
		t.Fatalf("Marshal(protobuf response) error = %v", err)
	}
	got, err := DecodeCommandResponseWire(bytes.NewReader(protobufPayload), commandWireContentTypeProtobuf, int64(len(protobufPayload)))
	if err != nil {
		t.Fatalf("DecodeCommandResponseWire(protobuf) error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DecodeCommandResponseWire(protobuf) = %#v, want %#v", got, want)
	}

	jsonPayload, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal(JSON response) error = %v", err)
	}
	got, err = DecodeCommandResponseWire(bytes.NewReader(jsonPayload), "application/json; charset=utf-8", int64(len(jsonPayload)))
	if err != nil {
		t.Fatalf("DecodeCommandResponseWire(JSON) error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DecodeCommandResponseWire(JSON) = %#v, want %#v", got, want)
	}
}

func TestMonitoringHandlerAcceptsProtobufCommandWire(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	message, err := cacheCommandRequestToProto(CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"})
	if err != nil {
		t.Fatalf("cacheCommandRequestToProto() error = %v", err)
	}
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewReader(payload))
	req.Header.Set("Content-Type", commandWireContentTypeProtobuf)
	req.Header.Set("Accept", commandWireContentTypeProtobuf)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("protobuf command status = %d, want 200; body=%s", resp.Code, resp.Body.String())
	}
	if got := resp.Header().Values("Vary"); !headerValuesContain(got, "Accept") {
		t.Fatalf("Vary = %#v, want Accept", got)
	}
	response, err := decodeCommandResponseWire(bytes.NewReader(resp.Body.Bytes()), resp.Header().Get("Content-Type"), maxMonitoringJSONRequestBytes)
	if err != nil {
		t.Fatalf("decodeCommandResponseWire() error = %v", err)
	}
	if !response.OK {
		t.Fatalf("protobuf command response = %#v, want ok", response)
	}
	if got := ht.GetString("session:1"); got != "value" {
		t.Fatalf("GetString(session:1) = %q, want value", got)
	}
}

func TestMonitoringHandlerCommandResponseRespectsAcceptQuality(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	message, err := cacheCommandRequestToProto(CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"})
	if err != nil {
		t.Fatalf("cacheCommandRequestToProto() error = %v", err)
	}
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewReader(payload))
	req.Header.Set("Content-Type", commandWireContentTypeProtobuf)
	req.Header.Set("Accept", "application/x-protobuf;q=0, application/json;q=1")
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("command status = %d, want 200; body=%s", resp.Code, resp.Body.String())
	}
	if got := resp.Header().Get("Content-Type"); got != commandWireContentTypeJSON {
		t.Fatalf("Content-Type = %q, want json", got)
	}
	if got := resp.Header().Values("Vary"); !headerValuesContain(got, "Accept") {
		t.Fatalf("Vary = %#v, want Accept", got)
	}
	var response CacheCommandResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &response); err != nil {
		t.Fatalf("response JSON error = %v", err)
	}
	if !response.OK {
		t.Fatalf("response = %#v, want ok", response)
	}
}

func TestMonitoringHandlerRejectsUnacceptableCommandResponseBeforeExecute(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()
	req := httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"SETSTR","key":"session:1","value":"value"}`))
	req.Header.Set("Accept", "application/xml")
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusNotAcceptable {
		t.Fatalf("command status = %d, want 406; body=%s", resp.Code, resp.Body.String())
	}
	if got := resp.Header().Values("Vary"); !headerValuesContain(got, "Accept") {
		t.Fatalf("Vary = %#v, want Accept", got)
	}
	if ht.Exists("session:1") {
		t.Fatal("unacceptable command response executed mutation")
	}
}

func TestMonitoringHandlerRejectsUnacceptableCommandResponseWithoutReadingBody(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()
	body := &commandWireTrackingBody{reader: strings.NewReader(`{"command":"SETSTR","key":"session:1","value":"value"}`)}
	req := httptest.NewRequest(http.MethodPost, "/api/commands", nil)
	req.Body = body
	req.ContentLength = int64(body.reader.Len())
	req.Header.Set("Content-Type", commandWireContentTypeJSON)
	req.Header.Set("Accept", "application/xml")
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusNotAcceptable {
		t.Fatalf("command status = %d, want 406; body=%s", resp.Code, resp.Body.String())
	}
	if got := resp.Header().Values("Vary"); !headerValuesContain(got, "Accept") {
		t.Fatalf("Vary = %#v, want Accept", got)
	}
	if body.read {
		t.Fatal("unacceptable command response read request body")
	}
	if !body.closed {
		t.Fatal("unacceptable command response did not close request body")
	}
	if ht.Exists("session:1") {
		t.Fatal("unacceptable command response executed mutation")
	}
}
