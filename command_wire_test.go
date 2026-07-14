package hatriecache

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

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
		{value: "text/json", want: CommandWireFormatJSON},
		{value: "application/x-protobuf", want: CommandWireFormatProtobuf},
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
	}{
		{
			name:     "empty uses fallback",
			fallback: CommandWireFormatProtobuf,
			want:     CommandWireFormatProtobuf,
		},
		{
			name:     "higher quality json beats protobuf",
			accept:   "application/x-protobuf;q=0.1, application/json;q=0.9",
			fallback: CommandWireFormatProtobuf,
			want:     CommandWireFormatJSON,
		},
		{
			name:     "higher quality protobuf beats json",
			accept:   "application/json;q=0.1, application/x-protobuf;q=0.9",
			fallback: CommandWireFormatJSON,
			want:     CommandWireFormatProtobuf,
		},
		{
			name:     "explicit q zero excludes fallback under wildcard",
			accept:   "application/x-protobuf;q=0, */*;q=1",
			fallback: CommandWireFormatProtobuf,
			want:     CommandWireFormatJSON,
		},
		{
			name:     "wildcard keeps fallback when not excluded",
			accept:   "application/json;q=0, */*;q=1",
			fallback: CommandWireFormatProtobuf,
			want:     CommandWireFormatProtobuf,
		},
		{
			name:     "invalid q disables that media range",
			accept:   "application/json;q=bogus, application/x-protobuf;q=0.5",
			fallback: CommandWireFormatJSON,
			want:     CommandWireFormatProtobuf,
		},
		{
			name:     "equal quality keeps fallback preference",
			accept:   "application/json;q=0.5, application/x-protobuf;q=0.5",
			fallback: CommandWireFormatJSON,
			want:     CommandWireFormatJSON,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := commandWireFormatFromAccept(tt.accept, tt.fallback); got != tt.want {
				t.Fatalf("commandWireFormatFromAccept(%q, %q) = %q, want %q", tt.accept, tt.fallback, got, tt.want)
			}
		})
	}
}

func TestDecodeCommandResponseWireRejectsUnsupportedAndOversized(t *testing.T) {
	if _, err := decodeCommandResponseWire(strings.NewReader(`{"ok":true}`), "application/xml", 1<<20); err == nil {
		t.Fatal("decodeCommandResponseWire(unsupported) error = nil, want error")
	}

	if _, err := decodeCommandResponseWire(strings.NewReader(`{"ok":true}`), commandWireContentTypeJSON, 4); !errors.Is(err, errReplicationResponseTooLarge) {
		t.Fatalf("decodeCommandResponseWire(oversized JSON) error = %v, want errReplicationResponseTooLarge", err)
	}
	if _, err := decodeCommandResponseWire(bytes.NewReader([]byte{0, 1}), commandWireContentTypeProtobuf, 1); !errors.Is(err, errReplicationResponseTooLarge) {
		t.Fatalf("decodeCommandResponseWire(oversized protobuf) error = %v, want errReplicationResponseTooLarge", err)
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
	if body != nil || contentType != "" || contentEncoding != "" {
		t.Fatalf("commandRequestBody(protobuf complex pair) = body %T contentType %q contentEncoding %q, want empty outputs", body, contentType, contentEncoding)
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
