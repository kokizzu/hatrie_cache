package hatriecache

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/protobuf/proto"
)

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
	var response CacheCommandResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &response); err != nil {
		t.Fatalf("response JSON error = %v", err)
	}
	if !response.OK {
		t.Fatalf("response = %#v, want ok", response)
	}
}
