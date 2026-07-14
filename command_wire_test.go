package hatriecache

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/protobuf/proto"
)

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
