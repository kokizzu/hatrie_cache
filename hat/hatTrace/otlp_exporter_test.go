package hatTrace

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOTLPHTTPExporterSendsTracePayloadAndHeaders(t *testing.T) {
	var requestBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q", got)
		}
		if got := request.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("Authorization = %q", got)
		}
		if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil {
			t.Fatalf("decode request = %v", err)
		}
		writer.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	exporter, err := NewOTLPHTTPExporter(OTLPHTTPExporterOptions{
		Endpoint:    server.URL + "/v1/traces",
		Headers:     http.Header{"Authorization": {"Bearer test-token"}},
		ServiceName: "hatrie-test",
	})
	if err != nil {
		t.Fatalf("NewOTLPHTTPExporter() error = %v", err)
	}
	if err := exporter.Export(context.Background(), []Span{{
		TraceID:       "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanID:        "00f067aa0ba902b7",
		Name:          "hatrie.sql.query",
		StartUnixNano: 10,
		EndUnixNano:   20,
		Status:        "OK",
		Attributes:    map[string]string{"hatrie.sql.query_id": "q1"},
	}}); err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	encoded, err := json.Marshal(requestBody)
	if err != nil {
		t.Fatal(err)
	}
	body := string(encoded)
	for _, want := range []string{"hatrie-test", "4bf92f3577b34da6a3ce929d0e0e4736", "hatrie.sql.query_id", "hatrie.sql.query"} {
		if !strings.Contains(body, want) {
			t.Fatalf("payload missing %q: %s", want, body)
		}
	}
}

func TestOTLPHTTPExporterRejectsUnsafeEndpointAndOversizedBatch(t *testing.T) {
	for _, endpoint := range []string{"", "file:///tmp/traces", "http://", "http://user:pass@example.test/traces"} {
		if _, err := NewOTLPHTTPExporter(OTLPHTTPExporterOptions{Endpoint: endpoint}); err == nil {
			t.Fatalf("NewOTLPHTTPExporter(%q) error = nil", endpoint)
		}
	}
	exporter, err := NewOTLPHTTPExporter(OTLPHTTPExporterOptions{Endpoint: "http://example.test/traces", MaxBatch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := exporter.Export(context.Background(), []Span{{}, {}}); err == nil {
		t.Fatal("oversized Export() error = nil")
	}
	payloadLimited, err := NewOTLPHTTPExporter(OTLPHTTPExporterOptions{Endpoint: "http://example.test/traces", MaxPayloadBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	err = payloadLimited.Export(context.Background(), []Span{{
		TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanID:  "00f067aa0ba902b7",
		Name:    "payload-limit",
	}})
	if !errors.Is(err, ErrOTLPHTTPExporterPayload) {
		t.Fatalf("payload-limited Export() error = %v, want ErrOTLPHTTPExporterPayload", err)
	}
}

func TestOTLPHTTPExporterReturnsHTTPFailureWithoutResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "secret response text", http.StatusUnauthorized)
	}))
	defer server.Close()
	exporter, err := NewOTLPHTTPExporter(OTLPHTTPExporterOptions{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	err = exporter.Export(context.Background(), []Span{{
		TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanID:  "00f067aa0ba902b7",
		Name:    "failure",
	}})
	if err == nil || strings.Contains(err.Error(), "secret response text") {
		t.Fatalf("Export() error = %v, want status-only error", err)
	}
}
