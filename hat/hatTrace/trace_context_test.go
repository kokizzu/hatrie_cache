package hatTrace

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTraceParentRoundTripsThroughContextAndHTTP(t *testing.T) {
	original, err := NewSpanContext("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", 1)
	if err != nil {
		t.Fatalf("NewSpanContext() error = %v", err)
	}
	if got := original.TraceParent(); got != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Fatalf("TraceParent() = %q", got)
	}

	ctx := WithSpanContext(context.Background(), original)
	header := make(http.Header)
	InjectHTTP(ctx, header)
	if got := header.Get(TraceParentHeader); got != original.TraceParent() {
		t.Fatalf("injected traceparent = %q", got)
	}
	got, ok := SpanContextFromContext(ExtractHTTPContext(context.Background(), header))
	if !ok || got != original {
		t.Fatalf("extracted context = %#v, %v; want %#v, true", got, ok, original)
	}
}

func TestHTTPMiddlewareExtractsTraceParent(t *testing.T) {
	var got string
	handler := HTTPMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		got, _ = TraceParentFromContext(request.Context())
	}))
	request := httptest.NewRequest(http.MethodGet, "/api/replication", nil)
	request.Header.Set(TraceParentHeader, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	handler.ServeHTTP(httptest.NewRecorder(), request)
	if got != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Fatalf("middleware traceparent = %q", got)
	}
}

func TestTraceParentRejectsMalformedOrZeroValues(t *testing.T) {
	invalid := []string{
		"",
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
		"01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-zz",
	}
	for _, value := range invalid {
		if _, err := ParseTraceParent(value); err == nil {
			t.Fatalf("ParseTraceParent(%q) error = nil", value)
		}
	}
	header := http.Header{TraceParentHeader: []string{invalid[0]}}
	if _, ok := SpanContextFromContext(ExtractHTTPContext(context.Background(), header)); ok {
		t.Fatal("malformed traceparent was installed in context")
	}
}
