// Package hatTrace contains small, transport-neutral tracing helpers.
package hatTrace

import (
	"context"
	"encoding/hex"
	"net/http"
	"strings"
)

// TraceParentHeader is the W3C Trace Context HTTP header name.
const TraceParentHeader = "traceparent"

type spanContextKey struct{}

// SpanContext is the portion of a trace context needed for propagation.
// TraceID and SpanID contain hexadecimal strings in their wire representation.
type SpanContext struct {
	TraceID    string
	SpanID     string
	TraceFlags byte
}

// NewSpanContext validates and creates a span context from hexadecimal IDs.
func NewSpanContext(traceID, spanID string, traceFlags byte) (SpanContext, error) {
	traceID = strings.ToLower(traceID)
	spanID = strings.ToLower(spanID)
	if !validHexID(traceID, 32) {
		return SpanContext{}, &traceContextError{field: "trace ID"}
	}
	if !validHexID(spanID, 16) {
		return SpanContext{}, &traceContextError{field: "span ID"}
	}
	return SpanContext{TraceID: traceID, SpanID: spanID, TraceFlags: traceFlags}, nil
}

// ParseTraceParent parses a version 00 W3C traceparent value.
func ParseTraceParent(value string) (SpanContext, error) {
	parts := strings.Split(value, "-")
	if len(parts) != 4 || parts[0] != "00" || len(parts[3]) != 2 {
		return SpanContext{}, &traceContextError{field: "traceparent"}
	}
	flags, err := hex.DecodeString(parts[3])
	if err != nil || len(flags) != 1 {
		return SpanContext{}, &traceContextError{field: "trace flags"}
	}
	return NewSpanContext(parts[1], parts[2], flags[0])
}

// TraceParent returns the canonical version 00 wire value. It returns an
// empty string for a context that was constructed without validation.
func (s SpanContext) TraceParent() string {
	if !validHexID(s.TraceID, 32) || !validHexID(s.SpanID, 16) {
		return ""
	}
	return "00-" + strings.ToLower(s.TraceID) + "-" + strings.ToLower(s.SpanID) + "-" + hex.EncodeToString([]byte{s.TraceFlags})
}

// WithSpanContext stores a valid span context in ctx.
func WithSpanContext(ctx context.Context, span SpanContext) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if span.TraceParent() == "" {
		return ctx
	}
	return context.WithValue(ctx, spanContextKey{}, span)
}

// WithTraceParent parses value and stores it in ctx when valid. Invalid input
// is ignored so malformed remote metadata cannot affect request handling.
func WithTraceParent(ctx context.Context, value string) context.Context {
	span, err := ParseTraceParent(value)
	if err != nil {
		if ctx == nil {
			return context.Background()
		}
		return ctx
	}
	return WithSpanContext(ctx, span)
}

// SpanContextFromContext returns the valid span context stored in ctx.
func SpanContextFromContext(ctx context.Context) (SpanContext, bool) {
	if ctx == nil {
		return SpanContext{}, false
	}
	span, ok := ctx.Value(spanContextKey{}).(SpanContext)
	if !ok || span.TraceParent() == "" {
		return SpanContext{}, false
	}
	return span, true
}

// TraceParentFromContext returns the wire value stored in ctx.
func TraceParentFromContext(ctx context.Context) (string, bool) {
	span, ok := SpanContextFromContext(ctx)
	if !ok {
		return "", false
	}
	return span.TraceParent(), true
}

// InjectHTTP adds the current trace context to an outgoing HTTP header.
func InjectHTTP(ctx context.Context, header http.Header) {
	if header == nil {
		return
	}
	if traceParent, ok := TraceParentFromContext(ctx); ok {
		header.Set(TraceParentHeader, traceParent)
	}
}

// ExtractHTTPContext returns ctx with a valid incoming traceparent installed.
// Invalid or absent headers leave ctx unchanged.
func ExtractHTTPContext(ctx context.Context, header http.Header) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if header == nil {
		return ctx
	}
	return WithTraceParent(ctx, header.Get(TraceParentHeader))
}

// HTTPMiddleware extracts trace context before invoking next.
func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := ExtractHTTPContext(r.Context(), r.Header)
		if ctx == r.Context() {
			next.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type traceContextError struct {
	field string
}

func (e *traceContextError) Error() string {
	return "invalid " + e.field
}

func validHexID(value string, length int) bool {
	if len(value) != length {
		return false
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return false
	}
	for _, b := range decoded {
		if b != 0 {
			return true
		}
	}
	return false
}
