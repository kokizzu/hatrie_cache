package hatTrace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultOTLPHTTPMaxBatch        = 512
	MaxOTLPHTTPMaxBatch            = 4096
	DefaultOTLPHTTPMaxPayloadBytes = 4 << 20
	MaxOTLPHTTPMaxPayloadBytes     = 64 << 20
	defaultOTLPHTTPTimeout         = 10 * time.Second
)

var (
	ErrOTLPHTTPExporterNil      = errors.New("hatrie trace: OTLP HTTP exporter is nil")
	ErrOTLPHTTPExporterEndpoint = errors.New("hatrie trace: OTLP HTTP endpoint must be an absolute HTTP or HTTPS URL")
	ErrOTLPHTTPExporterBatch    = errors.New("hatrie trace: OTLP HTTP batch exceeds the configured limit")
	ErrOTLPHTTPExporterPayload  = errors.New("hatrie trace: OTLP HTTP payload exceeds the configured limit")
)

// Span is the transport-neutral subset of an OpenTelemetry span used by the
// built-in exporter. Attributes are copied before encoding and are never
// retained by the exporter.
type Span struct {
	TraceID       string
	SpanID        string
	ParentSpanID  string
	Name          string
	StartUnixNano int64
	EndUnixNano   int64
	Status        string
	Attributes    map[string]string
}

// SpanExporter consumes a bounded batch of completed spans.
type SpanExporter interface {
	Export(context.Context, []Span) error
}

// OTLPHTTPExporterOptions configures the opt-in OTLP/HTTP JSON exporter.
// Endpoint should normally include the collector's /v1/traces path.
type OTLPHTTPExporterOptions struct {
	Endpoint        string
	Client          *http.Client
	Headers         http.Header
	ServiceName     string
	ServiceVersion  string
	MaxBatch        int
	MaxPayloadBytes int
}

// OTLPHTTPExporter sends completed spans to an OTLP/HTTP JSON endpoint. It is
// intentionally caller-owned and performs no background work.
type OTLPHTTPExporter struct {
	endpoint        string
	client          *http.Client
	headers         http.Header
	serviceName     string
	serviceVersion  string
	maxBatch        int
	maxPayloadBytes int
}

// NewOTLPHTTPExporter validates and creates an exporter. A nil Client selects
// a bounded default HTTP client; caller-provided clients retain their own
// transport, proxy, and TLS policy.
func NewOTLPHTTPExporter(options OTLPHTTPExporterOptions) (*OTLPHTTPExporter, error) {
	endpoint := strings.TrimSpace(options.Endpoint)
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, ErrOTLPHTTPExporterEndpoint
	}
	maxBatch := options.MaxBatch
	if maxBatch == 0 {
		maxBatch = DefaultOTLPHTTPMaxBatch
	}
	if maxBatch < 1 || maxBatch > MaxOTLPHTTPMaxBatch {
		return nil, fmt.Errorf("hatrie trace: OTLP HTTP max batch must be between 1 and %d", MaxOTLPHTTPMaxBatch)
	}
	maxPayloadBytes := options.MaxPayloadBytes
	if maxPayloadBytes == 0 {
		maxPayloadBytes = DefaultOTLPHTTPMaxPayloadBytes
	}
	if maxPayloadBytes < 1 || maxPayloadBytes > MaxOTLPHTTPMaxPayloadBytes {
		return nil, fmt.Errorf("hatrie trace: OTLP HTTP max payload must be between 1 and %d bytes", MaxOTLPHTTPMaxPayloadBytes)
	}
	serviceName := strings.TrimSpace(options.ServiceName)
	if serviceName == "" {
		serviceName = "hatrie_cache"
	}
	client := options.Client
	if client == nil {
		client = &http.Client{
			Timeout: defaultOTLPHTTPTimeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	return &OTLPHTTPExporter{
		endpoint:        endpoint,
		client:          client,
		headers:         cloneHTTPHeaders(options.Headers),
		serviceName:     serviceName,
		serviceVersion:  strings.TrimSpace(options.ServiceVersion),
		maxBatch:        maxBatch,
		maxPayloadBytes: maxPayloadBytes,
	}, nil
}

// Export sends one bounded batch. Empty batches are accepted without a
// network request. HTTP response bodies are deliberately not included in
// returned errors so collectors cannot reflect sensitive payload data.
func (exporter *OTLPHTTPExporter) Export(ctx context.Context, spans []Span) error {
	if exporter == nil {
		return ErrOTLPHTTPExporterNil
	}
	if exporter.client == nil || exporter.endpoint == "" {
		return ErrOTLPHTTPExporterEndpoint
	}
	if len(spans) == 0 {
		return nil
	}
	if len(spans) > exporter.maxBatch {
		return fmt.Errorf("%w: got %d, limit %d", ErrOTLPHTTPExporterBatch, len(spans), exporter.maxBatch)
	}
	payload, err := exporter.marshal(spans)
	if err != nil {
		return err
	}
	if len(payload) > exporter.maxPayloadBytes {
		return fmt.Errorf("%w: got %d, limit %d", ErrOTLPHTTPExporterPayload, len(payload), exporter.maxPayloadBytes)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, exporter.endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("hatrie trace: create OTLP HTTP request: %w", err)
	}
	for key, values := range exporter.headers {
		for _, value := range values {
			request.Header.Add(key, value)
		}
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	response, err := exporter.client.Do(request)
	if err != nil {
		return fmt.Errorf("hatrie trace: export OTLP HTTP spans: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("hatrie trace: OTLP HTTP collector returned %s", response.Status)
	}
	return nil
}

func (exporter *OTLPHTTPExporter) marshal(spans []Span) ([]byte, error) {
	encodedSpans := make([]otlpSpan, len(spans))
	for index, span := range spans {
		if err := validateSpan(span); err != nil {
			return nil, fmt.Errorf("hatrie trace: span %d: %w", index, err)
		}
		encodedSpans[index] = otlpSpan{
			TraceID:       strings.ToLower(span.TraceID),
			SpanID:        strings.ToLower(span.SpanID),
			ParentSpanID:  strings.ToLower(span.ParentSpanID),
			Name:          span.Name,
			StartTimeNano: strconv.FormatInt(span.StartUnixNano, 10),
			EndTimeNano:   strconv.FormatInt(span.EndUnixNano, 10),
			Attributes:    otlpAttributes(span.Attributes),
			Status:        otlpStatus{Code: otlpStatusCode(span.Status)},
		}
	}
	resourceAttributes := []otlpKeyValue{{Key: "service.name", Value: otlpAnyValue{StringValue: exporter.serviceName}}}
	if exporter.serviceVersion != "" {
		resourceAttributes = append(resourceAttributes, otlpKeyValue{Key: "service.version", Value: otlpAnyValue{StringValue: exporter.serviceVersion}})
	}
	payload := otlpTraceRequest{
		ResourceSpans: []otlpResourceSpans{{
			Resource: otlpResource{Attributes: resourceAttributes},
			ScopeSpans: []otlpScopeSpans{{
				Scope: otlpScope{Name: "hatrie_cache"},
				Spans: encodedSpans,
			}},
		}},
	}
	return json.Marshal(payload)
}

func validateSpan(span Span) error {
	if !validHexID(strings.ToLower(span.TraceID), 32) {
		return errors.New("trace ID is invalid")
	}
	if !validHexID(strings.ToLower(span.SpanID), 16) {
		return errors.New("span ID is invalid")
	}
	if span.ParentSpanID != "" && !validHexID(strings.ToLower(span.ParentSpanID), 16) {
		return errors.New("parent span ID is invalid")
	}
	if strings.TrimSpace(span.Name) == "" {
		return errors.New("span name is required")
	}
	if span.StartUnixNano < 0 || span.EndUnixNano < span.StartUnixNano {
		return errors.New("span timestamps are invalid")
	}
	return nil
}

func cloneHTTPHeaders(headers http.Header) http.Header {
	if headers == nil {
		return nil
	}
	cloned := make(http.Header, len(headers))
	for key, values := range headers {
		cloned[key] = append([]string(nil), values...)
	}
	return cloned
}

func otlpAttributes(attributes map[string]string) []otlpKeyValue {
	if len(attributes) == 0 {
		return nil
	}
	keys := make([]string, 0, len(attributes))
	for key := range attributes {
		if strings.TrimSpace(key) != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	result := make([]otlpKeyValue, len(keys))
	for index, key := range keys {
		result[index] = otlpKeyValue{Key: key, Value: otlpAnyValue{StringValue: attributes[key]}}
	}
	return result
}

func otlpStatusCode(status string) int {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "OK":
		return 1
	case "ERROR":
		return 2
	default:
		return 0
	}
}

type otlpTraceRequest struct {
	ResourceSpans []otlpResourceSpans `json:"resourceSpans"`
}

type otlpResourceSpans struct {
	Resource   otlpResource     `json:"resource"`
	ScopeSpans []otlpScopeSpans `json:"scopeSpans"`
}

type otlpResource struct {
	Attributes []otlpKeyValue `json:"attributes,omitempty"`
}

type otlpScopeSpans struct {
	Scope otlpScope  `json:"scope"`
	Spans []otlpSpan `json:"spans"`
}

type otlpScope struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

type otlpSpan struct {
	TraceID       string         `json:"traceId"`
	SpanID        string         `json:"spanId"`
	ParentSpanID  string         `json:"parentSpanId,omitempty"`
	Name          string         `json:"name"`
	StartTimeNano string         `json:"startTimeUnixNano"`
	EndTimeNano   string         `json:"endTimeUnixNano"`
	Attributes    []otlpKeyValue `json:"attributes,omitempty"`
	Status        otlpStatus     `json:"status"`
}

type otlpStatus struct {
	Code int `json:"code"`
}

type otlpKeyValue struct {
	Key   string       `json:"key"`
	Value otlpAnyValue `json:"value"`
}

type otlpAnyValue struct {
	StringValue string `json:"stringValue"`
}
