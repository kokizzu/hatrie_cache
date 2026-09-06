package hatCache

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"google.golang.org/grpc/metadata"
	"hatrie_cache/hat/hatTrace"
)

const testTraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func testTraceContext(t *testing.T) context.Context {
	t.Helper()
	span, err := hatTrace.ParseTraceParent(testTraceParent)
	if err != nil {
		t.Fatal(err)
	}
	return hatTrace.WithSpanContext(context.Background(), span)
}

func TestHTTPReplicationCarriesTraceParent(t *testing.T) {
	var gotTraceParent string
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{})
	replicator.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		gotTraceParent = request.Header.Get(hatTrace.TraceParentHeader)
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"ok":true,"message":"ok"}`)),
			Request:    request,
		}, nil
	})}
	defer replicator.Close()

	result := replicator.postReplicationCommand(
		testTraceContext(t),
		TopologyNode{ID: "node-b", Address: "http://node.test"},
		CacheCommandRequest{Command: "SET", Key: "trace:key", Value: "value"},
	)
	if !result.OK {
		t.Fatalf("postReplicationCommand() = %#v, want success", result)
	}
	if gotTraceParent != testTraceParent {
		t.Fatalf("replication traceparent = %q, want %q", gotTraceParent, testTraceParent)
	}
}

func TestGRPCReplicationCarriesTraceParentAndAuth(t *testing.T) {
	ctx := replicationGRPCOutgoingContext(testTraceContext(t), "replication-token")
	metadataValues, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("replication gRPC outgoing metadata missing")
	}
	if got := metadataValues.Get(hatTrace.TraceParentHeader); len(got) != 1 || got[0] != testTraceParent {
		t.Fatalf("outgoing traceparent = %#v, want [%q]", got, testTraceParent)
	}
	if got := metadataValues.Get("x-hatrie-replication-token"); len(got) != 1 || got[0] != "replication-token" {
		t.Fatalf("outgoing replication token = %#v", got)
	}
}

func TestGRPCContextExtractsTraceParent(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		hatTrace.TraceParentHeader, testTraceParent,
	))
	got, ok := hatTrace.SpanContextFromContext(grpcContext(ctx))
	if !ok || got.TraceParent() != testTraceParent {
		t.Fatalf("grpc trace context = %#v/%v, want %q", got, ok, testTraceParent)
	}

	malformed := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		hatTrace.TraceParentHeader, "not-a-traceparent",
	))
	if _, ok := hatTrace.SpanContextFromContext(grpcContext(malformed)); ok {
		t.Fatal("malformed gRPC traceparent was installed")
	}
}
