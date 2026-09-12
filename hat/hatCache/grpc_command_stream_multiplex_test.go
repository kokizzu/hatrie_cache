package hatCache

import (
	"context"
	"errors"
	"io"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestCacheGRPCServerCommandStreamCorrelatesMultiplexedResponses(t *testing.T) {
	trie := newTestTrie(t)
	client, stop := newTestGRPCClient(t, trie, CacheGRPCOptions{CommandStreamWorkers: 2})
	defer stop()

	stream, err := client.CommandStream(context.Background())
	if err != nil {
		t.Fatalf("CommandStream() error = %v", err)
	}
	requests := []*hatriecachev1.CommandRequest{
		{RequestId: 101, Command: "SETSTR", Key: "multiplex:name", Value: "ivi"},
		{RequestId: 102, Command: "SETINT", Key: "multiplex:count", Value: "42"},
	}
	for _, request := range requests {
		if err := stream.Send(request); err != nil {
			t.Fatalf("CommandStream.Send(%d) error = %v", request.GetRequestId(), err)
		}
	}

	responses := make(map[uint64]*hatriecachev1.CommandResponse, len(requests))
	for range requests {
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("CommandStream.Recv() error = %v", err)
		}
		responses[response.GetRequestId()] = response
	}
	for _, request := range requests {
		response, ok := responses[request.GetRequestId()]
		if !ok {
			t.Fatalf("missing response for request ID %d: %#v", request.GetRequestId(), responses)
		}
		if !response.GetOk() {
			t.Fatalf("response %d = %#v, want success", request.GetRequestId(), response)
		}
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CommandStream.CloseSend() error = %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("CommandStream final Recv() error = %v, want EOF", err)
	}
}

func TestCacheGRPCServerCommandStreamRejectsMixedRequestIDModes(t *testing.T) {
	trie := newTestTrie(t)
	client, stop := newTestGRPCClient(t, trie, CacheGRPCOptions{CommandStreamWorkers: 2})
	defer stop()

	stream, err := client.CommandStream(context.Background())
	if err != nil {
		t.Fatalf("CommandStream() error = %v", err)
	}
	if err := stream.Send(&hatriecachev1.CommandRequest{
		RequestId: 201,
		Command:   "SETSTR",
		Key:       "multiplex:mixed",
		Value:     "first",
	}); err != nil {
		t.Fatalf("CommandStream.Send(first) error = %v", err)
	}
	if err := stream.Send(&hatriecachev1.CommandRequest{
		Command: "SETSTR",
		Key:     "multiplex:mixed",
		Value:   "second",
	}); err != nil {
		t.Fatalf("CommandStream.Send(second) error = %v", err)
	}

	for {
		_, err := stream.Recv()
		if err == nil {
			continue
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("CommandStream mixed-mode error = %v, want InvalidArgument", err)
		}
		return
	}
}

func TestCacheGRPCServerNormalizesCommandStreamWorkers(t *testing.T) {
	server := NewCacheGRPCServer(nil, CacheGRPCOptions{})
	if server.options.CommandStreamWorkers != defaultCommandStreamWorkers {
		t.Fatalf("default CommandStreamWorkers = %d, want %d", server.options.CommandStreamWorkers, defaultCommandStreamWorkers)
	}
	server = NewCacheGRPCServer(nil, CacheGRPCOptions{CommandStreamWorkers: maxCommandStreamWorkers + 1})
	if server.options.CommandStreamWorkers != maxCommandStreamWorkers {
		t.Fatalf("capped CommandStreamWorkers = %d, want %d", server.options.CommandStreamWorkers, maxCommandStreamWorkers)
	}
}
