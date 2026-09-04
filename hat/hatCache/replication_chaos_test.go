package hatCache

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type replicationChaosFault uint8

const (
	replicationChaosTransportError replicationChaosFault = iota + 1
	replicationChaosHTTPError
	replicationChaosMalformedResponse
)

type replicationChaosTransport struct {
	faults   []replicationChaosFault
	attempts atomic.Int32
}

func newReplicationChaosTransport(faults []replicationChaosFault) *replicationChaosTransport {
	return &replicationChaosTransport{faults: append([]replicationChaosFault(nil), faults...)}
}

func (transport *replicationChaosTransport) Attempts() int {
	if transport == nil {
		return 0
	}
	return int(transport.attempts.Load())
}

func (transport *replicationChaosTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	attempt := int(transport.attempts.Add(1)) - 1
	var fault replicationChaosFault
	if attempt < len(transport.faults) {
		fault = transport.faults[attempt]
	}
	switch fault {
	case replicationChaosTransportError:
		return nil, errors.New("injected connection reset")
	case replicationChaosHTTPError:
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Status:     "503 Service Unavailable",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":false,"message":"temporary"}`)),
			Request:    request,
		}, nil
	case replicationChaosMalformedResponse:
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":true`)),
			Request:    request,
		}, nil
	default:
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":true,"message":"ok"}`)),
			Request:    request,
		}, nil
	}
}

func TestReplicationChaosRetriesMixedTransientFailures(t *testing.T) {
	transport := newReplicationChaosTransport([]replicationChaosFault{
		replicationChaosTransportError,
		replicationChaosHTTPError,
		replicationChaosMalformedResponse,
	})
	client := &http.Client{Transport: transport}
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, "http://node-b.chaos")
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:               "node-a",
		Topology:           topology,
		Election:           election,
		Client:             client,
		AsyncQueueSize:     1,
		AsyncMaxAttempts:   4,
		AsyncRetryInterval: time.Millisecond,
	})
	t.Cleanup(replicator.Close)

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:chaos", Value: "value"}
	response := trie.ExecuteCommand(write)
	result := replicator.ReplicateCommand(context.Background(), trie, write, response)
	if !result.Queued || result.Skipped {
		t.Fatalf("async enqueue result = %#v, want queued", result)
	}

	final := waitForReplicationLastResult(t, replicator, func(result ReplicationResult) bool {
		return !result.Queued && len(result.Targets) == 1 && result.Targets[0].OK
	})
	if got := transport.Attempts(); got != 4 {
		t.Fatalf("attempts = %d, want three injected failures followed by recovery", got)
	}
	if final.Queue == nil || final.Queue.Attempts != 4 || final.Queue.Failures != 3 || final.Queue.Retried != 3 || final.Queue.Successes != 1 {
		t.Fatalf("chaos queue stats = %#v, want three failures, three retries, one success", final.Queue)
	}
}
