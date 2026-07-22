package hatriecache

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/internal/jsonwire"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func TestParseReplicationTransport(t *testing.T) {
	for input, want := range map[string]ReplicationTransport{
		"":            ReplicationTransportHTTP,
		"http":        ReplicationTransportHTTP,
		"grpc-stream": ReplicationTransportGRPCStream,
	} {
		got, err := ParseReplicationTransport(input)
		if err != nil || got != want {
			t.Fatalf("ParseReplicationTransport(%q) = %q/%v, want %q", input, got, err, want)
		}
	}
	if _, err := ParseReplicationTransport("udp"); err == nil {
		t.Fatal("ParseReplicationTransport(udp) error = nil, want validation error")
	}
}

func TestNewHTTPReplicatorDefaultsReplicationBatchLimit(t *testing.T) {
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{})
	if replicator.batchMaxBytes != DefaultReplicationBatchMaxBytes {
		t.Fatalf("default replication batch limit = %d, want %d", replicator.batchMaxBytes, DefaultReplicationBatchMaxBytes)
	}
	if replicator.grpcStreamWindow != DefaultReplicationGRPCStreamWindow {
		t.Fatalf("default gRPC stream window = %d, want %d", replicator.grpcStreamWindow, DefaultReplicationGRPCStreamWindow)
	}
	if replicator.grpcLiveBatchMaxCommands != DefaultReplicationGRPCLiveBatchMaxCommands {
		t.Fatalf("default live gRPC batch commands = %d, want %d", replicator.grpcLiveBatchMaxCommands, DefaultReplicationGRPCLiveBatchMaxCommands)
	}
	if replicator.grpcLiveBatchWindow != DefaultReplicationGRPCLiveBatchWindow {
		t.Fatalf("default live gRPC batch window = %s, want %s", replicator.grpcLiveBatchWindow, DefaultReplicationGRPCLiveBatchWindow)
	}

	unlimited := NewHTTPReplicator(HTTPReplicatorOptions{ReplicationBatchMaxBytes: -1})
	if unlimited.batchMaxBytes != 0 {
		t.Fatalf("negative replication batch limit = %d, want unlimited", unlimited.batchMaxBytes)
	}
	disabledGrouping := NewHTTPReplicator(HTTPReplicatorOptions{
		GRPCLiveBatchMaxCommands: -1,
		GRPCLiveBatchWindow:      -time.Second,
	})
	if disabledGrouping.grpcLiveBatchMaxCommands != 1 || disabledGrouping.grpcLiveBatchWindow != 0 {
		t.Fatalf("negative live batch options = %d/%s, want 1/0", disabledGrouping.grpcLiveBatchMaxCommands, disabledGrouping.grpcLiveBatchWindow)
	}
}

func TestHTTPReplicatorRestoresLevelDBOutboxWithinConfiguredQueueCapacity(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	delivered := make(chan string, 100)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		select {
		case <-started:
		default:
			close(started)
			<-release
		}
		delivered <- request.Key
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()
	defer releaseOnce.Do(func() { close(release) })

	outbox, err := OpenLevelDBReplicationOutboxWithOptions(filepath.Join(t.TempDir(), "outbox"), ReplicationOutboxOptions{
		Codec: ReplicationOutboxCodecBinary,
	})
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutboxWithOptions() error = %v", err)
	}
	t.Cleanup(func() { _ = outbox.Close() })
	batch := new(leveldb.Batch)
	for id := uint64(1); id <= 100; id++ {
		key := fmt.Sprintf("restore:%03d", id)
		record := newReplicationOutboxJob(replicationJob{
			id:         id,
			result:     ReplicationResult{Command: "SETSTR", Key: key},
			enqueuedAt: time.Unix(int64(id), 0),
			tasks: []replicationTask{{
				target:  TopologyNode{ID: "node-b", Address: target.URL},
				payload: CacheCommandRequest{Command: replicationSetCompactCommand, Key: key, BinaryValue: []byte("value")},
			}},
		})
		data, err := outbox.marshalJob(record)
		if err != nil {
			t.Fatalf("marshalJob(%d) error = %v", id, err)
		}
		batch.Put(replicationOutboxLevelDBJobKey(id), data)
	}
	if err := outbox.db.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
		t.Fatalf("outbox batch write error = %v", err)
	}

	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		AsyncQueueSize:     8,
		AsyncMaxAttempts:   1,
		AsyncOutbox:        outbox,
		AsyncRetryInterval: time.Millisecond,
	})
	t.Cleanup(replicator.Close)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("restored outbox did not start delivery")
	}
	replicator.mu.RLock()
	queueCapacity := cap(replicator.queue)
	queueDepth := len(replicator.queue)
	pending := len(replicator.pending)
	replicator.mu.RUnlock()
	if queueCapacity != 8 || queueDepth > 8 || pending > 8 {
		t.Fatalf("restored queue capacity/depth/pending = %d/%d/%d, want all bounded by 8", queueCapacity, queueDepth, pending)
	}
	if queue := replicator.LastResult().Queue; queue == nil || !queue.DurableBacklog {
		t.Fatalf("restored queue stats = %#v, want durable backlog visible", queue)
	}
	concurrentKey := "restore:101"
	concurrentJob := replicationJob{
		result: ReplicationResult{Command: "SETSTR", Key: concurrentKey},
		tasks: []replicationTask{{
			target:  TopologyNode{ID: "node-b", Address: target.URL},
			payload: CacheCommandRequest{Command: replicationSetCompactCommand, Key: concurrentKey, BinaryValue: []byte("value")},
		}},
	}
	replicator.reserveAsyncJob(&concurrentJob)
	if err := replicator.persistAsyncJob(concurrentJob); err != nil {
		t.Fatalf("persistAsyncJob(concurrent) error = %v", err)
	}
	if deferred := replicator.prepareAsyncJobForQueue(concurrentJob); !deferred {
		t.Fatal("concurrent durable job was not deferred behind restored backlog")
	}
	releaseOnce.Do(func() { close(release) })
	for id := 1; id <= 101; id++ {
		select {
		case key := <-delivered:
			want := fmt.Sprintf("restore:%03d", id)
			if key != want {
				t.Fatalf("restored delivery %d key = %q, want %q", id, key, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for restored delivery %d", id)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(outbox.jobs()) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if jobs := outbox.jobs(); len(jobs) != 0 {
		t.Fatalf("outbox jobs after replay = %d, want 0", len(jobs))
	}
	if queue := replicator.LastResult().Queue; queue == nil || queue.DurableBacklog {
		t.Fatalf("final queue stats = %#v, want durable backlog cleared", queue)
	}
}

func TestReplicationGRPCStreamHTTPFallbackIsConfigurable(t *testing.T) {
	var digestRequests atomic.Int64
	var writeRequests atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		if normalizedCommand(request.Command) == replicationDigestCommand {
			digestRequests.Add(1)
			rejectReplicationDigestTestCommand(t, w, r, request)
			return
		}
		writeRequests.Add(1)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()
	topology := replicationTestTopology(t, target.URL)

	for _, tt := range []struct {
		name            string
		disableFallback bool
		wantOK          bool
		wantWrites      int64
	}{
		{name: "fallback enabled", wantOK: true, wantWrites: 1},
		{name: "fallback disabled", disableFallback: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			digestRequests.Store(0)
			writeRequests.Store(0)
			trie := newTestTrie(t)
			trie.UpsertString("session:1", "value")
			replicator := NewHTTPReplicator(HTTPReplicatorOptions{
				Self:                "node-a",
				Topology:            topology,
				Election:            NewElectionStore(topology, ElectionOptions{}),
				Client:              target.Client(),
				Transport:           ReplicationTransportGRPCStream,
				DisableHTTPFallback: tt.disableFallback,
			})
			result := replicator.syncAllPaged(context.Background(), trie, "session:", 10)
			if len(result.Targets) != 1 || result.Targets[0].OK != tt.wantOK {
				t.Fatalf("sync targets = %#v, want OK=%v", result.Targets, tt.wantOK)
			}
			if got := digestRequests.Load(); got != 1 {
				t.Fatalf("HTTP digest requests = %d, want 1", got)
			}
			if got := writeRequests.Load(); got != tt.wantWrites {
				t.Fatalf("HTTP fallback writes = %d, want %d", got, tt.wantWrites)
			}
			if tt.disableFallback && !strings.Contains(result.Targets[0].Error, "grpc_address") {
				t.Fatalf("disabled fallback error = %q, want missing grpc_address", result.Targets[0].Error)
			}
		})
	}
}

func TestReplicationGRPCStreamLiveHTTPFallbackIsConfigurable(t *testing.T) {
	var requests atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		if normalizedCommand(request.Command) != replicationSetCompactCommand {
			t.Fatalf("live fallback command = %q, want %s", request.Command, replicationSetCompactCommand)
		}
		requests.Add(1)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()
	topology := replicationTestTopology(t, target.URL)

	for _, tt := range []struct {
		name            string
		disableFallback bool
		wantOK          bool
		wantRequests    int64
	}{
		{name: "fallback enabled", wantOK: true, wantRequests: 1},
		{name: "fallback disabled", disableFallback: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requests.Store(0)
			trie := newTestTrie(t)
			trie.UpsertString("live:key", "value")
			replicator := NewHTTPReplicator(HTTPReplicatorOptions{
				Self:                "node-a",
				Topology:            topology,
				Election:            NewElectionStore(topology, ElectionOptions{}),
				Client:              target.Client(),
				Transport:           ReplicationTransportGRPCStream,
				DisableHTTPFallback: tt.disableFallback,
			})
			t.Cleanup(replicator.Close)
			result := replicator.ReplicateCommand(context.Background(), trie,
				CacheCommandRequest{Command: "SETSTR", Key: "live:key", Value: "value"}, CacheCommandResponse{OK: true})
			if len(result.Targets) != 1 || result.Targets[0].OK != tt.wantOK {
				t.Fatalf("live replication targets = %#v, want OK=%v", result.Targets, tt.wantOK)
			}
			if got := requests.Load(); got != tt.wantRequests {
				t.Fatalf("live HTTP fallback requests = %d, want %d", got, tt.wantRequests)
			}
			if tt.disableFallback && !strings.Contains(result.Targets[0].Error, "grpc_address") {
				t.Fatalf("disabled live fallback error = %q, want missing grpc_address", result.Targets[0].Error)
			}
		})
	}
}

func TestReplicationGRPCStreamDigestDeleteUsesConfiguredHTTPFallback(t *testing.T) {
	var requests atomic.Int64
	var received CacheCommandRequest
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = mustDecodeReplicationTestCommand(t, w, r)
		requests.Add(1)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer targetServer.Close()

	trie := newTestTrie(t)
	trie.UpsertString("session:new", "value")
	topology := replicationTestTopology(t, targetServer.URL)
	changes := []replicationDigestChange{{key: "session:new"}, {key: "session:stale"}}

	for _, tt := range []struct {
		name            string
		disableFallback bool
		wantOK          bool
		wantRequests    int64
	}{
		{name: "fallback enabled", wantOK: true, wantRequests: 1},
		{name: "fallback disabled", disableFallback: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requests.Store(0)
			received = CacheCommandRequest{}
			replicator := NewHTTPReplicator(HTTPReplicatorOptions{
				Self:                "node-a",
				Topology:            topology,
				Election:            NewElectionStore(topology, ElectionOptions{}),
				Client:              targetServer.Client(),
				DisableHTTPFallback: tt.disableFallback,
			})
			t.Cleanup(replicator.Close)
			routing, ok := replicator.snapshotReplicationRouting()
			if !ok {
				t.Fatal("snapshotReplicationRouting() ok = false")
			}
			target := routing.nodes["node-b"]
			session := newReplicationGRPCSyncSession(context.Background(), replicator)
			t.Cleanup(session.close)

			targets, changed, deleted, _ := replicator.executeReplicationDigestChanges(
				context.Background(), trie, routing, target, changes, session, false,
			)
			if changed != 1 || deleted != 1 || len(targets) != 1 || targets[0].OK != tt.wantOK {
				t.Fatalf("digest repair = targets %#v changed %d deleted %d, want OK=%v and 1/1", targets, changed, deleted, tt.wantOK)
			}
			if got := requests.Load(); got != tt.wantRequests {
				t.Fatalf("HTTP repair requests = %d, want %d", got, tt.wantRequests)
			}
			if tt.disableFallback {
				if !strings.Contains(targets[0].Error, "requires sync payloads") {
					t.Fatalf("disabled fallback error = %q, want incompatible stream payload", targets[0].Error)
				}
				return
			}
			payloads := mustDecodeReplicationBatchValues(t, received)
			commands := make(map[string]string, len(payloads))
			for _, payload := range payloads {
				commands[payload.Key] = normalizedCommand(payload.Command)
			}
			if commands["session:new"] != replicationSetCompactCommand || commands["session:stale"] != "INTERNALDEL" {
				t.Fatalf("HTTP repair commands = %#v, want compact set plus stale delete", commands)
			}
		})
	}
}

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func mustDecodeReplicationTestCommand(t *testing.T, w http.ResponseWriter, r *http.Request) CacheCommandRequest {
	t.Helper()
	request, _, closeBody, ok := monitoringCommandRequest(w, r)
	if !ok {
		t.Fatal("decode replication command request failed")
	}
	defer closeBody()
	return request
}

func isTypedReplicationSetPayload(request CacheCommandRequest, key string) bool {
	if request.Key != key || request.Value != "" {
		return false
	}
	entry, err := decodeTypedReplicationSnapshot(key, request.Command, request.BinaryValue)
	return err == nil && entry.Key == key
}

func mustDecodeTypedReplicationSnapshot(t *testing.T, request CacheCommandRequest, key string) snapshotEntry {
	t.Helper()
	if request.Key != key || request.Value != "" {
		t.Fatalf("replication request = %#v, want typed binary snapshot for %q", request, key)
	}
	entry, err := decodeTypedReplicationSnapshot(key, request.Command, request.BinaryValue)
	if err != nil {
		t.Fatalf("decode typed replication snapshot error = %v", err)
	}
	return entry
}

func decodeTypedReplicationSnapshot(key string, command string, data []byte) (snapshotEntry, error) {
	switch normalizedCommand(command) {
	case replicationSetCompactCommand:
		return unmarshalReplicationValueBinary(key, data)
	case replicationSetBinaryCommand:
		return decodeLevelDBEntryForKey(key, data)
	default:
		return snapshotEntry{}, errors.New("not a typed replication set")
	}
}

func mustDecodeReplicationBatchValues(t *testing.T, request CacheCommandRequest) []CacheCommandRequest {
	t.Helper()
	if request.Command != "INTERNALBATCH" && request.Command != replicationBatchEnvelopeCommand {
		t.Fatalf("replication request command = %q, want internal batch", request.Command)
	}
	if len(request.Batch) > 0 {
		return append([]CacheCommandRequest(nil), request.Batch...)
	}
	out := make([]CacheCommandRequest, 0, len(request.Values))
	for idx, value := range request.Values {
		text, ok := value.(string)
		if !ok {
			t.Fatalf("batch value %d = %T, want string", idx, value)
		}
		var payload CacheCommandRequest
		if err := json.Unmarshal([]byte(text), &payload); err != nil {
			t.Fatalf("batch value %d JSON error = %v", idx, err)
		}
		out = append(out, payload)
	}
	return out
}

func rejectReplicationDigestTestCommand(t *testing.T, w http.ResponseWriter, r *http.Request, request CacheCommandRequest) bool {
	t.Helper()
	if normalizedCommand(request.Command) != replicationDigestCommand {
		return false
	}
	format, _ := commandWireFormatFromContentType(r.Header.Get("Content-Type"))
	writeCommandResponseWire(w, r, http.StatusOK, commandError("unsupported command"), format)
	return true
}

func TestReplicationBatchEnvelopeSharesMetadata(t *testing.T) {
	payloads := []CacheCommandRequest{
		{
			Command: "INTERNALSET",
			Key:     "session:1",
			Value:   `{"type":"string","string":"one"}`,
			Pairs: Map{
				replicationMetaSourceNode:          "node-a",
				replicationMetaSequence:            "41",
				replicationMetaTopologyFingerprint: "fingerprint-a",
			},
		},
		{
			Command: "INTERNALDEL",
			Key:     "session:2",
			Pairs: Map{
				replicationMetaSourceNode:          "node-a",
				replicationMetaSequence:            "42",
				replicationMetaTopologyFingerprint: "fingerprint-a",
			},
		},
	}

	envelope, err := replicationBatchEnvelopePayload(payloads)
	if err != nil {
		t.Fatalf("replicationBatchEnvelopePayload() error = %v", err)
	}
	if envelope.Command != "INTERNALBATCHV2" || len(envelope.Batch) != 2 {
		t.Fatalf("envelope = %#v, want INTERNALBATCHV2 with two payloads", envelope)
	}
	if source, sequence, fingerprint := replicationSafetyMetadata(envelope); source != "node-a" || sequence != 42 || fingerprint != "fingerprint-a" {
		t.Fatalf("envelope metadata = %q/%d/%q, want node-a/42/fingerprint-a", source, sequence, fingerprint)
	}
	for idx, payload := range envelope.Batch {
		if _, _, fingerprint := replicationSafetyMetadata(payload); len(payload.Pairs) != 0 || fingerprint != "" {
			t.Fatalf("envelope payload %d metadata = %#v, want shared envelope metadata only", idx, payload.Pairs)
		}
	}
	if payloads[0].Pairs[replicationMetaSequence] != "41" || payloads[1].Pairs[replicationMetaSequence] != "42" {
		t.Fatalf("source payload metadata mutated = %#v/%#v", payloads[0].Pairs, payloads[1].Pairs)
	}
}

func TestReplicationSyncBatchRequestBodyRoundTripsProtobuf(t *testing.T) {
	payloads := []replicationSyncPayload{
		{key: "session:1", binaryValue: []byte{0, 1, 2, 3}},
		{key: "session:\x00two", binaryValue: []byte{255, 4, 5}},
	}
	for _, tt := range []struct {
		name                 string
		compressionThreshold int
		wantEncoding         string
	}{
		{name: "uncompressed", compressionThreshold: 0},
		{name: "streaming gzip", compressionThreshold: 1, wantEncoding: "gzip"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			body, contentType, contentEncoding, err := replicationSyncBatchRequestBody(
				payloads, replicationSetCompactCommand, "node-a", 42, "fingerprint-a", tt.compressionThreshold,
			)
			if err != nil {
				t.Fatalf("replicationSyncBatchRequestBody() error = %v", err)
			}
			if contentType != commandWireContentTypeProtobuf || contentEncoding != tt.wantEncoding {
				t.Fatalf("content type/encoding = %q/%q, want protobuf/%q", contentType, contentEncoding, tt.wantEncoding)
			}
			reader := body
			if contentEncoding == "gzip" {
				gzipReader, err := gzip.NewReader(body)
				if err != nil {
					t.Fatalf("gzip.NewReader() error = %v", err)
				}
				defer gzipReader.Close()
				reader = gzipReader
			}
			data, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("ReadAll(request body) error = %v", err)
			}
			if closer, ok := body.(io.Closer); ok {
				if err := closer.Close(); err != nil {
					t.Fatalf("Close(request body) error = %v", err)
				}
			}
			if got := len(data); got != replicationSyncBatchProtoSize(payloads, replicationSetCompactCommand, "node-a", 42, "fingerprint-a") {
				t.Fatalf("protobuf size = %d, want exact estimate %d", got, replicationSyncBatchProtoSize(payloads, replicationSetCompactCommand, "node-a", 42, "fingerprint-a"))
			}
			decoded, err := decodeCommandRequestProto(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatalf("decodeCommandRequestProto() error = %v", err)
			}
			if decoded.Command != replicationBatchEnvelopeCommand || len(decoded.Batch) != len(payloads) {
				t.Fatalf("decoded envelope = %#v, want two sync payloads", decoded)
			}
			if source, sequence, fingerprint := replicationSafetyMetadata(decoded); source != "node-a" || sequence != 42 || fingerprint != "fingerprint-a" {
				t.Fatalf("decoded metadata = %q/%d/%q, want node-a/42/fingerprint-a", source, sequence, fingerprint)
			}
			for idx, payload := range payloads {
				child := decoded.Batch[idx]
				if child.Command != replicationSetCompactCommand || child.Key != payload.key || !bytes.Equal(child.BinaryValue, payload.binaryValue) {
					t.Fatalf("decoded child %d = %#v, want %q compact binary", idx, child, payload.key)
				}
			}
		})
	}
}

func TestDeferredReplicationBatchEnvelopeBorrowsPayloads(t *testing.T) {
	payloads := []CacheCommandRequest{
		{Command: replicationSetCompactCommand, Key: "session:1", BinaryValue: []byte("one")},
		{Command: "INTERNALDEL", Key: "session:2"},
	}

	envelope := replicationBatchEnvelopePayloadWithMetadata(payloads, "node-a", 42, "fingerprint-a")
	if envelope.Command != replicationBatchEnvelopeCommand || len(envelope.Batch) != len(payloads) {
		t.Fatalf("envelope = %#v, want %s with %d payloads", envelope, replicationBatchEnvelopeCommand, len(payloads))
	}
	if &envelope.Batch[0] != &payloads[0] {
		t.Fatalf("envelope batch backing array = %p, want borrowed payload array %p", &envelope.Batch[0], &payloads[0])
	}
	if source, sequence, fingerprint := replicationSafetyMetadata(envelope); source != "node-a" || sequence != 42 || fingerprint != "fingerprint-a" {
		t.Fatalf("envelope metadata = %q/%d/%q, want node-a/42/fingerprint-a", source, sequence, fingerprint)
	}
	if payloads[0].Pairs != nil || payloads[1].Pairs != nil {
		t.Fatalf("source payload metadata mutated = %#v/%#v", payloads[0].Pairs, payloads[1].Pairs)
	}
	if envelope.Batch[0].Key != "session:1" || string(envelope.Batch[0].BinaryValue) != "one" || envelope.Batch[1].Key != "session:2" {
		t.Fatalf("envelope children = %#v, want original payload fields", envelope.Batch)
	}
}

func TestReplicationCommandPayloadUsesTypedBinaryValue(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("session:1", "one")

	payload, ok := replicationCommandPayload(trie, "session:1", replicationPayloadSet)
	if !ok {
		t.Fatal("replicationCommandPayload() ok = false, want payload")
	}
	if payload.Command != replicationSetCompactCommand || payload.Value != "" || !replicationValueDataIsBinary(payload.BinaryValue) {
		t.Fatalf("replication payload = %#v, want compact keyless binary internal set", payload)
	}
	operation, err := commandReplicationValueOperation(payload.Key, payload.BinaryValue)
	if err != nil {
		t.Fatalf("commandSnapshotBinaryOperation() error = %v", err)
	}
	restored := newTestTrie(t)
	if response := executePreparedInternalReplicationCommand(restored, payload, &operation); !response.OK {
		t.Fatalf("executePreparedInternalReplicationCommand() = %#v, want success", response)
	}
	if got, ok, err := restored.GetStringChecked("session:1"); err != nil || !ok || got != "one" {
		t.Fatalf("restored GetStringChecked() = %q/%v/%v, want one/true/nil", got, ok, err)
	}
}

func TestHTTPReplicatorCompactSetFallsBackToV2Binary(t *testing.T) {
	var requests atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		switch requests.Add(1) {
		case 1:
			if request.Command != replicationSetCompactCommand || !replicationValueDataIsBinary(request.BinaryValue) {
				t.Fatalf("first request = %#v, want compact V3 set", request)
			}
			writeJSON(w, commandError("unsupported command"))
		case 2:
			if request.Command != replicationSetBinaryCommand || !levelDBEntryDataIsBinary(request.BinaryValue) {
				t.Fatalf("second request = %#v, want V2 binary fallback", request)
			}
			writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
		default:
			t.Fatalf("unexpected request %d", requests.Load())
		}
	}))
	defer target.Close()

	source := newTestTrie(t)
	source.UpsertString("session:1", "one")
	payload, ok := replicationCommandPayload(source, "session:1", replicationPayloadSet)
	if !ok {
		t.Fatal("replicationCommandPayload() ok = false")
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a", Client: target.Client()})
	payload = replicator.annotateReplicationPayloadWithFingerprint(payload, "fingerprint-a")
	result := replicator.executeReplicationTargetBatch(context.Background(), TopologyNode{ID: "node-b", Address: target.URL}, []CacheCommandRequest{payload})
	if !result.OK || requests.Load() != 2 {
		t.Fatalf("compact fallback result = %#v requests=%d, want successful V2 retry", result, requests.Load())
	}
}

func TestHTTPReplicatorCompactSetFallsBackThroughV2ToLegacy(t *testing.T) {
	var requests atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		switch requests.Add(1) {
		case 1:
			if request.Command != replicationSetCompactCommand {
				t.Fatalf("first command = %q, want compact V3", request.Command)
			}
			writeJSON(w, commandError("unsupported command"))
		case 2:
			if request.Command != replicationSetBinaryCommand {
				t.Fatalf("second command = %q, want binary V2", request.Command)
			}
			writeJSON(w, commandError("unsupported command"))
		case 3:
			if request.Command != "INTERNALSET" || request.Value == "" || len(request.BinaryValue) != 0 {
				t.Fatalf("third request = %#v, want legacy JSON set", request)
			}
			writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
		default:
			t.Fatalf("unexpected request %d", requests.Load())
		}
	}))
	defer target.Close()

	source := newTestTrie(t)
	source.UpsertString("session:1", "one")
	payload, ok := replicationCommandPayload(source, "session:1", replicationPayloadSet)
	if !ok {
		t.Fatal("replicationCommandPayload() ok = false")
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Client: target.Client()})
	result := replicator.executeReplicationTargetBatch(context.Background(), TopologyNode{ID: "node-b", Address: target.URL}, []CacheCommandRequest{payload})
	if !result.OK || requests.Load() != 3 {
		t.Fatalf("legacy fallback result = %#v requests=%d, want successful three-step retry", result, requests.Load())
	}
}

func TestHTTPReplicatorDeferredCompactBatchFallsBackToV2(t *testing.T) {
	var requests atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		if request.Command != replicationBatchEnvelopeCommand || len(request.Batch) != 2 {
			t.Fatalf("request = %#v, want two-item batch envelope", request)
		}
		if source, sequence, fingerprint := replicationSafetyMetadata(request); source != "node-a" || sequence == 0 || fingerprint != "fingerprint-a" {
			t.Fatalf("envelope metadata = %q/%d/%q", source, sequence, fingerprint)
		}
		switch requests.Add(1) {
		case 1:
			if request.Batch[0].Command != replicationSetCompactCommand || !replicationValueDataIsBinary(request.Batch[0].BinaryValue) {
				t.Fatalf("first batch child = %#v, want compact V3", request.Batch[0])
			}
			writeJSON(w, commandError("internal replication batch value 0 must be INTERNALSET or INTERNALDEL"))
		case 2:
			for idx, payload := range request.Batch {
				if payload.Command != replicationSetBinaryCommand || !levelDBEntryDataIsBinary(payload.BinaryValue) {
					t.Fatalf("V2 batch child %d = %#v", idx, payload)
				}
			}
			writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
		default:
			t.Fatalf("unexpected request %d", requests.Load())
		}
	}))
	defer target.Close()

	source := newTestTrie(t)
	source.UpsertString("session:1", "one")
	source.UpsertString("session:2", "two")
	targetNode := TopologyNode{ID: "node-b", Address: target.URL}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a", Client: target.Client()})
	groups := make([]replicationTaskGroup, 0, 1)
	indexes := make(map[TopologyNode]int, 1)
	for _, key := range []string{"session:1", "session:2"} {
		payload, ok := replicationCommandPayload(source, key, replicationPayloadSet)
		if !ok {
			t.Fatalf("replicationCommandPayload(%q) ok = false", key)
		}
		groups = replicator.appendReplicationSyncPayloadToTargetGroups(groups, indexes, 2, []TopologyNode{targetNode}, payload.Key, payload.BinaryValue, "fingerprint-a")
	}
	result := replicator.executeReplicationTaskGroups(context.Background(), ReplicationResult{}, groups)
	if len(result.Targets) != 1 || !result.Targets[0].OK || requests.Load() != 2 {
		t.Fatalf("V2 batch fallback result = %#v requests=%d", result, requests.Load())
	}
}

func TestExecuteCacheCommandAcceptsCompactReplicationValue(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("session:1", "one")
	payload, ok := replicationCommandPayload(source, "session:1", replicationPayloadSet)
	if !ok {
		t.Fatal("replicationCommandPayload() ok = false")
	}

	restored := newTestTrie(t)
	response, rejected := executeCacheCommand(context.Background(), restored, payload, commandExecutionOptions{})
	if rejected || !response.OK {
		t.Fatalf("executeCacheCommand() = %#v/%v, want compact value accepted", response, rejected)
	}
	if got, ok, err := restored.GetStringChecked("session:1"); err != nil || !ok || got != "one" {
		t.Fatalf("restored value = %q/%v/%v, want one/true/nil", got, ok, err)
	}
}

func TestHTTPReplicatorBatchEnvelopeFallsBackToLegacyMetadata(t *testing.T) {
	var requests atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requestNumber := requests.Add(1)
		if requestNumber == 1 {
			if request.Command != replicationBatchEnvelopeCommand {
				t.Fatalf("first batch command = %q, want %s", request.Command, replicationBatchEnvelopeCommand)
			}
			writeJSON(w, commandError("unsupported command"))
			return
		}
		if request.Command != "INTERNALBATCH" || len(request.Batch) != 2 {
			t.Fatalf("fallback request = %#v, want legacy internal batch", request)
		}
		for idx, payload := range request.Batch {
			source, sequence, fingerprint := replicationSafetyMetadata(payload)
			if source != "node-a" || sequence == 0 || fingerprint != "fingerprint-a" {
				t.Fatalf("fallback payload %d metadata = %q/%d/%q", idx, source, sequence, fingerprint)
			}
		}
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Client: target.Client()})
	payloads := []CacheCommandRequest{
		{Command: "INTERNALSET", Key: "session:1", Value: `{"type":"string","string":"one"}`, Pairs: Map{
			replicationMetaSourceNode: "node-a", replicationMetaSequence: "1", replicationMetaTopologyFingerprint: "fingerprint-a",
		}},
		{Command: "INTERNALDEL", Key: "session:2", Pairs: Map{
			replicationMetaSourceNode: "node-a", replicationMetaSequence: "2", replicationMetaTopologyFingerprint: "fingerprint-a",
		}},
	}
	result := replicator.executeReplicationTargetBatch(context.Background(), TopologyNode{ID: "node-b", Address: target.URL}, payloads)
	if !result.OK || result.Error != "" {
		t.Fatalf("fallback result = %#v, want success", result)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests = %d, want envelope plus one legacy fallback", got)
	}
}

func TestHTTPReplicatorTypedBatchFallsBackForPreviousEnvelopePeer(t *testing.T) {
	var requests atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		if requests.Add(1) == 1 {
			if request.Command != replicationBatchEnvelopeCommand || len(request.Batch) != 2 || request.Batch[0].Command != replicationSetBinaryCommand {
				t.Fatalf("first request = %#v, want typed envelope batch", request)
			}
			writeJSON(w, commandError("internal replication batch value 0 must be INTERNALSET or INTERNALDEL"))
			return
		}
		if request.Command != "INTERNALBATCH" || len(request.Batch) != 2 {
			t.Fatalf("fallback request = %#v, want legacy batch", request)
		}
		for idx, payload := range request.Batch {
			if payload.Command != "INTERNALSET" || payload.Value == "" || len(payload.BinaryValue) != 0 {
				t.Fatalf("fallback payload %d = %#v, want legacy JSON snapshot", idx, payload)
			}
		}
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	source := newTestTrie(t)
	source.UpsertString("session:1", "one")
	source.UpsertString("session:2", "two")
	payloads := make([]CacheCommandRequest, 0, 2)
	for _, key := range []string{"session:1", "session:2"} {
		payload, ok := replicationCommandPayload(source, key, replicationPayloadSet)
		if !ok {
			t.Fatalf("replicationCommandPayload(%q) ok = false", key)
		}
		payload, err := replicationBinaryV2Payload(payload)
		if err != nil {
			t.Fatalf("replicationBinaryV2Payload(%q) error = %v", key, err)
		}
		payloads = append(payloads, payload)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Client: target.Client()})
	result := replicator.executeReplicationTargetBatch(context.Background(), TopologyNode{ID: "node-b", Address: target.URL}, payloads)
	if !result.OK || requests.Load() != 2 {
		t.Fatalf("typed batch result = %#v requests=%d, want successful legacy retry", result, requests.Load())
	}
}

func TestHTTPReplicatorTypedBinaryFallsBackToLegacySet(t *testing.T) {
	var requests atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requestNumber := requests.Add(1)
		if requestNumber == 1 {
			if request.Command != replicationSetBinaryCommand || len(request.BinaryValue) == 0 || request.Value != "" {
				t.Fatalf("typed request = %#v, want binary internal set", request)
			}
			writeJSON(w, commandError("unsupported command"))
			return
		}
		if request.Command != "INTERNALSET" || request.Value == "" || len(request.BinaryValue) != 0 {
			t.Fatalf("fallback request = %#v, want legacy JSON internal set", request)
		}
		if _, err := commandSnapshotOperation(request.Key, request.Value); err != nil {
			t.Fatalf("fallback snapshot JSON error = %v", err)
		}
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	source := newTestTrie(t)
	source.UpsertString("session:1", "one")
	payload, ok := replicationCommandPayload(source, "session:1", replicationPayloadSet)
	if !ok {
		t.Fatal("replicationCommandPayload() ok = false")
	}
	payload, err := replicationBinaryV2Payload(payload)
	if err != nil {
		t.Fatalf("replicationBinaryV2Payload() error = %v", err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Client: target.Client()})
	result := replicator.executeReplicationTargetBatch(context.Background(), TopologyNode{ID: "node-b", Address: target.URL}, []CacheCommandRequest{payload})
	if !result.OK || result.Error != "" {
		t.Fatalf("typed fallback result = %#v, want success", result)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests = %d, want typed request plus one legacy fallback", got)
	}
}

func TestHTTPReplicatorJSONWireSendsLegacySetDirectly(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	source := newTestTrie(t)
	source.UpsertString("session:1", "one")
	payload, ok := replicationCommandPayload(source, "session:1", replicationPayloadSet)
	if !ok {
		t.Fatal("replicationCommandPayload() ok = false")
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Client: target.Client(), WireFormat: CommandWireFormatJSON})
	result := replicator.executeReplicationTargetBatch(context.Background(), TopologyNode{ID: "node-b", Address: target.URL}, []CacheCommandRequest{payload})
	if !result.OK {
		t.Fatalf("JSON wire result = %#v, want success", result)
	}
	request := <-requests
	if request.Command != "INTERNALSET" || request.Value == "" || len(request.BinaryValue) != 0 {
		t.Fatalf("JSON wire request = %#v, want direct legacy set", request)
	}
	select {
	case extra := <-requests:
		t.Fatalf("unexpected JSON wire fallback request = %#v", extra)
	default:
	}
}

func TestHTTPReplicatorJSONWireSendsCompactSyncGroupAsLegacyBatch(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	source := newTestTrie(t)
	source.UpsertString("session:1", "one")
	source.UpsertString("session:2", "two")
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a", Client: target.Client(), WireFormat: CommandWireFormatJSON})
	targetNode := TopologyNode{ID: "node-b", Address: target.URL}
	groups := make([]replicationTaskGroup, 0, 1)
	indexes := make(map[TopologyNode]int, 1)
	for _, key := range []string{"session:1", "session:2"} {
		payload, ok := replicationCommandPayload(source, key, replicationPayloadSet)
		if !ok {
			t.Fatalf("replicationCommandPayload(%q) ok = false", key)
		}
		groups = replicator.appendReplicationSyncPayloadToTargetGroups(groups, indexes, 2, []TopologyNode{targetNode}, payload.Key, payload.BinaryValue, "fingerprint-a")
	}
	result := replicator.executeReplicationTaskGroups(context.Background(), ReplicationResult{}, groups)
	if len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("JSON sync result = %#v, want success", result)
	}
	request := <-requests
	if request.Command != "INTERNALBATCH" || len(request.Batch) != 2 {
		t.Fatalf("JSON sync request = %#v, want two-item legacy batch", request)
	}
	for idx, payload := range request.Batch {
		if payload.Command != "INTERNALSET" || payload.Value == "" || len(payload.BinaryValue) != 0 {
			t.Fatalf("JSON sync child %d = %#v, want legacy JSON set", idx, payload)
		}
		if source, sequence, fingerprint := replicationSafetyMetadata(payload); source != "node-a" || sequence == 0 || fingerprint != "fingerprint-a" {
			t.Fatalf("JSON sync child %d metadata = %q/%d/%q", idx, source, sequence, fingerprint)
		}
	}
}

func replicationRequestKeys(t *testing.T, request CacheCommandRequest) []string {
	t.Helper()
	switch request.Command {
	case "INTERNALSET", replicationSetBinaryCommand, replicationSetCompactCommand, "INTERNALDEL":
		return []string{request.Key}
	case "INTERNALBATCH", replicationBatchEnvelopeCommand:
		payloads := mustDecodeReplicationBatchValues(t, request)
		keys := make([]string, 0, len(payloads))
		for _, payload := range payloads {
			if payload.Command != "INTERNALSET" && payload.Command != replicationSetBinaryCommand && payload.Command != replicationSetCompactCommand && payload.Command != "INTERNALDEL" {
				t.Fatalf("batch payload = %#v, want internal replication command", payload)
			}
			keys = append(keys, payload.Key)
		}
		return keys
	default:
		t.Fatalf("replication request = %#v, want internal replication command", request)
		return nil
	}
}

func assertReplicationResultTiming(t *testing.T, result ReplicationResult) {
	t.Helper()
	if result.StartedAt == nil || result.FinishedAt == nil {
		t.Fatalf("replication timing = %v/%v, want started and finished timestamps", result.StartedAt, result.FinishedAt)
	}
	if result.StartedAt.IsZero() || result.FinishedAt.IsZero() || result.FinishedAt.Before(*result.StartedAt) {
		t.Fatalf("replication timing = %s/%s, want ordered non-zero timestamps", result.StartedAt, result.FinishedAt)
	}
	if result.DurationMillis < 0 {
		t.Fatalf("replication duration = %d, want non-negative", result.DurationMillis)
	}
}

func TestReplicationHealthScoresQueueState(t *testing.T) {
	healthy := withReplicationHealth(ReplicationResult{
		Queue: &ReplicationQueueStats{Enabled: true, Depth: 0, Capacity: 8},
	})
	if healthy.Health != "ok" || healthy.HealthScore != 100 || healthy.HealthReason != "healthy" {
		t.Fatalf("healthy replication = %#v, want ok 100", healthy)
	}

	degraded := withReplicationHealth(ReplicationResult{
		Queue: &ReplicationQueueStats{Enabled: true, Depth: 6, Capacity: 8},
	})
	if degraded.Health != "degraded" || degraded.HealthScore >= healthy.HealthScore {
		t.Fatalf("degraded replication = %#v, want lower degraded score", degraded)
	}

	unhealthy := withReplicationHealth(ReplicationResult{
		Queue: &ReplicationQueueStats{
			Enabled:               true,
			Depth:                 8,
			Capacity:              8,
			Dropped:               2,
			Attempts:              10,
			Failures:              8,
			OldestQueuedAgeMillis: int64((6 * time.Minute).Milliseconds()),
		},
	})
	if unhealthy.Health != "unhealthy" || unhealthy.HealthScore >= 60 {
		t.Fatalf("unhealthy replication = %#v, want unhealthy score below 60", unhealthy)
	}

	disabled := withReplicationHealth(ReplicationResult{Skipped: true, Reason: "replication is not configured"})
	if disabled.Health != "disabled" || disabled.HealthScore != 0 {
		t.Fatalf("disabled replication = %#v, want disabled 0", disabled)
	}
}

func writeReplicationTestCommandResponse(w http.ResponseWriter, r *http.Request, response CacheCommandResponse) {
	writeCommandResponseWire(w, r, http.StatusOK, response, CommandWireFormatJSON)
}

type trackingReadCloser struct {
	reader  *strings.Reader
	closed  bool
	drained bool
}

func newTrackingReadCloser(value string) *trackingReadCloser {
	return &trackingReadCloser{reader: strings.NewReader(value)}
}

func (body *trackingReadCloser) Read(data []byte) (int, error) {
	n, err := body.reader.Read(data)
	if err != nil {
		body.drained = true
	}
	return n, err
}

func (body *trackingReadCloser) Close() error {
	body.closed = true
	return nil
}

func TestHTTPReplicatorReplicatesSetAndDeleteToOwners(t *testing.T) {
	var requests []CacheCommandRequest
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/commands" {
			t.Fatalf("path = %s, want /api/commands", r.URL.Path)
		}
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests = append(requests, request)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	response := trie.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"})
	result := replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("set replication result = %#v, want one ok target", result)
	}

	response = trie.ExecuteCommand(CacheCommandRequest{Command: "DEL", Key: "session:1"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "DEL", Key: "session:1"}, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("delete replication result = %#v, want one ok target", result)
	}

	if len(requests) != 2 {
		t.Fatalf("replicated requests len = %d, want 2", len(requests))
	}
	if !isTypedReplicationSetPayload(requests[0], "session:1") {
		t.Fatalf("first replicated request = %#v, want typed binary snapshot", requests[0])
	}
	if requests[1].Command != "INTERNALDEL" || requests[1].Key != "session:1" {
		t.Fatalf("second replicated request = %#v, want INTERNALDEL", requests[1])
	}
	if got := replicator.LastResult(); !reflect.DeepEqual(got, result) {
		t.Fatalf("LastResult() = %#v, want last result %#v", got, result)
	}
}

func TestHTTPReplicatorSendsReplicationAuthToken(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer replica-secret" {
			t.Fatalf("Authorization = %q, want bearer replication token", got)
		}
		if got := r.Header.Get("X-Hatrie-Replication-Token"); got != "replica-secret" {
			t.Fatalf("X-Hatrie-Replication-Token = %q, want replication token", got)
		}
		_ = mustDecodeReplicationTestCommand(t, w, r)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:      "node-a",
		Topology:  topology,
		Election:  election,
		Client:    target.Client(),
		AuthToken: " replica-secret ",
	})

	response := trie.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"})
	result := replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("replication result = %#v, want one authenticated ok target", result)
	}
}

func TestHTTPReplicatorAnnotatesReplicationSafetyMetadata(t *testing.T) {
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Mode:    TopologyModeSharded,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a:8080"},
			{ID: "node-b", Address: "http://node-b:8080"},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a", Topology: topology})

	first := replicator.annotateReplicationPayload(CacheCommandRequest{Command: "INTERNALSET", Key: "k", Value: "{}"})
	second := replicator.annotateReplicationPayload(CacheCommandRequest{Command: "INTERNALDEL", Key: "k"})
	if first.Pairs[replicationMetaSourceNode] != "node-a" || second.Pairs[replicationMetaSourceNode] != "node-a" {
		t.Fatalf("source metadata = %#v/%#v, want node-a", first.Pairs, second.Pairs)
	}
	if first.Pairs[replicationMetaTopologyFingerprint] != topology.Fingerprint() || second.Pairs[replicationMetaTopologyFingerprint] != topology.Fingerprint() {
		t.Fatalf("topology fingerprint metadata = %#v/%#v, want %q", first.Pairs, second.Pairs, topology.Fingerprint())
	}
	firstSequence, err := commandUint64Value(first.Pairs[replicationMetaSequence])
	if err != nil {
		t.Fatalf("first sequence metadata error = %v", err)
	}
	secondSequence, err := commandUint64Value(second.Pairs[replicationMetaSequence])
	if err != nil {
		t.Fatalf("second sequence metadata error = %v", err)
	}
	if firstSequence == 0 || secondSequence != firstSequence+1 {
		t.Fatalf("replication sequences = %d/%d, want monotonically increasing", firstSequence, secondSequence)
	}
}

func TestExecuteCacheCommandSkipsDuplicateReplicationSequence(t *testing.T) {
	topology, err := NewTopologyStore(SingleNodeTopology("node-b", ""))
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	safety := NewReplicationSafetyStore()
	ht := CreateHatTrie()
	defer ht.Destroy()
	source := CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("session:1", "replicated")
	dump := source.ExecuteCommand(CacheCommandRequest{Command: "DUMP", Key: "session:1"})
	if !dump.OK || dump.Value == "" {
		t.Fatalf("source dump = %#v, want snapshot", dump)
	}

	request := CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:1",
		Value:   dump.Value,
		Pairs: Map{
			replicationMetaSourceNode:          "node-a",
			replicationMetaSequence:            "1",
			replicationMetaTopologyFingerprint: topology.Fingerprint(),
		},
	}
	response, rejected := executeCacheCommand(context.Background(), ht, request, commandExecutionOptions{
		Topology:          topology,
		ReplicationSafety: safety,
	})
	if rejected || !response.OK {
		t.Fatalf("first replication response = %#v rejected=%v, want ok", response, rejected)
	}
	duplicate := request
	duplicate.Command = "INTERNALDEL"
	duplicate.Value = ""
	response, rejected = executeCacheCommand(context.Background(), ht, duplicate, commandExecutionOptions{
		Topology:          topology,
		ReplicationSafety: safety,
	})
	if rejected || !response.OK || response.Message != "duplicate replication command" {
		t.Fatalf("duplicate replication response = %#v rejected=%v, want duplicate ok", response, rejected)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "session:1"}); !got.OK || got.Value != "replicated" {
		t.Fatalf("GET after duplicate replication = %#v, want original value", got)
	}
}

func TestExecuteCacheCommandAllowsOutOfOrderReplicationForDifferentKeys(t *testing.T) {
	topology, err := NewTopologyStore(SingleNodeTopology("node-b", ""))
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	safety := NewReplicationSafetyStore()
	target := newTestTrie(t)
	source := newTestTrie(t)
	source.UpsertString("key:a", "a")
	source.UpsertString("key:b", "b")

	for _, item := range []struct {
		key      string
		sequence string
	}{
		{key: "key:b", sequence: "2"},
		{key: "key:a", sequence: "1"},
	} {
		binaryValue, ok, err := source.commandDumpEntryBinary(item.key)
		if err != nil || !ok {
			t.Fatalf("commandDumpEntryBinary(%s) = %v/%v", item.key, ok, err)
		}
		response, rejected := executeCacheCommand(context.Background(), target, CacheCommandRequest{
			Command:     replicationSetCompactCommand,
			Key:         item.key,
			BinaryValue: binaryValue,
			Pairs: Map{
				replicationMetaSourceNode:          "node-a",
				replicationMetaSequence:            item.sequence,
				replicationMetaTopologyFingerprint: topology.Fingerprint(),
			},
		}, commandExecutionOptions{Topology: topology, ReplicationSafety: safety})
		if rejected || !response.OK || response.Message == "duplicate replication command" {
			t.Fatalf("replication %s sequence %s = %#v rejected=%v, want applied", item.key, item.sequence, response, rejected)
		}
	}
	if got := target.GetString("key:a"); got != "a" {
		t.Fatalf("target key:a = %q, want a", got)
	}
	if got := target.GetString("key:b"); got != "b" {
		t.Fatalf("target key:b = %q, want b", got)
	}
}

func TestExecuteCacheCommandRejectsReplicationTopologyMismatch(t *testing.T) {
	localTopology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Mode:    TopologyModeSharded,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a:8080"},
			{ID: "node-b", Address: "http://node-b:9090"},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore(local) error = %v", err)
	}
	remoteTopology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Mode:    TopologyModeSharded,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a:8080"},
			{ID: "node-b", Address: "http://node-b:8080"},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore(remote) error = %v", err)
	}
	ht := CreateHatTrie()
	defer ht.Destroy()

	response, rejected := executeCacheCommand(context.Background(), ht, CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:1",
		Value:   `{"type":"string","string":"replicated"}`,
		Pairs: Map{
			replicationMetaSourceNode:          "node-a",
			replicationMetaSequence:            "1",
			replicationMetaTopologyFingerprint: remoteTopology.Fingerprint(),
		},
	}, commandExecutionOptions{
		Topology:          localTopology,
		ReplicationSafety: NewReplicationSafetyStore(),
	})
	if !rejected || response.OK || !strings.Contains(response.Message, "topology fingerprint mismatch") {
		t.Fatalf("topology mismatch response = %#v rejected=%v, want rejected mismatch", response, rejected)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "session:1"}); got.Value != "" {
		t.Fatalf("GET after rejected replication = %#v, want missing value", got)
	}
}

func TestExecuteCacheCommandAllowsReplicationFingerprintWithoutClusterTopology(t *testing.T) {
	localTopology, err := NewTopologyStore(SingleNodeTopology("node-b", ""))
	if err != nil {
		t.Fatalf("NewTopologyStore(local) error = %v", err)
	}
	remoteTopology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Mode:    TopologyModeSharded,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a:8080"},
			{ID: "node-b", Address: "http://node-b:8080"},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore(remote) error = %v", err)
	}
	ht := CreateHatTrie()
	defer ht.Destroy()

	response, rejected := executeCacheCommand(context.Background(), ht, CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:1",
		Value:   `{"type":"string","string":"replicated"}`,
		Pairs: Map{
			replicationMetaSourceNode:          "node-a",
			replicationMetaSequence:            "1",
			replicationMetaTopologyFingerprint: remoteTopology.Fingerprint(),
		},
	}, commandExecutionOptions{
		Topology:          localTopology,
		ReplicationSafety: NewReplicationSafetyStore(),
	})
	if rejected || !response.OK {
		t.Fatalf("single-node replication response = %#v rejected=%v, want ok", response, rejected)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "session:1"}); !got.OK || got.Value != "replicated" {
		t.Fatalf("GET after single-node replication = %#v, want replicated value", got)
	}
}

func TestHTTPReplicatorUsesProtobufWireByDefault(t *testing.T) {
	var gotRequest CacheCommandRequest
	var gotContentType string
	var gotAccept string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		gotAccept = r.Header.Get("Accept")
		gotRequest = mustDecodeReplicationTestCommand(t, w, r)
		writeReplicationTestCommandResponse(w, r, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Client: target.Client(),
	})
	result := replicator.postReplicationCommand(context.Background(), TopologyNode{
		ID:      "node-b",
		Address: target.URL,
	}, CacheCommandRequest{Command: "INTERNALSET", Key: "session:1", Value: `{"type":"string","string":"value"}`})

	if !result.OK || result.Error != "" {
		t.Fatalf("postReplicationCommand() = %#v, want protobuf ok", result)
	}
	if gotContentType != commandWireContentTypeProtobuf || gotAccept != commandWireContentTypeProtobuf {
		t.Fatalf("wire headers content-type/accept = %q/%q, want protobuf", gotContentType, gotAccept)
	}
	if gotRequest.Command != "INTERNALSET" || gotRequest.Key != "session:1" || gotRequest.Value == "" {
		t.Fatalf("protobuf replicated request = %#v, want INTERNALSET snapshot", gotRequest)
	}
}

func TestHTTPReplicatorFallsBackToJSONForStructuredPayload(t *testing.T) {
	var gotContentType string
	var gotAccept string
	var gotRequest CacheCommandRequest
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		gotAccept = r.Header.Get("Accept")
		gotRequest = mustDecodeReplicationTestCommand(t, w, r)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Client: target.Client(),
	})
	request := CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:structured",
		Pairs: Map{
			"profile": Map{
				"name": "ivi",
				"tags": Slice{"alpha", "beta"},
			},
		},
	}
	result := replicator.postReplicationCommand(context.Background(), TopologyNode{
		ID:      "node-b",
		Address: target.URL,
	}, request)

	if !result.OK || result.Error != "" {
		t.Fatalf("postReplicationCommand() = %#v, want JSON fallback ok", result)
	}
	if gotContentType != commandWireContentTypeJSON || gotAccept != commandWireContentTypeJSON {
		t.Fatalf("wire headers content-type/accept = %q/%q, want JSON fallback", gotContentType, gotAccept)
	}
	if !reflect.DeepEqual(gotRequest, request) {
		t.Fatalf("replicated structured request = %#v, want %#v", gotRequest, request)
	}
}

func TestHTTPReplicatorUsesConfiguredJSONWire(t *testing.T) {
	var gotContentType string
	var gotAccept string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		gotAccept = r.Header.Get("Accept")
		request := mustDecodeReplicationTestCommand(t, w, r)
		if request.Command != "INTERNALDEL" || request.Key != "session:1" {
			t.Fatalf("json fallback request = %#v, want INTERNALDEL", request)
		}
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Client:     target.Client(),
		WireFormat: CommandWireFormatJSON,
	})
	result := replicator.postReplicationCommand(context.Background(), TopologyNode{
		ID:      "node-b",
		Address: target.URL,
	}, CacheCommandRequest{Command: "INTERNALDEL", Key: "session:1"})

	if !result.OK || result.Error != "" {
		t.Fatalf("postReplicationCommand() = %#v, want configured JSON ok", result)
	}
	if gotContentType != commandWireContentTypeJSON || gotAccept != commandWireContentTypeJSON {
		t.Fatalf("wire headers content-type/accept = %q/%q, want json", gotContentType, gotAccept)
	}
}

func TestHTTPReplicatorNilReceiverReportsNotConfigured(t *testing.T) {
	var replicator *HTTPReplicator
	trie := newTestTrie(t)
	trie.UpsertString("session:1", "value")

	result := replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{
		Command: "SETSTR",
		Key:     "session:1",
		Value:   "value",
	}, CacheCommandResponse{OK: true})
	if !result.Skipped || result.Command != "SETSTR" || result.Key != "session:1" || result.Reason != "replication is not configured" {
		t.Fatalf("ReplicateCommand(nil) = %#v, want not configured skip", result)
	}

	result = replicator.SyncAll(context.Background(), trie, "session:")
	if !result.Skipped || result.Command != "SYNC" || result.Key != "session:" || result.Reason != "replication is not configured" {
		t.Fatalf("SyncAll(nil) = %#v, want not configured sync skip", result)
	}
	if got := replicator.LastResult(); !got.Skipped || got.Reason != "replication is not configured" {
		t.Fatalf("LastResult(nil) = %#v, want not configured skip", got)
	}
}

func TestHTTPReplicatorSkipsWhenNotLeaderOrInternalCommand(t *testing.T) {
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, "127.0.0.1:1")
	election := NewElectionStore(topology, ElectionOptions{})
	if err := election.MarkOffline("node-a"); err != nil {
		t.Fatalf("MarkOffline(node-a) error = %v", err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
	})

	trie.UpsertString("session:1", "value")
	result := replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}, CacheCommandResponse{OK: true})
	if !result.Skipped || result.Reason != "local node is not elected leader" {
		t.Fatalf("not leader result = %#v, want skipped not leader", result)
	}

	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "INTERNALDEL", Key: "session:1"}, CacheCommandResponse{OK: true})
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("internal command result = %#v, want skipped internal command", result)
	}
}

func TestReplicationPayloadKindUsesJournaledMutationClassification(t *testing.T) {
	ttl := int64(30)
	priority := int64(5)
	for _, tt := range []struct {
		name    string
		request CacheCommandRequest
		want    replicationPayloadKind
	}{
		{
			name:    "read command",
			request: CacheCommandRequest{Command: "GET", Key: "key"},
			want:    replicationPayloadNone,
		},
		{
			name:    "failed write response",
			request: CacheCommandRequest{Command: "SETSTR", Key: "key", Value: "value"},
			want:    replicationPayloadNone,
		},
		{
			name:    "internal set",
			request: CacheCommandRequest{Command: "INTERNALSET", Key: "key", Value: `{"type":"string","string":"value"}`},
			want:    replicationPayloadNone,
		},
		{
			name:    "internal delete",
			request: CacheCommandRequest{Command: "INTERNALDEL", Key: "key"},
			want:    replicationPayloadNone,
		},
		{
			name:    "delete",
			request: CacheCommandRequest{Command: "DEL", Key: "key"},
			want:    replicationPayloadDelete,
		},
		{
			name:    "ttl alias",
			request: CacheCommandRequest{Command: "SETSTRX", Key: "key", Value: "value", TTLSeconds: &ttl},
			want:    replicationPayloadSet,
		},
		{
			name:    "priority queue alias",
			request: CacheCommandRequest{Command: "PUSHPRIORITY", Key: "jobs", Value: "job", Priority: &priority},
			want:    replicationPayloadSet,
		},
		{
			name:    "top-k alias",
			request: CacheCommandRequest{Command: "TOPKRESERVE", Key: "top", Value: "3"},
			want:    replicationPayloadSet,
		},
		{
			name:    "quantile alias",
			request: CacheCommandRequest{Command: "QADD", Key: "latency", Value: "12.5"},
			want:    replicationPayloadSet,
		},
		{
			name:    "fenwick alias",
			request: CacheCommandRequest{Command: "FWADD", Key: "scores", Values: Slice{json.Number("2"), json.Number("7")}},
			want:    replicationPayloadSet,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response := CacheCommandResponse{OK: true}
			if tt.name == "failed write response" {
				response.OK = false
			}
			if got := replicationPayloadKindFor(tt.request, response); got != tt.want {
				t.Fatalf("replicationPayloadKindFor(%#v) = %v, want %v", tt.request, got, tt.want)
			}
		})
	}
}

func TestHTTPReplicatorSkipsCanceledContextBeforeNetwork(t *testing.T) {
	called := make(chan struct{}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called <- struct{}{}
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	trie.UpsertString("session:1", "value")
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result := replicator.ReplicateCommand(ctx, trie, CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}, CacheCommandResponse{OK: true})
	if !result.Skipped || result.Reason != context.Canceled.Error() {
		t.Fatalf("canceled replication result = %#v, want skipped context canceled", result)
	}
	select {
	case <-called:
		t.Fatal("canceled replication reached remote target")
	default:
	}
	if got := replicator.LastResult(); !reflect.DeepEqual(got, result) {
		t.Fatalf("LastResult() = %#v, want canceled result %#v", got, result)
	}
}

func TestHTTPReplicatorAcceptsNilContext(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	response := trie.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"})
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	result := replicator.ReplicateCommand(nil, trie, CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("nil context replication result = %#v, want one ok target", result)
	}
	select {
	case request := <-requests:
		if !isTypedReplicationSetPayload(request, "session:1") {
			t.Fatalf("nil context replicated request = %#v, want typed binary snapshot", request)
		}
	default:
		t.Fatal("nil context replication did not reach remote target")
	}
}

func TestHTTPReplicatorAsyncQueuesMaterializedPayload(t *testing.T) {
	release := make(chan struct{})
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 2,
	})
	defer replicator.Close()

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "first"}
	response := trie.ExecuteCommand(write)
	result := replicator.ReplicateCommand(context.Background(), trie, write, response)
	if !result.Queued || result.Skipped || len(result.Targets) != 1 {
		t.Fatalf("async enqueue result = %#v, want one queued target", result)
	}
	trie.UpsertString("session:1", "second")
	close(release)

	select {
	case request := <-requests:
		if !isTypedReplicationSetPayload(request, "session:1") {
			t.Fatalf("async request = %#v, want typed binary session:1", request)
		}
		entry := mustDecodeTypedReplicationSnapshot(t, request, "session:1")
		if entry.String != "first" {
			t.Fatalf("async snapshot string = %q, want first", entry.String)
		}
	case <-time.After(time.Second):
		t.Fatal("async replication did not deliver queued request")
	}
	final := waitForReplicationLastResult(t, replicator, func(result ReplicationResult) bool {
		return !result.Queued && !result.Skipped && len(result.Targets) == 1 && result.Targets[0].OK
	})
	if final.Queue == nil || !final.Queue.Enabled || final.Queue.Capacity != 2 || final.Queue.Enqueued != 1 || final.Queue.Attempts != 1 || final.Queue.Successes != 1 || final.Queue.Failures != 0 || final.Queue.Dropped != 0 {
		t.Fatalf("async queue stats = %#v, want one successful queued delivery", final.Queue)
	}
}

func TestExecutePublicCommandBatchAsyncOutboxQueuesOneGroupedTargetJob(t *testing.T) {
	release := make(chan struct{})
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		requests <- mustDecodeReplicationTestCommand(t, w, r)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	outboxPath := filepath.Join(t.TempDir(), "replication-outbox.json")
	outbox, err := OpenReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenReplicationOutbox() error = %v", err)
	}
	defer outbox.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 1,
		AsyncOutbox:    outbox,
	})
	defer replicator.Close()
	defer close(release)

	request := CacheCommandRequest{
		Command: "BATCH",
		Batch: []CacheCommandRequest{
			{Command: "SETSTR", Key: "session:outbox-batch", Value: "first"},
			{Command: "SETSTR", Key: "session:outbox-batch", Value: "second"},
			{Command: "GET", Key: "session:outbox-batch"},
		},
	}
	response, rejected := executeCacheCommand(context.Background(), trie, request, commandExecutionOptions{Replicator: replicator})
	if rejected || !response.OK || len(response.Responses) != 3 || response.Responses[2].Value != "second" {
		t.Fatalf("executeCacheCommand(batch) = %#v rejected=%v, want ok with final GET second", response, rejected)
	}

	jobs := outbox.jobs()
	if len(jobs) != 1 {
		t.Fatalf("outbox jobs len = %d, want one grouped batch job", len(jobs))
	}
	if jobs[0].result.Command != "BATCH" || jobs[0].result.Entries != 2 || !jobs[0].result.Queued {
		t.Fatalf("outbox job result = %#v, want queued BATCH with two entries", jobs[0].result)
	}
	if len(jobs[0].tasks) != 2 {
		t.Fatalf("outbox job task len = %d, want two tasks", len(jobs[0].tasks))
	}
	wantValues := []string{"first", "second"}
	for idx, task := range jobs[0].tasks {
		if task.target.ID != "node-b" {
			t.Fatalf("task %d target = %#v, want node-b", idx, task.target)
		}
		payload := task.payload
		if !isTypedReplicationSetPayload(payload, "session:outbox-batch") {
			t.Fatalf("task %d payload = %#v, want typed binary session:outbox-batch", idx, payload)
		}
		entry, err := decodeTypedReplicationSnapshot(payload.Key, payload.Command, payload.BinaryValue)
		if err != nil {
			t.Fatalf("task %d snapshot error = %v", idx, err)
		}
		if entry.String != wantValues[idx] {
			t.Fatalf("task %d snapshot string = %q, want %q", idx, entry.String, wantValues[idx])
		}
	}

	last := replicator.LastResult()
	if last.Command != "BATCH" || !last.Queued || last.Queue == nil || last.Queue.Enqueued != 1 {
		t.Fatalf("last replication result = %#v, want one queued BATCH job", last)
	}
}

func TestJournalBackedReplicationOutboxPersistsReferenceAndRestoresExactPayload(t *testing.T) {
	dir := t.TempDir()
	journalPath := filepath.Join(dir, "commands.journal")
	outboxPath := filepath.Join(dir, "replication-outbox")
	journal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	outbox, err := OpenLevelDBReplicationOutboxWithOptions(outboxPath, ReplicationOutboxOptions{
		Codec: ReplicationOutboxCodecBinary,
	})
	if err != nil {
		_ = journal.Close()
		t.Fatalf("OpenLevelDBReplicationOutboxWithOptions() error = %v", err)
	}

	blocked := make(chan struct{})
	var blockedOnce sync.Once
	ctx, cancel := context.WithCancel(context.Background())
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		blockedOnce.Do(func() { close(blocked) })
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, "http://node-b.example")
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Context:        ctx,
		Self:           "node-a",
		Topology:       topology,
		Election:       NewElectionStore(topology, ElectionOptions{}),
		Client:         client,
		AsyncQueueSize: 1,
		AsyncOutbox:    outbox,
		Journal:        journal,
		WireFormat:     CommandWireFormatJSON,
	})
	request := CacheCommandRequest{Command: "SETSTR", Key: "session:journal-ref", Value: strings.Repeat("payload-", 512)}
	response, rejected := executeCacheCommand(context.Background(), trie, request, commandExecutionOptions{
		Journal:    journal,
		Replicator: replicator,
	})
	if rejected || !response.OK {
		t.Fatalf("executeCacheCommand() = %#v rejected=%v, want success", response, rejected)
	}
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("replication worker did not dequeue journal-backed job")
	}

	jobID := journal.Sequence()
	raw, err := outbox.db.Get(replicationOutboxLevelDBJobKey(jobID), nil)
	if err != nil {
		t.Fatalf("read durable outbox reference: %v", err)
	}
	record, err := unmarshalReplicationOutboxJob(raw)
	if err != nil {
		t.Fatalf("decode durable outbox reference: %v", err)
	}
	if record.JournalSequence != jobID || len(record.Tasks) != 0 {
		t.Fatalf("durable outbox record = %#v, want compact journal reference %d without payload tasks", record, jobID)
	}

	cancel()
	replicator.Close()
	if err := outbox.db.Delete(replicationOutboxLevelDBJobKey(jobID), &opt.WriteOptions{Sync: true}); err != nil {
		t.Fatalf("simulate crash before outbox index persistence: %v", err)
	}
	if err := outbox.Close(); err != nil {
		t.Fatalf("close outbox: %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("close journal: %v", err)
	}

	reopenedJournal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal(reopen) error = %v", err)
	}
	defer reopenedJournal.Close()
	reopenedOutbox, err := OpenLevelDBReplicationOutboxWithOptions(outboxPath, ReplicationOutboxOptions{
		Codec: ReplicationOutboxCodecBinary,
	})
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutboxWithOptions(reopen) error = %v", err)
	}
	defer reopenedOutbox.Close()

	delivered := make(chan CacheCommandRequest, 1)
	restoredClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		defer request.Body.Close()
		var payload CacheCommandRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			return nil, err
		}
		delivered <- payload
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"ok":true,"message":"ok"}`)),
		}, nil
	})}
	restored := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       NewElectionStore(topology, ElectionOptions{}),
		Client:         restoredClient,
		AsyncQueueSize: 1,
		AsyncOutbox:    reopenedOutbox,
		Journal:        reopenedJournal,
		WireFormat:     CommandWireFormatJSON,
	})
	defer restored.Close()
	select {
	case payload := <-delivered:
		if payload.Command != "INTERNALSET" || payload.Key != request.Key {
			t.Fatalf("restored payload = %#v, want exact legacy-compatible set", payload)
		}
		var entry snapshotEntry
		if err := json.Unmarshal([]byte(payload.Value), &entry); err != nil {
			t.Fatalf("decode restored snapshot: %v", err)
		}
		if entry.String != request.Value {
			t.Fatalf("restored payload value length = %d, want %d", len(entry.String), len(request.Value))
		}
	case <-time.After(time.Second):
		t.Fatal("journal-backed outbox did not restore and deliver pending payload")
	}
	deadline := time.Now().Add(time.Second)
	for len(reopenedOutbox.jobs()) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if jobs := reopenedOutbox.jobs(); len(jobs) != 0 {
		t.Fatalf("completed journal-backed outbox jobs = %d, want zero", len(jobs))
	}
}

func TestHTTPReplicatorAsyncRetriesFailedDelivery(t *testing.T) {
	attempts := make(chan struct{}, 2)
	client := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			attempts <- struct{}{}
			if len(attempts) == 1 {
				return nil, errors.New("temporary failure")
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"ok":true,"message":"ok"}`)),
				Request:    request,
			}, nil
		}),
	}
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, "http://node-b.local")
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:               "node-a",
		Topology:           topology,
		Election:           election,
		Client:             client,
		AsyncQueueSize:     2,
		AsyncMaxAttempts:   2,
		AsyncRetryInterval: time.Millisecond,
	})
	defer replicator.Close()

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}
	response := trie.ExecuteCommand(write)
	result := replicator.ReplicateCommand(context.Background(), trie, write, response)
	if !result.Queued || result.Skipped {
		t.Fatalf("async enqueue result = %#v, want queued", result)
	}
	final := waitForReplicationLastResult(t, replicator, func(result ReplicationResult) bool {
		return !result.Queued && len(result.Targets) == 1 && result.Targets[0].OK
	})
	if got := len(attempts); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
	if final.Queue == nil || final.Queue.Enqueued != 1 || final.Queue.Attempts != 2 || final.Queue.Successes != 1 || final.Queue.Failures != 1 || final.Queue.Retried != 1 {
		t.Fatalf("retry queue stats = %#v, want one failed attempt, one retry, one success", final.Queue)
	}
	if final.Queue.LastRetryAt == nil || final.Queue.LastRetryAgeMillis < 0 || final.Queue.FailuresByTarget["node-b"] != 1 {
		t.Fatalf("retry visibility stats = %#v, want retry timestamp and node-b failure count", final.Queue)
	}
}

func TestHTTPReplicatorAsyncRecordsDeadLetterAfterExhaustedRetries(t *testing.T) {
	attempts := make(chan struct{}, 4)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case attempts <- struct{}{}:
		default:
		}
		http.Error(w, "still down", http.StatusBadGateway)
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:               "node-a",
		Topology:           topology,
		Election:           election,
		Client:             target.Client(),
		AsyncQueueSize:     1,
		AsyncMaxAttempts:   2,
		AsyncRetryInterval: time.Millisecond,
	})
	defer replicator.Close()

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:dead", Value: "secret"}
	response := trie.ExecuteCommand(write)
	if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
		t.Fatalf("enqueue result = %#v, want queued", result)
	}

	final := waitForReplicationLastResult(t, replicator, func(result ReplicationResult) bool {
		return result.DeadLetterCount == 1
	})
	if final.Queue == nil || final.Queue.Failures != 2 || final.Queue.Retried != 1 {
		t.Fatalf("queue stats = %#v, want two failures and one retry", final.Queue)
	}
	if len(final.DeadLetters) != 1 {
		t.Fatalf("dead letters = %#v, want one", final.DeadLetters)
	}
	deadLetter := final.DeadLetters[0]
	if deadLetter.ID != 1 || deadLetter.Command != "SETSTR" || deadLetter.Key != "session:dead" || deadLetter.Attempts != 2 || deadLetter.FailedAt == nil {
		t.Fatalf("dead letter = %#v, want SETSTR session:dead with attempts", deadLetter)
	}
	if len(deadLetter.Targets) != 1 || deadLetter.Targets[0].Node != "node-b" || deadLetter.Targets[0].OK {
		t.Fatalf("dead letter targets = %#v, want failed node-b target", deadLetter.Targets)
	}
	if strings.Contains(deadLetter.Reason, "secret") {
		t.Fatalf("dead letter reason leaked command value: %#v", deadLetter)
	}
	if got := len(attempts); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
	if final.Health == "ok" || final.HealthScore >= 100 {
		t.Fatalf("health = %s/%d/%q, want dead-letter failure to reduce health", final.Health, final.HealthScore, final.HealthReason)
	}
}

func TestHTTPReplicatorAsyncDeadLettersAreBounded(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "still down", http.StatusBadGateway)
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:                 "node-a",
		Topology:             topology,
		Election:             election,
		Client:               target.Client(),
		AsyncQueueSize:       2,
		AsyncMaxAttempts:     1,
		AsyncDeadLetterLimit: 1,
	})
	defer replicator.Close()

	for _, key := range []string{"session:first", "session:second"} {
		write := CacheCommandRequest{Command: "SETSTR", Key: key, Value: "value"}
		response := trie.ExecuteCommand(write)
		if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
			t.Fatalf("enqueue %s result = %#v, want queued", key, result)
		}
	}

	final := waitForReplicationLastResult(t, replicator, func(result ReplicationResult) bool {
		return result.DeadLetterCount == 1 && len(result.DeadLetters) == 1 && result.DeadLetters[0].Key == "session:second"
	})
	if final.DeadLetters[0].ID != 2 {
		t.Fatalf("dead letters = %#v, want only newest id 2 retained", final.DeadLetters)
	}
}

func TestHTTPReplicatorAsyncOutboxReplaysPendingJobAfterRestart(t *testing.T) {
	var mode atomic.Int32
	entered := make(chan struct{}, 1)
	delivered := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if mode.Load() == 0 {
			select {
			case entered <- struct{}{}:
			default:
			}
			http.Error(w, "temporarily down", http.StatusBadGateway)
			return
		}
		request := mustDecodeReplicationTestCommand(t, w, r)
		delivered <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	outboxPath := filepath.Join(t.TempDir(), "replication-outbox.json")
	outbox, err := OpenReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenReplicationOutbox() error = %v", err)
	}
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:               "node-a",
		Topology:           topology,
		Election:           election,
		Client:             target.Client(),
		AsyncQueueSize:     1,
		AsyncMaxAttempts:   2,
		AsyncRetryInterval: time.Hour,
		AsyncOutbox:        outbox,
	})

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:outbox", Value: "value"}
	response := trie.ExecuteCommand(write)
	if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
		t.Fatalf("enqueue result = %#v, want queued", result)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first replication attempt did not start")
	}
	replicator.Close()

	reopened, err := OpenReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenReplicationOutbox(reopen) error = %v", err)
	}
	if jobs := reopened.jobs(); len(jobs) != 1 || jobs[0].result.Key != "session:outbox" {
		t.Fatalf("persisted jobs = %#v, want queued session:outbox job", jobs)
	}
	mode.Store(1)
	replayed := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 1,
		AsyncOutbox:    reopened,
	})
	defer replayed.Close()

	select {
	case request := <-delivered:
		if !isTypedReplicationSetPayload(request, "session:outbox") {
			t.Fatalf("replayed request = %#v, want persisted typed binary snapshot", request)
		}
	case <-time.After(time.Second):
		t.Fatal("persisted replication job was not replayed")
	}
	waitUntil(t, time.Second, func() bool {
		return len(reopened.jobs()) == 0
	})
}

func TestHTTPReplicatorAsyncOutboxPersistsDeadLetters(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "still down", http.StatusBadGateway)
	}))
	defer target.Close()

	outboxPath := filepath.Join(t.TempDir(), "replication-outbox.json")
	outbox, err := OpenReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenReplicationOutbox() error = %v", err)
	}
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:             "node-a",
		Topology:         topology,
		Election:         election,
		Client:           target.Client(),
		AsyncQueueSize:   1,
		AsyncMaxAttempts: 1,
		AsyncOutbox:      outbox,
	})

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:dead-outbox", Value: "value"}
	response := trie.ExecuteCommand(write)
	if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
		t.Fatalf("enqueue result = %#v, want queued", result)
	}
	final := waitForReplicationLastResult(t, replicator, func(result ReplicationResult) bool {
		return result.DeadLetterCount == 1
	})
	if len(final.DeadLetters) != 1 || final.DeadLetters[0].Key != "session:dead-outbox" {
		t.Fatalf("dead letters = %#v, want persisted key", final.DeadLetters)
	}
	replicator.Close()

	reopened, err := OpenReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenReplicationOutbox(reopen) error = %v", err)
	}
	replayed := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 1,
		AsyncOutbox:    reopened,
	})
	defer replayed.Close()
	last := replayed.LastResult()
	if last.DeadLetterCount != 1 || len(last.DeadLetters) != 1 || last.DeadLetters[0].Key != "session:dead-outbox" {
		t.Fatalf("restored dead letters = %#v count=%d, want retained session:dead-outbox", last.DeadLetters, last.DeadLetterCount)
	}
	if jobs := reopened.jobs(); len(jobs) != 0 {
		t.Fatalf("persisted jobs after dead-letter = %#v, want none", jobs)
	}
}

func TestReplicationOutboxJobPreservesTypedBinaryPayload(t *testing.T) {
	payload := CacheCommandRequest{
		Command:     replicationSetBinaryCommand,
		Key:         "session:binary-outbox",
		BinaryValue: []byte{0x48, 0x43, 0x42, 0x31, 0x01, 0x02, 0x03},
	}
	record := newReplicationOutboxJob(replicationJob{
		id: 1,
		tasks: []replicationTask{{
			target:  TopologyNode{ID: "node-b", Address: "http://node-b"},
			payload: payload,
		}},
	})

	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var decoded replicationOutboxJob
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	restored := decoded.replicationJob()
	if len(restored.tasks) != 1 || !bytes.Equal(restored.tasks[0].payload.BinaryValue, payload.BinaryValue) {
		t.Fatalf("restored payload = %#v, want binary value %x", restored.tasks, payload.BinaryValue)
	}
}

func TestLevelDBReplicationOutboxPersistsJobsAndDeadLetters(t *testing.T) {
	outboxPath := filepath.Join(t.TempDir(), "replication-outbox.leveldb")
	outbox, err := OpenLevelDBReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox() error = %v", err)
	}
	job := replicationJob{
		id: 1,
		result: ReplicationResult{
			Command: "SETSTR",
			Key:     "session:leveldb",
		},
		tasks: []replicationTask{{
			target:  TopologyNode{ID: "node-b", Address: "http://node-b"},
			payload: CacheCommandRequest{Command: "INTERNALSET", Key: "session:leveldb", Value: `{"type":"string","string":"value"}`},
		}},
		enqueuedAt: time.Now().UTC(),
	}
	if err := outbox.putJob(job); err != nil {
		t.Fatalf("putJob() error = %v", err)
	}
	if err := outbox.setDeadLetters(7, []ReplicationDeadLetter{{ID: 7, Key: "session:dead", Attempts: 2}}); err != nil {
		t.Fatalf("setDeadLetters() error = %v", err)
	}
	if err := outbox.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := OpenLevelDBReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox(reopen) error = %v", err)
	}
	defer reopened.Close()
	jobs := reopened.jobs()
	if len(jobs) != 1 || jobs[0].id != 1 || jobs[0].result.Key != "session:leveldb" || len(jobs[0].tasks) != 1 || jobs[0].tasks[0].payload.Command != "INTERNALSET" {
		t.Fatalf("jobs() = %#v, want persisted leveldb job", jobs)
	}
	deadSeq, deadLetters := reopened.deadLetters()
	if deadSeq != 7 || len(deadLetters) != 1 || deadLetters[0].Key != "session:dead" {
		t.Fatalf("deadLetters() = %d/%#v, want persisted dead letter", deadSeq, deadLetters)
	}
	if err := reopened.deleteJob(1); err != nil {
		t.Fatalf("deleteJob() error = %v", err)
	}
	if jobs := reopened.jobs(); len(jobs) != 0 {
		t.Fatalf("jobs() after delete = %#v, want none", jobs)
	}
}

func TestLevelDBReplicationOutboxBinaryDefaultAndJSONCompatibility(t *testing.T) {
	job := replicationJob{
		id:     1,
		result: ReplicationResult{Command: "SETBYTES", Key: "session:binary"},
		tasks: []replicationTask{{
			target: TopologyNode{ID: "node-b", Address: "http://node-b", GRPCAddress: "node-b:9090"},
			payload: CacheCommandRequest{
				Command:     replicationSetCompactCommand,
				Key:         "session:binary",
				BinaryValue: bytes.Repeat([]byte{0xab}, 4096),
			},
		}},
		enqueuedAt: time.Unix(1700000000, 123).UTC(),
	}
	path := filepath.Join(t.TempDir(), "replication-outbox.leveldb")
	outbox, err := OpenLevelDBReplicationOutbox(path)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox() error = %v", err)
	}
	if err := outbox.putJob(job); err != nil {
		t.Fatalf("putJob() error = %v", err)
	}
	raw, err := outbox.db.Get(replicationOutboxLevelDBJobKey(job.id), nil)
	if err != nil {
		t.Fatalf("read raw binary job error = %v", err)
	}
	if !bytes.HasPrefix(raw, replicationOutboxBinaryJobMagic) {
		prefix := raw
		if len(prefix) > len(replicationOutboxBinaryJobMagic) {
			prefix = prefix[:len(replicationOutboxBinaryJobMagic)]
		}
		t.Fatalf("default outbox job prefix = %x, want binary magic %x", prefix, replicationOutboxBinaryJobMagic)
	}
	legacyJSON, err := json.Marshal(newReplicationOutboxJob(job))
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if len(raw) >= len(legacyJSON) {
		t.Fatalf("binary job bytes = %d, want smaller than JSON bytes %d", len(raw), len(legacyJSON))
	}

	legacy := job
	legacy.id = 2
	legacy.result.Key = "session:legacy-json"
	legacyJSON, err = json.Marshal(newReplicationOutboxJob(legacy))
	if err != nil {
		t.Fatalf("json.Marshal(legacy) error = %v", err)
	}
	if err := outbox.db.Put(replicationOutboxLevelDBJobKey(legacy.id), legacyJSON, &opt.WriteOptions{Sync: true}); err != nil {
		t.Fatalf("put legacy JSON job error = %v", err)
	}
	jobs := outbox.jobs()
	if len(jobs) != 2 || jobs[0].result.Key != "session:binary" || jobs[1].result.Key != "session:legacy-json" {
		t.Fatalf("mixed binary/JSON jobs = %#v, want both records in order", jobs)
	}
	if len(jobs[0].tasks) != 1 || !bytes.Equal(jobs[0].tasks[0].payload.BinaryValue, job.tasks[0].payload.BinaryValue) {
		t.Fatalf("binary job payload = %#v, want %d preserved bytes", jobs[0].tasks, len(job.tasks[0].payload.BinaryValue))
	}
	if err := outbox.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	jsonPath := filepath.Join(t.TempDir(), "replication-outbox-json.leveldb")
	jsonOutbox, err := OpenLevelDBReplicationOutboxWithOptions(jsonPath, ReplicationOutboxOptions{Codec: ReplicationOutboxCodecJSON})
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutboxWithOptions(JSON) error = %v", err)
	}
	defer jsonOutbox.Close()
	if err := jsonOutbox.putJob(job); err != nil {
		t.Fatalf("putJob(JSON) error = %v", err)
	}
	rawJSON, err := jsonOutbox.db.Get(replicationOutboxLevelDBJobKey(job.id), nil)
	if err != nil {
		t.Fatalf("read raw JSON job error = %v", err)
	}
	if len(rawJSON) == 0 || rawJSON[0] != '{' {
		t.Fatalf("configured JSON outbox job prefix = %x, want JSON object", rawJSON)
	}
}

func TestLevelDBReplicationOutboxGroupsConcurrentDurableWrites(t *testing.T) {
	const jobs = 32
	path := filepath.Join(t.TempDir(), "replication-outbox.leveldb")
	outbox, err := OpenLevelDBReplicationOutboxWithOptions(path, ReplicationOutboxOptions{
		BatchWindow: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutboxWithOptions() error = %v", err)
	}
	defer outbox.Close()

	start := make(chan struct{})
	errs := make(chan error, jobs)
	var writers sync.WaitGroup
	writers.Add(jobs)
	for id := 1; id <= jobs; id++ {
		go func(id int) {
			defer writers.Done()
			<-start
			errs <- outbox.putJob(replicationJob{
				id:         uint64(id),
				result:     ReplicationResult{Command: "SETSTR", Key: fmt.Sprintf("session:%02d", id)},
				tasks:      []replicationTask{{target: TopologyNode{ID: "node-b"}, payload: CacheCommandRequest{Command: "INTERNALSET", Key: fmt.Sprintf("session:%02d", id), Value: "value"}}},
				enqueuedAt: time.Now().UTC(),
			})
		}(id)
	}
	close(start)
	writers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent putJob() error = %v", err)
		}
	}
	if got := outbox.levelDBSyncWriteCount(); got != 1 {
		t.Fatalf("LevelDB synchronous writes = %d, want one grouped commit", got)
	}
	if got := len(outbox.jobs()); got != jobs {
		t.Fatalf("persisted grouped jobs = %d, want %d", got, jobs)
	}
}

func TestLevelDBReplicationOutboxCompletesDeadLetterAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replication-outbox.leveldb")
	outbox, err := OpenLevelDBReplicationOutboxWithOptions(path, ReplicationOutboxOptions{})
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutboxWithOptions() error = %v", err)
	}
	job := replicationJob{id: 1, result: ReplicationResult{Command: "SETSTR", Key: "session:failed"}}
	if err := outbox.putJob(job); err != nil {
		t.Fatalf("putJob() error = %v", err)
	}
	writesBefore := outbox.levelDBSyncWriteCount()
	deadLetters := []ReplicationDeadLetter{{ID: 7, Command: "SETSTR", Key: "session:failed", Attempts: 3}}
	if err := outbox.completeJob(1, 7, deadLetters, true); err != nil {
		t.Fatalf("completeJob() error = %v", err)
	}
	if got := outbox.levelDBSyncWriteCount() - writesBefore; got != 1 {
		t.Fatalf("completion synchronous writes = %d, want one atomic commit", got)
	}
	if jobs := outbox.jobs(); len(jobs) != 0 {
		t.Fatalf("jobs after completion = %#v, want none", jobs)
	}
	deadSeq, restored := outbox.deadLetters()
	if deadSeq != 7 || len(restored) != 1 || restored[0].Key != "session:failed" {
		t.Fatalf("dead letters after completion = %d/%#v, want atomic persisted failure", deadSeq, restored)
	}
	if err := outbox.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	reopened, err := OpenLevelDBReplicationOutbox(path)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox(reopen) error = %v", err)
	}
	defer reopened.Close()
	if jobs := reopened.jobs(); len(jobs) != 0 {
		t.Fatalf("reopened jobs = %#v, want completed job absent", jobs)
	}
	deadSeq, restored = reopened.deadLetters()
	if deadSeq != 7 || len(restored) != 1 || restored[0].Key != "session:failed" {
		t.Fatalf("reopened dead letters = %d/%#v, want atomic persisted failure", deadSeq, restored)
	}
}

func TestLevelDBReplicationOutboxCloseWaitsForGroupedCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replication-outbox.leveldb")
	outbox, err := OpenLevelDBReplicationOutboxWithOptions(path, ReplicationOutboxOptions{BatchWindow: 20 * time.Millisecond})
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutboxWithOptions() error = %v", err)
	}
	job := replicationJob{id: 1, result: ReplicationResult{Command: "SETSTR", Key: "session:close"}}
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- outbox.putJob(job)
	}()
	waitUntil(t, time.Second, func() bool {
		outbox.mu.Lock()
		defer outbox.mu.Unlock()
		return outbox.writeLeader
	})
	if err := outbox.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("grouped putJob() error = %v", err)
	}
	if err := outbox.putJob(replicationJob{id: 2}); !errors.Is(err, errReplicationOutboxClosed) {
		t.Fatalf("putJob() after close error = %v, want %v", err, errReplicationOutboxClosed)
	}

	reopened, err := OpenLevelDBReplicationOutbox(path)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox(reopen) error = %v", err)
	}
	defer reopened.Close()
	jobs := reopened.jobs()
	if len(jobs) != 1 || jobs[0].id != 1 || jobs[0].result.Key != "session:close" {
		t.Fatalf("reopened jobs = %#v, want committed job 1", jobs)
	}
}

func TestLevelDBReplicationOutboxSkipsCorruptPartialJobRecords(t *testing.T) {
	outboxPath := filepath.Join(t.TempDir(), "replication-outbox.leveldb")
	outbox, err := OpenLevelDBReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox() error = %v", err)
	}
	valid := replicationJob{
		id: 2,
		result: ReplicationResult{
			Command: "SETSTR",
			Key:     "session:valid",
		},
		tasks: []replicationTask{{
			target:  TopologyNode{ID: "node-b", Address: "http://node-b"},
			payload: CacheCommandRequest{Command: "INTERNALSET", Key: "session:valid", Value: `{"type":"string","string":"value"}`},
		}},
		enqueuedAt: time.Now().UTC(),
	}
	if err := outbox.putJob(valid); err != nil {
		t.Fatalf("putJob(valid) error = %v", err)
	}
	if err := outbox.db.Put(replicationOutboxLevelDBJobKey(1), []byte(`{"id":1,"result":`), nil); err != nil {
		t.Fatalf("put corrupt job error = %v", err)
	}
	if err := outbox.db.Put(replicationOutboxLevelDBJobKey(3), append(append([]byte(nil), replicationOutboxBinaryJobMagic...), 0x80), nil); err != nil {
		t.Fatalf("put corrupt binary job error = %v", err)
	}
	if jobs := outbox.jobs(); len(jobs) != 1 || jobs[0].id != 2 || jobs[0].result.Key != "session:valid" {
		t.Fatalf("jobs() with corrupt record = %#v, want only valid job", jobs)
	}
	page, cursor, hasMore := outbox.jobPage(0, 1)
	if len(page) != 1 || page[0].id != 2 || cursor != 2 || !hasMore {
		t.Fatalf("jobPage(first) = %#v cursor=%d more=%v, want valid job 2 and corrupt suffix", page, cursor, hasMore)
	}
	page, cursor, hasMore = outbox.jobPage(cursor, 1)
	if len(page) != 0 || cursor != 3 || hasMore {
		t.Fatalf("jobPage(corrupt suffix) = %#v cursor=%d more=%v, want cursor 3 at end", page, cursor, hasMore)
	}
	if err := outbox.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := OpenLevelDBReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox(reopen) error = %v", err)
	}
	defer reopened.Close()
	if jobs := reopened.jobs(); len(jobs) != 1 || jobs[0].id != 2 || jobs[0].result.Key != "session:valid" {
		t.Fatalf("reopened jobs() with corrupt record = %#v, want only valid job", jobs)
	}
}

func TestHTTPReplicatorLevelDBOutboxReplaysPendingJobAfterRestart(t *testing.T) {
	var mode atomic.Int32
	entered := make(chan struct{}, 1)
	delivered := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if mode.Load() == 0 {
			select {
			case entered <- struct{}{}:
			default:
			}
			http.Error(w, "temporarily down", http.StatusBadGateway)
			return
		}
		request := mustDecodeReplicationTestCommand(t, w, r)
		delivered <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	outboxPath := filepath.Join(t.TempDir(), "replication-outbox.leveldb")
	outbox, err := OpenLevelDBReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox() error = %v", err)
	}
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:               "node-a",
		Topology:           topology,
		Election:           election,
		Client:             target.Client(),
		AsyncQueueSize:     1,
		AsyncMaxAttempts:   2,
		AsyncRetryInterval: time.Hour,
		AsyncOutbox:        outbox,
	})

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:leveldb-outbox", Value: "value"}
	response := trie.ExecuteCommand(write)
	if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
		t.Fatalf("enqueue result = %#v, want queued", result)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first replication attempt did not start")
	}
	replicator.Close()
	if err := outbox.Close(); err != nil {
		t.Fatalf("Close(outbox) error = %v", err)
	}

	reopened, err := OpenLevelDBReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox(reopen) error = %v", err)
	}
	if jobs := reopened.jobs(); len(jobs) != 1 || jobs[0].result.Key != "session:leveldb-outbox" {
		t.Fatalf("persisted jobs = %#v, want queued session:leveldb-outbox job", jobs)
	}
	mode.Store(1)
	replayed := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 1,
		AsyncOutbox:    reopened,
	})
	defer reopened.Close()
	defer replayed.Close()

	select {
	case request := <-delivered:
		if !isTypedReplicationSetPayload(request, "session:leveldb-outbox") {
			t.Fatalf("replayed request = %#v, want persisted typed binary snapshot", request)
		}
	case <-time.After(time.Second):
		t.Fatal("persisted replication job was not replayed")
	}
	waitUntil(t, time.Second, func() bool {
		return len(reopened.jobs()) == 0
	})
}

func TestHTTPReplicatorLevelDBOutboxReplaysPendingJobsInOrderAfterRetryInterrupted(t *testing.T) {
	var mode atomic.Int32
	attempted := make(chan string, 2)
	delivered := make(chan string, 2)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		if mode.Load() == 0 {
			attempted <- request.Key
			http.Error(w, "temporarily down", http.StatusBadGateway)
			return
		}
		delivered <- request.Key
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	outboxPath := filepath.Join(t.TempDir(), "replication-outbox.leveldb")
	outbox, err := OpenLevelDBReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox() error = %v", err)
	}
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:               "node-a",
		Topology:           topology,
		Election:           election,
		Client:             target.Client(),
		AsyncQueueSize:     2,
		AsyncMaxAttempts:   2,
		AsyncRetryInterval: time.Hour,
		AsyncOutbox:        outbox,
	})

	for _, key := range []string{"session:first", "session:second"} {
		write := CacheCommandRequest{Command: "SETSTR", Key: key, Value: "value"}
		response := trie.ExecuteCommand(write)
		if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
			t.Fatalf("enqueue %s result = %#v, want queued", key, result)
		}
	}
	select {
	case key := <-attempted:
		if key != "session:first" {
			t.Fatalf("first failed attempt key = %q, want session:first", key)
		}
	case <-time.After(time.Second):
		t.Fatal("first replication attempt did not start")
	}
	replicator.Close()
	if err := outbox.Close(); err != nil {
		t.Fatalf("Close(outbox) error = %v", err)
	}

	reopened, err := OpenLevelDBReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox(reopen) error = %v", err)
	}
	if jobs := reopened.jobs(); len(jobs) != 2 || jobs[0].result.Key != "session:first" || jobs[1].result.Key != "session:second" {
		t.Fatalf("persisted jobs = %#v, want first then second", jobs)
	}
	mode.Store(1)
	replayed := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 2,
		AsyncOutbox:    reopened,
	})
	defer reopened.Close()
	defer replayed.Close()

	for _, want := range []string{"session:first", "session:second"} {
		select {
		case got := <-delivered:
			if got != want {
				t.Fatalf("delivered key = %q, want %q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for replayed %s", want)
		}
	}
	waitUntil(t, time.Second, func() bool {
		return len(reopened.jobs()) == 0
	})
}

func TestHTTPReplicatorCircuitBreakerSkipsOnlyOpenTarget(t *testing.T) {
	failedRequests := make(chan struct{}, 4)
	healthyRequests := make(chan struct{}, 4)
	failingTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case failedRequests <- struct{}{}:
		default:
		}
		http.Error(w, "still down", http.StatusBadGateway)
	}))
	defer failingTarget.Close()
	healthyTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case healthyRequests <- struct{}{}:
		default:
		}
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer healthyTarget.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopologyWithReplicas(t, failingTarget.URL, healthyTarget.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:                   "node-a",
		Topology:               topology,
		Election:               election,
		Client:                 failingTarget.Client(),
		CircuitBreakerFailures: 1,
		CircuitBreakerCooldown: time.Hour,
	})

	firstWrite := CacheCommandRequest{Command: "SETSTR", Key: "session:breaker", Value: "first"}
	first := replicator.ReplicateCommand(context.Background(), trie, firstWrite, trie.ExecuteCommand(firstWrite))
	if len(first.Targets) != 2 || first.Targets[0].Node != "node-b" || !first.Targets[0].CircuitOpen || first.Targets[1].Node != "node-c" || !first.Targets[1].OK {
		t.Fatalf("first replication targets = %#v, want node-b open and node-c ok", first.Targets)
	}
	if len(failedRequests) != 1 || len(healthyRequests) != 1 {
		t.Fatalf("request counts after first write = failed:%d healthy:%d, want 1/1", len(failedRequests), len(healthyRequests))
	}

	secondWrite := CacheCommandRequest{Command: "SETSTR", Key: "session:breaker", Value: "second"}
	second := replicator.ReplicateCommand(context.Background(), trie, secondWrite, trie.ExecuteCommand(secondWrite))
	if len(second.Targets) != 2 || second.Targets[0].Node != "node-b" || !second.Targets[0].CircuitOpen || second.Targets[0].CircuitState != replicationCircuitOpen || second.Targets[1].Node != "node-c" || !second.Targets[1].OK {
		t.Fatalf("second replication targets = %#v, want node-b skipped open and node-c ok", second.Targets)
	}
	if len(failedRequests) != 1 || len(healthyRequests) != 2 {
		t.Fatalf("request counts after open breaker = failed:%d healthy:%d, want 1/2", len(failedRequests), len(healthyRequests))
	}
	last := replicator.LastResult()
	if len(last.CircuitBreakers) != 1 || last.CircuitBreakers[0].Node != "node-b" || last.CircuitBreakers[0].State != replicationCircuitOpen {
		t.Fatalf("circuit breakers = %#v, want only node-b open", last.CircuitBreakers)
	}
	if last.Health == "ok" || !strings.Contains(last.HealthReason, "circuit breaker") {
		t.Fatalf("health = %s/%d/%q, want breaker-degraded health", last.Health, last.HealthScore, last.HealthReason)
	}
}

func TestHTTPReplicatorCircuitBreakerHalfOpenRecovers(t *testing.T) {
	var attempts atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt == 1 {
			http.Error(w, "try later", http.StatusBadGateway)
			return
		}
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:                   "node-a",
		Topology:               topology,
		Election:               election,
		Client:                 target.Client(),
		CircuitBreakerFailures: 1,
		CircuitBreakerCooldown: 10 * time.Millisecond,
	})

	firstWrite := CacheCommandRequest{Command: "SETSTR", Key: "session:recover", Value: "first"}
	first := replicator.ReplicateCommand(context.Background(), trie, firstWrite, trie.ExecuteCommand(firstWrite))
	if len(first.Targets) != 1 || !first.Targets[0].CircuitOpen {
		t.Fatalf("first targets = %#v, want open circuit after failure", first.Targets)
	}
	secondWrite := CacheCommandRequest{Command: "SETSTR", Key: "session:recover", Value: "second"}
	second := replicator.ReplicateCommand(context.Background(), trie, secondWrite, trie.ExecuteCommand(secondWrite))
	if len(second.Targets) != 1 || !second.Targets[0].CircuitOpen || attempts.Load() != 1 {
		t.Fatalf("second targets = %#v attempts=%d, want skipped while open", second.Targets, attempts.Load())
	}

	time.Sleep(20 * time.Millisecond)
	thirdWrite := CacheCommandRequest{Command: "SETSTR", Key: "session:recover", Value: "third"}
	third := replicator.ReplicateCommand(context.Background(), trie, thirdWrite, trie.ExecuteCommand(thirdWrite))
	if len(third.Targets) != 1 || !third.Targets[0].OK || third.Targets[0].CircuitState != replicationCircuitClosed {
		t.Fatalf("third targets = %#v, want half-open probe success and closed circuit", third.Targets)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want one failure plus one half-open probe", attempts.Load())
	}
	if breakers := replicator.LastResult().CircuitBreakers; len(breakers) != 0 {
		t.Fatalf("circuit breakers = %#v, want cleared after successful probe", breakers)
	}
}

func TestHTTPReplicatorAsyncReportsFullQueue(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()
	defer close(release)

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 1,
	})
	defer replicator.Close()

	first := CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "one"}
	firstResponse := trie.ExecuteCommand(first)
	if result := replicator.ReplicateCommand(context.Background(), trie, first, firstResponse); !result.Queued || result.Skipped {
		t.Fatalf("first enqueue result = %#v, want queued", result)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start first queued delivery")
	}
	second := CacheCommandRequest{Command: "SETSTR", Key: "session:2", Value: "two"}
	secondResponse := trie.ExecuteCommand(second)
	if result := replicator.ReplicateCommand(context.Background(), trie, second, secondResponse); !result.Queued || result.Skipped {
		t.Fatalf("second enqueue result = %#v, want queued while worker is blocked", result)
	}
	third := CacheCommandRequest{Command: "SETSTR", Key: "session:3", Value: "three"}
	thirdResponse := trie.ExecuteCommand(third)
	result := replicator.ReplicateCommand(context.Background(), trie, third, thirdResponse)
	if !result.Skipped || result.Reason != "replication queue is full" || result.Queued {
		t.Fatalf("third enqueue result = %#v, want full queue skip", result)
	}
	last := replicator.LastResult()
	if last.Queue == nil || last.Queue.Enqueued != 2 || last.Queue.Dropped != 1 || last.Queue.Depth != 1 || last.Queue.Capacity != 1 {
		t.Fatalf("full queue stats = %#v, want two enqueued, one dropped, one pending", last.Queue)
	}
	if last.Queue.OldestQueuedAt == nil || last.Queue.OldestQueuedKey != "session:2" || last.Queue.OldestQueuedAgeMillis < 0 {
		t.Fatalf("oldest queued stats = %#v, want pending session:2 with age", last.Queue)
	}
	if last.Queue.InFlightStartedAt == nil || last.Queue.InFlightKey != "session:1" || last.Queue.InFlightAgeMillis < 0 {
		t.Fatalf("in-flight stats = %#v, want blocked session:1 with age", last.Queue)
	}
	if last.Queue.DroppedByTarget["node-b"] != 1 {
		t.Fatalf("dropped target stats = %#v, want one dropped node-b target", last.Queue)
	}
}

func TestHTTPReplicatorAsyncCloseIsIdempotentAndRejectsEnqueue(t *testing.T) {
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, "http://node-b.local")
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		AsyncQueueSize: 1,
	})
	replicator.Close()
	replicator.Close()

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}
	response := trie.ExecuteCommand(write)
	result := replicator.ReplicateCommand(context.Background(), trie, write, response)
	if !result.Skipped || result.Reason != "replication queue is closed" || result.Queued {
		t.Fatalf("post-close replicate result = %#v, want closed queue skip", result)
	}
	last := replicator.LastResult()
	if last.Queue == nil || !last.Queue.Closed || last.Queue.Dropped != 1 {
		t.Fatalf("closed queue stats = %#v, want closed with one dropped enqueue", last.Queue)
	}
}

func TestHTTPReplicatorAsyncContextCancelStopsQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Context:        ctx,
		AsyncQueueSize: 1,
	})
	cancel()
	waitUntil(t, time.Second, func() bool {
		last := replicator.LastResult()
		return last.Queue != nil && last.Queue.Closed
	})

	trie := newTestTrie(t)
	write := CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}
	response := trie.ExecuteCommand(write)
	result := replicator.ReplicateCommand(context.Background(), trie, write, response)
	if !result.Skipped || result.Reason != "replication queue is closed" || result.Queued {
		t.Fatalf("post-cancel replicate result = %#v, want closed queue skip", result)
	}
	last := replicator.LastResult()
	if last.Queue == nil || !last.Queue.Closed || last.Queue.Dropped != 1 {
		t.Fatalf("closed queue stats after context cancel = %#v, want closed with one dropped enqueue", last.Queue)
	}

	stopped := make(chan struct{})
	go func() {
		replicator.Close()
		replicator.Close()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("async replicator Close did not return after context cancel")
	}
}

func TestHTTPReplicatorAsyncCloseCancelsInFlightDelivery(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()
	defer close(release)

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 1,
		Timeout:        time.Minute,
	})

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}
	response := trie.ExecuteCommand(write)
	if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
		t.Fatalf("enqueue result = %#v, want queued", result)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("async replication target was not called")
	}

	stopped := make(chan struct{})
	go func() {
		replicator.Close()
		replicator.Close()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("async replicator Close did not cancel in-flight delivery")
	}
	last := replicator.LastResult()
	if last.Queue == nil || !last.Queue.Closed {
		t.Fatalf("queue stats after canceled close = %#v, want closed queue", last.Queue)
	}
}

func TestHTTPReplicatorAsyncCloseCancelsRetryWait(t *testing.T) {
	requests := make(chan struct{}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requests <- struct{}{}:
		default:
		}
		http.Error(w, "retry me", http.StatusBadGateway)
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:               "node-a",
		Topology:           topology,
		Election:           election,
		Client:             target.Client(),
		AsyncQueueSize:     1,
		AsyncMaxAttempts:   2,
		AsyncRetryInterval: time.Hour,
	})

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}
	response := trie.ExecuteCommand(write)
	if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
		t.Fatalf("enqueue result = %#v, want queued", result)
	}
	select {
	case <-requests:
	case <-time.After(time.Second):
		t.Fatal("async replication target was not called")
	}
	waitUntil(t, time.Second, func() bool {
		last := replicator.LastResult()
		return last.Queue != nil && last.Queue.Retried == 1
	})

	stopped := make(chan struct{})
	go func() {
		replicator.Close()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("async replicator Close did not cancel retry wait")
	}
	last := replicator.LastResult()
	if last.Queue == nil || !last.Queue.Closed || last.Queue.Retried != 1 || last.Queue.Attempts != 1 {
		t.Fatalf("queue stats after canceled retry wait = %#v, want one retry wait canceled", last.Queue)
	}
}

func TestHTTPReplicatorUsesTopologyWhenElectionUnconfigured(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	response := trie.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"})
	topology := replicationTestTopology(t, target.URL)
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Client:   target.Client(),
	})

	result := replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("topology-only replication result = %#v, want one ok target", result)
	}
	select {
	case request := <-requests:
		if !isTypedReplicationSetPayload(request, "session:1") {
			t.Fatalf("topology-only replicated request = %#v, want typed binary snapshot", request)
		}
	default:
		t.Fatal("topology-only replication did not reach remote target")
	}

	follower := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-b",
		Topology: topology,
		Client:   target.Client(),
	})
	result = follower.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}, response)
	if !result.Skipped || result.Reason != "local node is not elected leader" {
		t.Fatalf("topology-only follower result = %#v, want skipped not leader", result)
	}
}

func TestHTTPReplicatorSyncAllReplicatesLeaderOwnedEntries(t *testing.T) {
	requests := make(chan CacheCommandRequest, 2)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/commands" {
			t.Fatalf("path = %s, want /api/commands", r.URL.Path)
		}
		request := mustDecodeReplicationTestCommand(t, w, r)
		if rejectReplicationDigestTestCommand(t, w, r, request) {
			return
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	trie.UpsertString("session:1", "value-1")
	trie.UpsertString("session:2", "value-2")
	trie.UpsertString("other:1", "ignored")
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	result := replicator.SyncAll(context.Background(), trie, "session:")
	if result.Skipped || result.Command != "SYNC" || result.Key != "session:" || result.Entries != 2 || len(result.Targets) != 1 {
		t.Fatalf("sync result = %#v, want one batched target for two synced entries", result)
	}
	assertReplicationResultTiming(t, result)
	for _, target := range result.Targets {
		if !target.OK || target.Node != "node-b" {
			t.Fatalf("sync target = %#v, want ok node-b target", target)
		}
	}
	request := <-requests
	payloads := mustDecodeReplicationBatchValues(t, request)
	if len(payloads) != 2 {
		t.Fatalf("sync batch payloads len = %d, want 2: %#v", len(payloads), payloads)
	}
	targetKeys := map[string]bool{}
	for _, payload := range payloads {
		if !isTypedReplicationSetPayload(payload, payload.Key) {
			t.Fatalf("sync batch payload = %#v, want typed binary snapshot", payload)
		}
		targetKeys[payload.Key] = true
	}
	if !targetKeys["session:1"] || !targetKeys["session:2"] {
		t.Fatalf("sync batch keys = %#v, want session keys", targetKeys)
	}
	select {
	case request := <-requests:
		t.Fatalf("unexpected sync request = %#v", request)
	default:
	}
	if got := replicator.LastResult(); !reflect.DeepEqual(got, result) {
		t.Fatalf("LastResult() = %#v, want sync result %#v", got, result)
	}
}

func TestHTTPReplicatorSyncAllDigestTransfersOnlyDifferencesAndDeletesStaleKeys(t *testing.T) {
	source := newTestTrie(t)
	targetTrie := newTestTrie(t)
	for key, value := range map[string]string{
		"session:changed": "new",
		"session:missing": "source-only",
		"session:same":    "equal",
	} {
		source.UpsertString(key, value)
	}
	for key, value := range map[string]string{
		"other:preserved": "unrelated",
		"session:changed": "old",
		"session:same":    "equal",
		"session:stale":   "target-only",
	} {
		targetTrie.UpsertString(key, value)
	}

	requests := make(chan CacheCommandRequest, 4)
	var topology *TopologyStore
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		response, rejected := executeCacheCommand(r.Context(), targetTrie, request, commandExecutionOptions{
			NodeName:          "node-b",
			Topology:          topology,
			ReplicationSafety: NewReplicationSafetyStore(),
		})
		status := http.StatusOK
		if rejected {
			status = http.StatusConflict
		}
		format, _ := commandWireFormatFromContentType(r.Header.Get("Content-Type"))
		writeCommandResponseWire(w, r, status, response, format)
	}))
	defer target.Close()
	topology = replicationTestTopology(t, target.URL)
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: NewElectionStore(topology, ElectionOptions{}),
		Client:   target.Client(),
	})

	result := replicator.SyncAll(context.Background(), source, "session:")
	if result.Skipped || result.Entries != 3 || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("SyncAll(digest) = %#v, want three compared entries and one successful changed batch", result)
	}
	if got := normalizedCommand((<-requests).Command); got != replicationDigestCommand {
		t.Fatalf("first sync command = %q, want %s", got, replicationDigestCommand)
	}
	payloads := mustDecodeReplicationBatchValues(t, <-requests)
	if len(payloads) != 3 {
		t.Fatalf("changed payloads = %#v, want two sets and one delete", payloads)
	}
	commands := make(map[string]string, len(payloads))
	for _, payload := range payloads {
		commands[payload.Key] = normalizedCommand(payload.Command)
	}
	if commands["session:changed"] != replicationSetCompactCommand ||
		commands["session:missing"] != replicationSetCompactCommand ||
		commands["session:stale"] != "INTERNALDEL" {
		t.Fatalf("changed commands = %#v, want changed/missing sets and stale delete", commands)
	}
	if _, exists := commands["session:same"]; exists {
		t.Fatalf("unchanged key was transferred: %#v", commands)
	}
	select {
	case request := <-requests:
		t.Fatalf("unexpected extra sync request = %#v", request)
	default:
	}
	for key, want := range map[string]string{
		"session:changed": "new",
		"session:missing": "source-only",
		"session:same":    "equal",
	} {
		if got := targetTrie.GetString(key); got != want {
			t.Fatalf("target %s = %q, want %q", key, got, want)
		}
	}
	if targetTrie.Exists("session:stale") {
		t.Fatal("target stale key still exists after digest sync")
	}
	if got := targetTrie.GetString("other:preserved"); got != "unrelated" {
		t.Fatalf("unrelated target key = %q, want preserved", got)
	}

	unchanged := replicator.SyncAll(context.Background(), source, "session:")
	if unchanged.Skipped || len(unchanged.Targets) != 1 || !unchanged.Targets[0].OK || !strings.Contains(unchanged.Reason, "transferred 0, deleted 0") {
		t.Fatalf("SyncAll(unchanged digest) = %#v, want one digest request and no writes", unchanged)
	}
	if got := normalizedCommand((<-requests).Command); got != replicationDigestCommand {
		t.Fatalf("unchanged sync command = %q, want %s", got, replicationDigestCommand)
	}
	select {
	case request := <-requests:
		t.Fatalf("unchanged sync sent value request = %#v", request)
	default:
	}
}

func TestHTTPReplicatorSyncAllDigestFallsBackToFullPushForOlderTarget(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("session:1", "one")
	source.UpsertString("session:2", "two")
	requests := make(chan CacheCommandRequest, 6)
	var requestCount atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requestCount.Add(1)
		requests <- request
		if normalizedCommand(request.Command) == replicationDigestCommand {
			format, _ := commandWireFormatFromContentType(r.Header.Get("Content-Type"))
			writeCommandResponseWire(w, r, http.StatusOK, commandError("unsupported command"), format)
			return
		}
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()
	topology := replicationTestTopology(t, target.URL)
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: NewElectionStore(topology, ElectionOptions{}),
		Client:   target.Client(),
	})

	result := replicator.SyncAll(context.Background(), source, "")
	if result.Skipped || result.Entries != 2 || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("SyncAll(fallback) = %#v, want one successful full-push batch", result)
	}
	if got := normalizedCommand((<-requests).Command); got != replicationDigestCommand {
		t.Fatalf("first sync command = %q, want %s", got, replicationDigestCommand)
	}
	if got := normalizedCommand((<-requests).Command); got != replicationDigestCommand {
		t.Fatalf("legacy fallback command = %q, want %s", got, replicationDigestCommand)
	}
	payloads := mustDecodeReplicationBatchValues(t, <-requests)
	if len(payloads) != 2 {
		t.Fatalf("fallback payloads = %#v, want both source values", payloads)
	}
	for _, payload := range payloads {
		if normalizedCommand(payload.Command) != replicationSetCompactCommand {
			t.Fatalf("fallback payload = %#v, want compact set", payload)
		}
	}
	if got := requestCount.Load(); got != 3 {
		t.Fatalf("first fallback sync requests = %d, want two probes plus one repair", got)
	}
	routing, ok := replicator.snapshotReplicationRouting()
	if !ok {
		t.Fatal("snapshotReplicationRouting() ok = false")
	}
	learnedTarget, single := singleReplicationDigestTarget(routing, "node-a")
	if !single {
		t.Fatalf("singleReplicationDigestTarget() = %#v/false, want node-b", learnedTarget)
	}
	if !replicator.replicationDigestUnsupported(learnedTarget, routing.fingerprint) {
		t.Fatal("first fallback did not retain unsupported digest capability")
	}

	second := replicator.SyncAll(context.Background(), source, "")
	if second.Skipped || second.Entries != 2 || len(second.Targets) != 1 || !second.Targets[0].OK {
		t.Fatalf("second SyncAll(fallback) = %#v, want direct successful full push", second)
	}
	secondPayloads := mustDecodeReplicationBatchValues(t, <-requests)
	if len(secondPayloads) != 2 {
		t.Fatalf("cached fallback payloads = %#v, want both source values", secondPayloads)
	}
	select {
	case request := <-requests:
		t.Fatalf("cached fallback sent redundant request = %#v", request)
	default:
	}
	if got := requestCount.Load(); got != 4 {
		t.Fatalf("two fallback sync requests = %d, want first 3 plus one cached repair", got)
	}
}

func TestReplicationDigestUnsupportedCapabilityExpiresAndMatchesTarget(t *testing.T) {
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a"})
	t.Cleanup(replicator.Close)
	now := time.Unix(1_700_000_000, 0)
	replicator.capabilityNow = func() time.Time { return now }
	target := TopologyNode{ID: "node-b", Address: "http://node-b"}
	replicator.markReplicationDigestUnsupported(target, "fingerprint-a")

	if !replicator.replicationDigestUnsupported(target, "fingerprint-a") {
		t.Fatal("replicationDigestUnsupported() = false, want learned capability")
	}
	if replicator.replicationDigestUnsupported(TopologyNode{ID: target.ID, Address: "http://replacement"}, "fingerprint-a") {
		t.Fatal("replacement address reused stale digest capability")
	}
	if replicator.replicationDigestUnsupported(target, "fingerprint-b") {
		t.Fatal("new topology fingerprint reused stale digest capability")
	}
	now = now.Add(replicationDigestCapabilityTTL + time.Nanosecond)
	if replicator.replicationDigestUnsupported(target, "fingerprint-a") {
		t.Fatal("expired digest capability remained active")
	}
}

func TestReplicationDigestUnsupportedSkipsClockWithoutCapability(t *testing.T) {
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a"})
	t.Cleanup(replicator.Close)
	clockReads := 0
	replicator.capabilityNow = func() time.Time {
		clockReads++
		return time.Unix(1_700_000_000, 0)
	}
	if replicator.replicationDigestUnsupported(TopologyNode{ID: "node-b", Address: "http://node-b"}, "fingerprint-a") {
		t.Fatal("missing replication digest capability reported unsupported")
	}
	if clockReads != 0 {
		t.Fatalf("missing capability clock reads = %d, want 0", clockReads)
	}
}

func TestReplicationScanRouteForKeyMatchesGenericRouting(t *testing.T) {
	for _, tt := range []struct {
		name     string
		topology ClusterTopology
	}{
		{
			name: "single shard",
			topology: ClusterTopology{
				Version: 1,
				Self:    "node-a",
				Nodes:   []TopologyNode{{ID: "node-a"}, {ID: "node-b", Address: "http://node-b"}},
				Shards:  []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
			},
		},
		{
			name: "single bucket range",
			topology: ClusterTopology{
				Version:      1,
				Self:         "node-a",
				BucketCount:  16,
				BucketRanges: []TopologyBucketRange{{Start: 0, End: 15, Shard: 7}},
				Nodes:        []TopologyNode{{ID: "node-a"}, {ID: "node-b", Address: "http://node-b"}},
				Shards:       []TopologyShard{{ID: 7, Primary: "node-a", Replicas: []string{"node-b"}}},
			},
		},
		{
			name: "multiple shards",
			topology: ClusterTopology{
				Version:      1,
				Self:         "node-a",
				BucketCount:  16,
				BucketRanges: []TopologyBucketRange{{Start: 0, End: 7, Shard: 1}, {Start: 8, End: 15, Shard: 2}},
				Nodes:        []TopologyNode{{ID: "node-a"}, {ID: "node-b"}, {ID: "node-c"}},
				Shards: []TopologyShard{
					{ID: 1, Primary: "node-a", Replicas: []string{"node-b"}},
					{ID: 2, Primary: "node-c", Replicas: []string{"node-a"}},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			topology, err := NewTopologyStore(tt.topology)
			if err != nil {
				t.Fatalf("NewTopologyStore() error = %v", err)
			}
			replicator := NewHTTPReplicator(HTTPReplicatorOptions{
				Self:     "node-a",
				Topology: topology,
				Election: NewElectionStore(topology, ElectionOptions{}),
			})
			t.Cleanup(replicator.Close)
			routing, ok := replicator.snapshotReplicationRouting()
			if !ok {
				t.Fatal("snapshotReplicationRouting() ok = false")
			}
			for _, key := range []string{"", "session:1", "session:999", "other:value"} {
				want, wantOK := routing.routeForKey(key)
				got, gotOK := routing.replicationScanRouteForKey(key)
				if gotOK != wantOK {
					t.Fatalf("replicationScanRouteForKey(%q) ok = %v, want %v", key, gotOK, wantOK)
				}
				if !gotOK {
					continue
				}
				if got.Route.Shard.ID != want.Route.Shard.ID || !reflect.DeepEqual(got.Leader, want.Leader) || !reflect.DeepEqual(got.Route.Owners, want.Route.Owners) || !reflect.DeepEqual(routing.replicationTargets(got, "node-a"), routing.replicationTargets(want, "node-a")) {
					t.Fatalf("replication scan route(%q) = %#v, want routing scope %#v", key, got, want)
				}
			}
		})
	}
}

func TestReplicationValueDigestEncodingRoundTrip(t *testing.T) {
	want := replicationDigest{hash: math.MaxUint64 - 7, size: math.MaxUint64 - 11}
	encoded := encodeReplicationValueDigest(want)
	got, err := decodeReplicationValueDigest(encoded)
	if err != nil || got != want {
		t.Fatalf("decodeReplicationValueDigest(%q) = %#v/%v, want %#v", encoded, got, err, want)
	}
	if _, err := decodeReplicationValueDigest(encoded + "x"); err == nil {
		t.Fatal("decodeReplicationValueDigest(invalid) error = nil")
	}
}

func TestHTTPReplicatorRejectsUnorderedDigestWithoutWriting(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("session:1", "one")
	source.UpsertString("session:2", "two")
	var writes atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		format, _ := commandWireFormatFromContentType(r.Header.Get("Content-Type"))
		if normalizedCommand(request.Command) != replicationDigestCommand {
			writes.Add(1)
			writeCommandResponseWire(w, r, http.StatusOK, CacheCommandResponse{OK: true}, format)
			return
		}
		digest := encodeReplicationValueDigest(replicationDigest{hash: 1, size: 1})
		writeCommandResponseWire(w, r, http.StatusOK, CacheCommandResponse{
			OK:      true,
			Message: replicationDigestFinalMessage,
			Responses: []CacheCommandResponse{
				{OK: true, Message: "session:2", Value: digest},
				{OK: true, Message: "session:1", Value: digest},
			},
		}, format)
	}))
	defer target.Close()
	topology := replicationTestTopology(t, target.URL)
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: NewElectionStore(topology, ElectionOptions{}),
		Client:   target.Client(),
	})

	result := replicator.SyncAll(context.Background(), source, "session:")
	if len(result.Targets) == 0 || result.Targets[len(result.Targets)-1].OK || !strings.Contains(result.Targets[len(result.Targets)-1].Error, "strictly ordered") {
		t.Fatalf("SyncAll(unordered digest) = %#v, want ordered-entry failure", result)
	}
	if got := writes.Load(); got != 0 {
		t.Fatalf("repair writes = %d, want none after malformed digest", got)
	}
}

func TestHTTPReplicatorRejectsOutOfScopeDigestWithoutWriting(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("session:1", "one")
	var writes atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		format, _ := commandWireFormatFromContentType(r.Header.Get("Content-Type"))
		if normalizedCommand(request.Command) != replicationDigestCommand {
			writes.Add(1)
			writeCommandResponseWire(w, r, http.StatusOK, CacheCommandResponse{OK: true}, format)
			return
		}
		writeCommandResponseWire(w, r, http.StatusOK, CacheCommandResponse{
			OK:      true,
			Message: replicationDigestFinalMessage,
			Responses: []CacheCommandResponse{{
				OK:      true,
				Message: "other:must-not-delete",
				Value:   encodeReplicationValueDigest(replicationDigest{hash: 1, size: 1}),
			}},
		}, format)
	}))
	defer target.Close()
	topology := replicationTestTopology(t, target.URL)
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: NewElectionStore(topology, ElectionOptions{}),
		Client:   target.Client(),
	})

	result := replicator.SyncAll(context.Background(), source, "session:")
	if len(result.Targets) == 0 || result.Targets[len(result.Targets)-1].OK || !strings.Contains(result.Targets[len(result.Targets)-1].Error, "outside replication digest scope") {
		t.Fatalf("SyncAll(out-of-scope digest) = %#v, want scope failure", result)
	}
	if got := writes.Load(); got != 0 {
		t.Fatalf("repair writes = %d, want none after out-of-scope digest", got)
	}
}

func TestHTTPReplicatorSyncAllBoundsDigestAndFallbackWriteBatches(t *testing.T) {
	const keyCount = defaultReplicationSyncKeyPageSize + 1
	for _, test := range []struct {
		name         string
		legacyTarget bool
	}{
		{name: "digest mismatch"},
		{name: "legacy fallback", legacyTarget: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := newTestTrie(t)
			targetTrie := newTestTrie(t)
			for idx := 0; idx < keyCount; idx++ {
				key := fmt.Sprintf("session:%04d", idx)
				source.UpsertString(key, "source")
				if !test.legacyTarget {
					targetTrie.UpsertString(key, "target")
				}
			}

			var topology *TopologyStore
			safety := NewReplicationSafetyStore()
			largestBatch := 0
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				request := mustDecodeReplicationTestCommand(t, w, r)
				format, _ := commandWireFormatFromContentType(r.Header.Get("Content-Type"))
				if test.legacyTarget && normalizedCommand(request.Command) == replicationDigestCommand {
					writeCommandResponseWire(w, r, http.StatusOK, commandError("unsupported command"), format)
					return
				}
				if normalizedCommand(request.Command) != replicationDigestCommand {
					size := 1
					if normalizedCommand(request.Command) == "INTERNALBATCH" || normalizedCommand(request.Command) == replicationBatchEnvelopeCommand {
						size = len(mustDecodeReplicationBatchValues(t, request))
					}
					if size > largestBatch {
						largestBatch = size
					}
				}
				response, rejected := executeCacheCommand(r.Context(), targetTrie, request, commandExecutionOptions{
					NodeName:          "node-b",
					Topology:          topology,
					ReplicationSafety: safety,
				})
				status := http.StatusOK
				if rejected {
					status = http.StatusConflict
				}
				writeCommandResponseWire(w, r, status, response, format)
			}))
			defer target.Close()
			topology = replicationTestTopology(t, target.URL)
			replicator := NewHTTPReplicator(HTTPReplicatorOptions{
				Self:     "node-a",
				Topology: topology,
				Election: NewElectionStore(topology, ElectionOptions{}),
				Client:   target.Client(),
			})

			result := replicator.SyncAll(context.Background(), source, "session:")
			if result.Skipped || len(result.Targets) == 0 {
				t.Fatalf("SyncAll() = %#v, want successful bounded sync", result)
			}
			for _, targetResult := range result.Targets {
				if !targetResult.OK {
					t.Fatalf("SyncAll() target = %#v, want success", targetResult)
				}
			}
			if largestBatch > defaultReplicationSyncKeyPageSize {
				t.Fatalf("largest write batch = %d, want at most %d", largestBatch, defaultReplicationSyncKeyPageSize)
			}
			if got := targetTrie.GetString("session:0000"); got != "source" {
				t.Fatalf("first target value = %q, want source", got)
			}
			if got := targetTrie.GetString(fmt.Sprintf("session:%04d", keyCount-1)); got != "source" {
				t.Fatalf("last target value = %q, want source", got)
			}
		})
	}
}

func TestHTTPReplicatorSyncAllChecksDigestTargetsConcurrently(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("session:1", "one")
	arrived := make(chan string, 2)
	release := make(chan struct{})
	var topology *TopologyStore
	newTarget := func(node string) *httptest.Server {
		t.Helper()
		trie := newTestTrie(t)
		trie.UpsertString("session:1", "one")
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			request := mustDecodeReplicationTestCommand(t, w, r)
			if normalizedCommand(request.Command) != replicationDigestCommand {
				t.Fatalf("%s command = %q, want digest", node, request.Command)
			}
			arrived <- node
			<-release
			response, rejected := executeCacheCommand(r.Context(), trie, request, commandExecutionOptions{
				NodeName: node,
				Topology: topology,
			})
			status := http.StatusOK
			if rejected {
				status = http.StatusConflict
			}
			format, _ := commandWireFormatFromContentType(r.Header.Get("Content-Type"))
			writeCommandResponseWire(w, r, status, response, format)
		}))
	}
	targetB := newTarget("node-b")
	defer targetB.Close()
	targetC := newTarget("node-c")
	defer targetC.Close()
	var err error
	topology, err = NewTopologyStore(ClusterTopology{
		Version: 1,
		Mode:    TopologyModeFullReplica,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a"},
			{ID: "node-b", Address: targetB.URL},
			{ID: "node-c", Address: targetC.URL},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:               "node-a",
		Topology:           topology,
		Client:             targetB.Client(),
		MaxInFlightTargets: 2,
	})
	done := make(chan ReplicationResult, 1)
	go func() {
		done <- replicator.SyncAll(context.Background(), source, "session:")
	}()

	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case node := <-arrived:
			seen[node] = true
		case <-time.After(250 * time.Millisecond):
			close(release)
			<-done
			t.Fatalf("concurrent digest arrivals = %#v, want node-b and node-c", seen)
		}
	}
	close(release)
	result := <-done
	if len(result.Targets) != 2 || !result.Targets[0].OK || result.Targets[0].Node != "node-b" || !result.Targets[1].OK || result.Targets[1].Node != "node-c" {
		t.Fatalf("SyncAll() targets = %#v, want ordered successful node-b/node-c", result.Targets)
	}
}

func TestHTTPReplicatorRecordsLatencyAndBatchMetrics(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		if rejectReplicationDigestTestCommand(t, w, r, request) {
			return
		}
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	trie.UpsertString("session:1", "value-1")
	trie.UpsertString("session:2", "value-2")
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	result := replicator.SyncAll(context.Background(), trie, "session:")
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("sync result = %#v, want one ok target", result)
	}
	metrics := replicator.MetricsSnapshot()
	latency := metrics.TargetLatencyMillis["node-b"]
	if latency.Count != 1 || latency.Sum < 0 {
		t.Fatalf("target latency histogram = %#v, want one observation", latency)
	}
	batch := metrics.TargetBatchItems["node-b"]
	if batch.Count != 1 || batch.Sum != 2 {
		t.Fatalf("batch size histogram = %#v, want one two-item observation", batch)
	}
}

func TestHTTPReplicatorSplitsSyncBatchesByEstimatedBytes(t *testing.T) {
	requests := make(chan CacheCommandRequest, 3)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/commands" {
			t.Fatalf("path = %s, want /api/commands", r.URL.Path)
		}
		request := mustDecodeReplicationTestCommand(t, w, r)
		if rejectReplicationDigestTestCommand(t, w, r, request) {
			return
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	trie.UpsertString("session:1", "value-1")
	trie.UpsertString("session:2", "value-2")
	trie.UpsertString("session:3", "value-3")
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:                     "node-a",
		Topology:                 topology,
		Election:                 election,
		Client:                   target.Client(),
		ReplicationBatchMaxBytes: 1,
	})

	result := replicator.SyncAll(context.Background(), trie, "session:")
	if result.Skipped || result.Entries != 3 || len(result.Targets) != 3 {
		t.Fatalf("sync result = %#v, want three split target requests", result)
	}
	seen := map[string]bool{}
	for idx := 0; idx < 3; idx++ {
		request := <-requests
		if !isTypedReplicationSetPayload(request, request.Key) {
			t.Fatalf("split request %d = %#v, want single typed binary snapshot", idx, request)
		}
		seen[request.Key] = true
	}
	for _, key := range []string{"session:1", "session:2", "session:3"} {
		if !seen[key] {
			t.Fatalf("split requests keys = %#v, missing %s", seen, key)
		}
	}
	select {
	case request := <-requests:
		t.Fatalf("unexpected sync request = %#v", request)
	default:
	}
}

func TestGroupReplicationTasksByTargetAdaptiveMatchesMapGrouping(t *testing.T) {
	for _, tt := range []struct {
		name          string
		uniqueTargets int
		tasks         int
	}{
		{name: "linear path", uniqueTargets: 3, tasks: 12},
		{name: "map fallback", uniqueTargets: 8, tasks: 16},
		{name: "large map path", uniqueTargets: 64, tasks: 128},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tasks := replicationGroupingBenchmarkTasks(tt.uniqueTargets, tt.tasks)
			got := groupReplicationTasksByTarget(tasks)
			want := groupReplicationTasksByTargetMap(tasks)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("adaptive groups = %#v, want map groups %#v", got, want)
			}
		})
	}
}

func TestGroupReplicationTasksByTargetCarriesPayloadBytes(t *testing.T) {
	target := TopologyNode{ID: "node-b", Address: "http://node-b"}
	tasks := []replicationTask{
		{target: target, payload: CacheCommandRequest{Command: "INTERNALSET", Key: "session:1", Value: "one"}, payloadBytes: 101},
		{target: target, payload: CacheCommandRequest{Command: "INTERNALSET", Key: "session:2", Value: "two"}, payloadBytes: 202},
	}

	groups := groupReplicationTasksByTarget(tasks)
	if len(groups) != 1 {
		t.Fatalf("groups len = %d, want 1", len(groups))
	}
	if !reflect.DeepEqual(groups[0].payloadBytes, []int{101, 202}) {
		t.Fatalf("group payload bytes = %#v, want carried estimates", groups[0].payloadBytes)
	}
}

func TestHTTPReplicatorAppendsPayloadDirectlyToTargetGroups(t *testing.T) {
	replicator := &HTTPReplicator{self: "node-a", batchMaxBytes: 4096}
	targetB := TopologyNode{ID: "node-b", Address: "http://node-b"}
	targetC := TopologyNode{ID: "node-c", Address: "http://node-c"}
	groups := make([]replicationTaskGroup, 0, 2)
	indexes := make(map[TopologyNode]int, 2)

	groups = replicator.appendReplicationPayloadToTargetGroups(groups, indexes, 8, []TopologyNode{targetB, targetC}, CacheCommandRequest{
		Command: "INTERNALSET", Key: "session:1", Value: `{"type":"string","string":"one"}`,
	}, "fingerprint-a")
	groups = replicator.appendReplicationPayloadToTargetGroups(groups, indexes, 8, []TopologyNode{targetB}, CacheCommandRequest{
		Command: "INTERNALDEL", Key: "session:2",
	}, "fingerprint-a")

	if len(groups) != 2 {
		t.Fatalf("groups len = %d, want 2", len(groups))
	}
	if !replicationTargetsEqual(groups[0].target, targetB) || !reflect.DeepEqual(groups[0].keys, []string{"session:1", "session:2"}) {
		t.Fatalf("node-b group = %#v, want both payloads", groups[0])
	}
	if !replicationTargetsEqual(groups[1].target, targetC) || !reflect.DeepEqual(groups[1].keys, []string{"session:1"}) {
		t.Fatalf("node-c group = %#v, want first payload", groups[1])
	}
	for groupIdx, group := range groups {
		for payloadIdx, payload := range group.payloads {
			if len(payload.Pairs) != 0 {
				t.Fatalf("group %d payload %d metadata = %#v, want deferred batch metadata", groupIdx, payloadIdx, payload.Pairs)
			}
			if group.payloadBytes[payloadIdx] <= 0 {
				t.Fatalf("group %d payload %d bytes = %d, want positive estimate", groupIdx, payloadIdx, group.payloadBytes[payloadIdx])
			}
		}
	}
	if replicator.sequence != 0 {
		t.Fatalf("replication sequence = %d, want allocation-free metadata deferred until delivery", replicator.sequence)
	}
}

func TestHTTPReplicatorExecutesTargetGroupsInParallelAndPreservesOrder(t *testing.T) {
	entered := make(chan string, 3)
	release := make(chan struct{})
	newTarget := func(id string) *httptest.Server {
		t.Helper()
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			entered <- id
			<-release
			writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
		}))
	}
	servers := []*httptest.Server{newTarget("node-c"), newTarget("node-a"), newTarget("node-b")}
	defer func() {
		for _, server := range servers {
			server.Close()
		}
	}()
	groups := make([]replicationTaskGroup, len(servers))
	for idx, server := range servers {
		groups[idx] = replicationTaskGroup{
			target:   TopologyNode{ID: []string{"node-c", "node-a", "node-b"}[idx], Address: server.URL},
			payloads: []CacheCommandRequest{{Command: "INTERNALDEL", Key: "session:1"}},
			keys:     []string{"session:1"},
		}
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Client: servers[0].Client()})
	resultCh := make(chan ReplicationResult, 1)
	go func() {
		resultCh <- replicator.executeReplicationTaskGroups(context.Background(), ReplicationResult{}, groups)
	}()
	for idx := 0; idx < len(groups); idx++ {
		select {
		case <-entered:
		case <-time.After(500 * time.Millisecond):
			close(release)
			t.Fatalf("only %d/%d targets entered before release", idx, len(groups))
		}
	}
	close(release)
	result := <-resultCh
	if len(result.Targets) != 3 {
		t.Fatalf("targets = %#v, want three results", result.Targets)
	}
	for idx, want := range []string{"node-c", "node-a", "node-b"} {
		if result.Targets[idx].Node != want || !result.Targets[idx].OK {
			t.Fatalf("target %d = %#v, want ordered successful %s", idx, result.Targets[idx], want)
		}
	}
}

func TestHTTPReplicatorBoundsParallelTargetGroups(t *testing.T) {
	var active atomic.Int64
	var maximum atomic.Int64
	entered := make(chan struct{}, 4)
	release := make(chan struct{}, 4)
	newTarget := func() *httptest.Server {
		t.Helper()
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			current := active.Add(1)
			for {
				previous := maximum.Load()
				if current <= previous || maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			entered <- struct{}{}
			<-release
			active.Add(-1)
			writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
		}))
	}
	servers := []*httptest.Server{newTarget(), newTarget(), newTarget(), newTarget()}
	defer func() {
		for _, server := range servers {
			server.Close()
		}
	}()
	groups := make([]replicationTaskGroup, len(servers))
	for idx, server := range servers {
		groups[idx] = replicationTaskGroup{
			target:   TopologyNode{ID: "node-" + strconv.Itoa(idx), Address: server.URL},
			payloads: []CacheCommandRequest{{Command: "INTERNALDEL", Key: "session:1"}},
		}
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Client: servers[0].Client(), MaxInFlightTargets: 2})
	resultCh := make(chan ReplicationResult, 1)
	go func() {
		resultCh <- replicator.executeReplicationTaskGroups(context.Background(), ReplicationResult{}, groups)
	}()
	for idx := 0; idx < 2; idx++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatalf("initial target %d did not enter", idx)
		}
	}
	select {
	case <-entered:
		t.Fatal("third target entered before a bounded worker was released")
	case <-time.After(50 * time.Millisecond):
	}
	release <- struct{}{}
	release <- struct{}{}
	for idx := 0; idx < 2; idx++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatalf("remaining target %d did not enter", idx)
		}
	}
	release <- struct{}{}
	release <- struct{}{}
	result := <-resultCh
	if len(result.Targets) != 4 || maximum.Load() != 2 {
		t.Fatalf("result targets/max concurrency = %d/%d, want 4/2", len(result.Targets), maximum.Load())
	}
}

func TestHTTPReplicatorDeferredGroupMetadataFallsBackToLegacy(t *testing.T) {
	var requests atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		switch requests.Add(1) {
		case 1:
			if request.Command != replicationBatchEnvelopeCommand || len(request.Batch) != 2 {
				t.Fatalf("typed request = %#v, want two-item batch envelope", request)
			}
			if source, sequence, fingerprint := replicationSafetyMetadata(request); source != "node-a" || sequence == 0 || fingerprint != "fingerprint-a" {
				t.Fatalf("typed envelope metadata = %q/%d/%q", source, sequence, fingerprint)
			}
			for idx, payload := range request.Batch {
				if len(payload.Pairs) != 0 {
					t.Fatalf("typed child %d metadata = %#v, want envelope-only metadata", idx, payload.Pairs)
				}
			}
			writeJSON(w, commandError("unsupported command"))
		case 2:
			if request.Command != "INTERNALBATCH" || len(request.Batch) != 2 {
				t.Fatalf("legacy fallback = %#v, want two-item INTERNALBATCH", request)
			}
			for idx, payload := range request.Batch {
				source, sequence, fingerprint := replicationSafetyMetadata(payload)
				if source != "node-a" || sequence == 0 || fingerprint != "fingerprint-a" {
					t.Fatalf("legacy child %d metadata = %q/%d/%q", idx, source, sequence, fingerprint)
				}
			}
			writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
		default:
			t.Fatalf("unexpected request %d", requests.Load())
		}
	}))
	defer target.Close()

	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a", Client: target.Client()})
	targetNode := TopologyNode{ID: "node-b", Address: target.URL}
	groups := replicator.appendReplicationPayloadToTargetGroups(nil, map[TopologyNode]int{}, 2, []TopologyNode{targetNode}, CacheCommandRequest{
		Command: "INTERNALSET", Key: "session:1", Value: `{"type":"string","string":"one"}`,
	}, "fingerprint-a")
	groups = replicator.appendReplicationPayloadToTargetGroups(groups, map[TopologyNode]int{targetNode: 0}, 2, []TopologyNode{targetNode}, CacheCommandRequest{
		Command: "INTERNALDEL", Key: "session:2",
	}, "fingerprint-a")

	result := replicator.executeReplicationTaskGroups(context.Background(), ReplicationResult{}, groups)
	if len(result.Targets) != 1 || !result.Targets[0].OK || requests.Load() != 2 {
		t.Fatalf("fallback result = %#v requests=%d, want successful typed-to-legacy retry", result, requests.Load())
	}
}

func TestHTTPReplicatorAppendsTasksWithAnnotatedPayloadBytes(t *testing.T) {
	replicator := &HTTPReplicator{
		self:          "node-a",
		batchMaxBytes: 4096,
	}
	targets := []TopologyNode{
		{ID: "node-b", Address: "http://node-b"},
		{ID: "node-c", Address: "http://node-c"},
	}
	payload := CacheCommandRequest{Command: "INTERNALSET", Key: "session:1", Value: "one"}

	tasks := replicator.appendReplicationTasksForTargets(nil, targets, payload)
	if len(tasks) != len(targets) {
		t.Fatalf("tasks len = %d, want %d", len(tasks), len(targets))
	}
	for idx, task := range tasks {
		if !replicationTargetsEqual(task.target, targets[idx]) {
			t.Fatalf("task %d target = %#v, want %#v", idx, task.target, targets[idx])
		}
		if task.payloadBytes <= 0 {
			t.Fatalf("task %d payload bytes = %d, want positive carried estimate", idx, task.payloadBytes)
		}
		if task.payload.Pairs[replicationMetaSourceNode] != "node-a" || task.payload.Pairs[replicationMetaSequence] == "" {
			t.Fatalf("task %d payload metadata = %#v, want source and sequence", idx, task.payload.Pairs)
		}
		if idx > 0 {
			if task.payloadBytes != tasks[0].payloadBytes {
				t.Fatalf("task %d payload bytes = %d, want shared estimate %d", idx, task.payloadBytes, tasks[0].payloadBytes)
			}
			if task.payload.Pairs[replicationMetaSequence] != tasks[0].payload.Pairs[replicationMetaSequence] {
				t.Fatalf("task %d sequence = %q, want shared sequence %q", idx, task.payload.Pairs[replicationMetaSequence], tasks[0].payload.Pairs[replicationMetaSequence])
			}
		}
	}
}

func TestHTTPReplicatorAppendsTasksWithoutPayloadEstimateWhenBatchLimitDisabled(t *testing.T) {
	replicator := &HTTPReplicator{self: "node-a"}
	targets := []TopologyNode{{ID: "node-b", Address: "http://node-b"}}
	payload := CacheCommandRequest{Command: "INTERNALSET", Key: "session:1", Value: "one"}

	tasks := replicator.appendReplicationTasksForTargets(nil, targets, payload)
	if len(tasks) != 1 {
		t.Fatalf("tasks len = %d, want 1", len(tasks))
	}
	if tasks[0].payloadBytes != 0 {
		t.Fatalf("payload bytes = %d, want no unused estimate without a batch byte limit", tasks[0].payloadBytes)
	}
	if tasks[0].payload.Pairs[replicationMetaSourceNode] != "node-a" || tasks[0].payload.Pairs[replicationMetaSequence] == "" {
		t.Fatalf("payload metadata = %#v, want source and sequence", tasks[0].payload.Pairs)
	}
}

func TestSplitReplicationTaskGroupByMaxBytesUsesCarriedPayloadBytes(t *testing.T) {
	target := TopologyNode{ID: "node-b", Address: "http://node-b"}
	group := replicationTaskGroup{
		target: target,
		payloads: []CacheCommandRequest{
			{Command: "INTERNALSET", Key: "session:1", Value: "one"},
			{Command: "INTERNALSET", Key: "session:2", Value: "two"},
		},
		keys:         []string{"session:1", "session:2"},
		payloadBytes: []int{90, 90},
	}

	groups := splitReplicationTaskGroupByMaxBytes(group, 160)
	if len(groups) != 2 {
		t.Fatalf("split groups len = %d, want 2 from carried estimates", len(groups))
	}
	if len(groups[0].payloadBytes) != 1 || groups[0].payloadBytes[0] != 90 {
		t.Fatalf("first split payload bytes = %#v, want carried first estimate", groups[0].payloadBytes)
	}
	if len(groups[1].payloadBytes) != 1 || groups[1].payloadBytes[0] != 90 {
		t.Fatalf("second split payload bytes = %#v, want carried second estimate", groups[1].payloadBytes)
	}
}

func TestSplitReplicationTaskGroupByMaxBytesReusesGenericBacking(t *testing.T) {
	payloads := []CacheCommandRequest{
		{Command: "INTERNALSET", Key: "session:1", Value: "one"},
		{Command: "INTERNALSET", Key: "session:2", Value: "two"},
		{Command: "INTERNALSET", Key: "session:3", Value: "three"},
	}
	keys := []string{"session:1", "session:2", "session:3"}
	payloadBytes := []int{90, 90, 90}
	group := replicationTaskGroup{
		target:           TopologyNode{ID: "node-b", Address: "http://node-b"},
		payloads:         payloads,
		keys:             keys,
		payloadBytes:     payloadBytes,
		deferredMetadata: true,
		metadataSource:   "node-a",
		metadataTopology: "fingerprint-a",
	}

	groups := splitReplicationTaskGroupByMaxBytes(group, 160)
	if len(groups) != len(payloads) {
		t.Fatalf("split groups len = %d, want %d", len(groups), len(payloads))
	}
	for idx := range groups {
		split := groups[idx]
		if len(split.payloads) != 1 || len(split.keys) != 1 || len(split.payloadBytes) != 1 {
			t.Fatalf("split group %d lengths = %d/%d/%d, want 1/1/1", idx, len(split.payloads), len(split.keys), len(split.payloadBytes))
		}
		if &split.payloads[0] != &payloads[idx] || &split.keys[0] != &keys[idx] || &split.payloadBytes[0] != &payloadBytes[idx] {
			t.Fatalf("split group %d copied generic backing storage", idx)
		}
		if !split.deferredMetadata || split.metadataSource != group.metadataSource || split.metadataTopology != group.metadataTopology {
			t.Fatalf("split group %d metadata = %#v, want source metadata preserved", idx, split)
		}
	}
}

func TestSplitReplicationTaskGroupByMaxBytesReusesCompactBacking(t *testing.T) {
	payloads := []replicationSyncPayload{
		{key: "session:1", binaryValue: []byte("binary-one"), payloadBytes: 90},
		{key: "session:2", binaryValue: []byte("binary-two"), payloadBytes: 90},
		{key: "session:3", binaryValue: []byte("binary-three"), payloadBytes: 90},
	}
	group := replicationTaskGroup{
		target:           TopologyNode{ID: "node-b", Address: "http://node-b"},
		syncPayloads:     payloads,
		deferredMetadata: true,
		metadataSource:   "node-a",
		metadataTopology: "fingerprint-a",
	}

	groups := splitReplicationTaskGroupByMaxBytes(group, 160)
	if len(groups) != len(payloads) {
		t.Fatalf("split groups len = %d, want %d", len(groups), len(payloads))
	}
	for idx := range groups {
		split := groups[idx]
		if len(split.syncPayloads) != 1 || &split.syncPayloads[0] != &payloads[idx] {
			t.Fatalf("split group %d copied compact backing storage", idx)
		}
		if !split.deferredMetadata || split.metadataSource != group.metadataSource || split.metadataTopology != group.metadataTopology {
			t.Fatalf("split group %d metadata = %#v, want source metadata preserved", idx, split)
		}
	}
}

func TestSplitReplicationTaskGroupByMaxBytesReusesArenaIndexBacking(t *testing.T) {
	arena := newReplicationSyncPayloadArena(3)
	indexes := make([]uint32, 0, 3)
	for idx, key := range []string{"session:1", "session:2", "session:3"} {
		record, err := arena.append(key, []byte("binary-"+key), 90)
		if err != nil {
			t.Fatalf("arena append %d: %v", idx, err)
		}
		indexes = append(indexes, record)
	}
	group := replicationTaskGroup{
		target:             TopologyNode{ID: "node-b", Address: "http://node-b"},
		syncPayloadArena:   arena,
		syncPayloadIndexes: indexes,
		deferredMetadata:   true,
		metadataSource:     "node-a",
		metadataTopology:   "fingerprint-a",
	}

	groups := splitReplicationTaskGroupByMaxBytes(group, 160)
	if len(groups) != len(indexes) {
		t.Fatalf("split groups len = %d, want %d", len(groups), len(indexes))
	}
	for idx := range groups {
		split := groups[idx]
		if split.syncPayloadArena != arena || len(split.syncPayloadIndexes) != 1 || &split.syncPayloadIndexes[0] != &indexes[idx] {
			t.Fatalf("split group %d copied arena index backing storage", idx)
		}
		if !split.deferredMetadata || split.metadataSource != group.metadataSource || split.metadataTopology != group.metadataTopology {
			t.Fatalf("split group %d metadata = %#v, want source metadata preserved", idx, split)
		}
	}
}

func TestSplitReplicationTaskGroupByMaxBytesReusesArenaRecordRange(t *testing.T) {
	arena := newReplicationSyncPayloadArena(3)
	for idx, key := range []string{"session:1", "session:2", "session:3"} {
		if _, err := arena.append(key, []byte("binary-"+key), 90); err != nil {
			t.Fatalf("arena append %d: %v", idx, err)
		}
	}
	group := replicationTaskGroup{
		target:                 TopologyNode{ID: "node-b", Address: "http://node-b"},
		syncPayloadArena:       arena,
		syncPayloadRecordCount: 3,
		deferredMetadata:       true,
		metadataSource:         "node-a",
		metadataTopology:       "fingerprint-a",
	}

	groups := splitReplicationTaskGroupByMaxBytes(group, 160)
	if len(groups) != len(arena.records) {
		t.Fatalf("split groups len = %d, want %d", len(groups), len(arena.records))
	}
	for idx := range groups {
		split := groups[idx]
		if split.syncPayloadArena != arena || split.syncPayloadRecordStart != uint32(idx) || split.syncPayloadRecordCount != 1 || len(split.syncPayloadIndexes) != 0 {
			t.Fatalf("split group %d range = %#v, want shared arena record %d", idx, split, idx)
		}
		payload := split.replicationSyncPayloadBatch().payload(0)
		if payload.key != "session:"+strconv.Itoa(idx+1) {
			t.Fatalf("split group %d key = %q, want session:%d", idx, payload.key, idx+1)
		}
	}
}

func TestSplitReplicationTaskGroupByMaxBytesReusesDirectArenaRecordRange(t *testing.T) {
	arena := newReplicationSyncPayloadDirectArena(3)
	for idx, key := range []string{"session:1", "session:2", "session:3"} {
		valueOffset := len(arena.values)
		value := "binary-" + key
		arena.values = append(arena.values, value...)
		if err := arena.appendDirectRecord(key, valueOffset, len(arena.values)-valueOffset); err != nil {
			t.Fatalf("direct arena append %d: %v", idx, err)
		}
	}
	group := replicationTaskGroup{
		target:                 TopologyNode{ID: "node-b", Address: "http://node-b"},
		syncPayloadArena:       arena,
		syncPayloadRecordCount: 3,
		deferredMetadata:       true,
		metadataSource:         "node-a",
		metadataTopology:       "fingerprint-a",
	}

	groups := splitReplicationTaskGroupByMaxBytes(group, 40)
	if len(groups) != len(arena.directRecords) {
		t.Fatalf("split groups len = %d, want %d", len(groups), len(arena.directRecords))
	}
	for idx := range groups {
		split := groups[idx]
		if split.syncPayloadArena != arena || split.syncPayloadRecordStart != uint32(idx) || split.syncPayloadRecordCount != 1 || len(split.syncPayloadIndexes) != 0 {
			t.Fatalf("split group %d range = %#v, want shared direct arena record %d", idx, split, idx)
		}
		payload := split.replicationSyncPayloadBatch().payload(0)
		if payload.key != "session:"+strconv.Itoa(idx+1) || string(payload.binaryValue) != "binary-"+payload.key {
			t.Fatalf("split group %d payload = %#v, want matching direct arena value", idx, payload)
		}
	}
}

func TestHTTPReplicatorSyncAllFullReplicaReplicatesToRemoteOwners(t *testing.T) {
	type targetRequest struct {
		node    string
		request CacheCommandRequest
	}
	requests := make(chan targetRequest, 2)
	newTarget := func(node string) *httptest.Server {
		t.Helper()
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/commands" {
				t.Fatalf("path = %s, want /api/commands", r.URL.Path)
			}
			request := mustDecodeReplicationTestCommand(t, w, r)
			if rejectReplicationDigestTestCommand(t, w, r, request) {
				return
			}
			requests <- targetRequest{
				node:    node,
				request: request,
			}
			writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
		}))
	}
	targetB := newTarget("node-b")
	defer targetB.Close()
	targetC := newTarget("node-c")
	defer targetC.Close()

	trie := newTestTrie(t)
	trie.UpsertString("session:1", "value-1")
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Mode:    TopologyModeFullReplica,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://127.0.0.1:1"},
			{ID: "node-b", Address: targetB.URL},
			{ID: "node-c", Address: targetC.URL},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore(full replica) error = %v", err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Client:   targetB.Client(),
	})

	result := replicator.SyncAll(context.Background(), trie, "session:")
	if result.Skipped || result.Command != "SYNC" || result.Key != "session:" || result.Entries != 1 || len(result.Targets) != 2 {
		t.Fatalf("full-replica sync result = %#v, want one entry replicated to two owners", result)
	}
	if result.Targets[0].Node != "node-b" || result.Targets[1].Node != "node-c" {
		t.Fatalf("full-replica sync targets = %#v, want node-b then node-c", result.Targets)
	}
	for _, target := range result.Targets {
		if !target.OK || target.Key != "session:1" || target.Node == "node-a" {
			t.Fatalf("full-replica sync target = %#v, want ok remote owner for session:1", target)
		}
	}

	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		targetRequest := <-requests
		if !isTypedReplicationSetPayload(targetRequest.request, "session:1") {
			t.Fatalf("full-replica sync request = %#v, want typed binary session snapshot", targetRequest.request)
		}
		seen[targetRequest.node] = true
	}
	if !seen["node-b"] || !seen["node-c"] || seen["node-a"] {
		t.Fatalf("full-replica sync request nodes = %#v, want only remote owners", seen)
	}
	select {
	case targetRequest := <-requests:
		t.Fatalf("unexpected full-replica sync request = %#v", targetRequest)
	default:
	}
}

func TestHTTPReplicatorSyncAllPagesLeaderOwnedEntries(t *testing.T) {
	requests := make(chan CacheCommandRequest, 3)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		if rejectReplicationDigestTestCommand(t, w, r, request) {
			return
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	trie.UpsertString("session:1", "value-1")
	trie.UpsertString("session:2", "value-2")
	trie.UpsertString("session:3", "value-3")
	trie.UpsertString("other:1", "ignored")
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	result := replicator.syncAllPaged(context.Background(), trie, "session:", 2)
	if result.Skipped || result.Entries != 3 || len(result.Targets) != 1 {
		t.Fatalf("paged sync result = %#v, want one coalesced target request for three synced entries", result)
	}
	for _, target := range result.Targets {
		if !target.OK {
			t.Fatalf("paged sync target = %#v, want ok target", target)
		}
	}
	targetKeys := map[string]bool{}
	batches := 0
	singles := 0
	for i := 0; i < 1; i++ {
		request := <-requests
		switch request.Command {
		case "INTERNALBATCH", replicationBatchEnvelopeCommand:
			batches++
			payloads := mustDecodeReplicationBatchValues(t, request)
			if len(payloads) != 3 {
				t.Fatalf("paged sync batch len = %d, want 3: %#v", len(payloads), payloads)
			}
		case replicationSetBinaryCommand, replicationSetCompactCommand:
			singles++
			if !isTypedReplicationSetPayload(request, request.Key) {
				t.Fatalf("paged sync single request = %#v, want typed binary snapshot", request)
			}
		default:
			t.Fatalf("paged sync request = %#v, want internal replication request", request)
		}
		for _, key := range replicationRequestKeys(t, request) {
			if !strings.HasPrefix(key, "session:") {
				t.Fatalf("paged sync request key = %q, want session key", key)
			}
			targetKeys[key] = true
		}
	}
	if batches != 1 || singles != 0 {
		t.Fatalf("paged sync request shape = %d batches %d singles, want one coalesced batch", batches, singles)
	}
	for _, key := range []string{"session:1", "session:2", "session:3"} {
		if !targetKeys[key] {
			t.Fatalf("paged sync request keys = %#v, missing %s", targetKeys, key)
		}
	}
	select {
	case request := <-requests:
		t.Fatalf("unexpected paged sync request = %#v", request)
	default:
	}
}

func TestAppendCommandDumpEntryBinaryWithoutStatsUsesDestination(t *testing.T) {
	for _, tt := range []struct {
		name       string
		partitions int
	}{
		{name: "root trie"},
		{name: "local partitions", partitions: 8},
	} {
		t.Run(tt.name, func(t *testing.T) {
			trie := newTestTrie(t)
			if tt.partitions > 0 {
				if err := trie.ConfigureLocalPartitions(tt.partitions); err != nil {
					t.Fatalf("ConfigureLocalPartitions() error = %v", err)
				}
			}
			trie.UpsertString("session:1", "one")
			want, exists, err := trie.commandDumpEntryBinaryWithoutStats("session:1")
			if err != nil || !exists {
				t.Fatalf("commandDumpEntryBinaryWithoutStats() = %v/%v, want value", exists, err)
			}

			prefix := []byte("existing:")
			destination := make([]byte, len(prefix), len(prefix)+len(want)+64)
			copy(destination, prefix)
			got, exists, err := trie.appendCommandDumpEntryBinaryWithoutStats(destination, "session:1")
			if err != nil || !exists {
				t.Fatalf("appendCommandDumpEntryBinaryWithoutStats() = %v/%v, want value", exists, err)
			}
			if !bytes.Equal(got[:len(prefix)], prefix) || !bytes.Equal(got[len(prefix):], want) {
				t.Fatalf("appended dump = %x, want prefix %x and value %x", got, prefix, want)
			}

			missing, exists, err := trie.appendCommandDumpEntryBinaryWithoutStats(destination, "session:missing")
			if err != nil || exists {
				t.Fatalf("missing append = %v/%v, want false/nil", exists, err)
			}
			if !bytes.Equal(missing, destination) {
				t.Fatalf("missing append changed destination = %x, want %x", missing, destination)
			}
		})
	}
}

func TestPrepareReplicationDigestTaskGroupSelectsCompactRepresentation(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("session:1", "one")
	trie.UpsertString("session:2", "two")
	target := TopologyNode{ID: "node-b", Address: "http://node-b"}
	allSet := []replicationDigestChange{{key: "session:1"}, {key: "session:2"}}
	mixed := []replicationDigestChange{{key: "session:1"}, {key: "session:stale", delete: true}}

	for _, tt := range []struct {
		name          string
		wireFormat    CommandWireFormat
		grpcAvailable bool
		changes       []replicationDigestChange
		wantCompact   bool
		wantDeleted   int
	}{
		{name: "default protobuf", changes: allSet, wantCompact: true},
		{name: "grpc with json fallback", wireFormat: CommandWireFormatJSON, grpcAvailable: true, changes: allSet, wantCompact: true},
		{name: "json compatibility", wireFormat: CommandWireFormatJSON, changes: allSet},
		{name: "mixed delete compatibility", changes: mixed, wantDeleted: 1},
		{name: "unexpected missing key compatibility", changes: []replicationDigestChange{{key: "session:1"}, {key: "session:stale"}}, wantDeleted: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a", WireFormat: tt.wireFormat})
			t.Cleanup(replicator.Close)
			group, changed, deleted, err := replicator.prepareReplicationDigestTaskGroup(
				trie, target, tt.changes, tt.grpcAvailable, "fingerprint-a",
			)
			if err != nil {
				t.Fatalf("prepareReplicationDigestTaskGroup() error = %v", err)
			}
			if changed != len(tt.changes)-tt.wantDeleted || deleted != tt.wantDeleted {
				t.Fatalf("changed/deleted = %d/%d, want %d/%d", changed, deleted, len(tt.changes)-tt.wantDeleted, tt.wantDeleted)
			}
			if !group.deferredMetadata || group.metadataSource != "node-a" || group.metadataTopology != "fingerprint-a" {
				t.Fatalf("group metadata = %#v, want deferred source metadata", group)
			}
			if tt.wantCompact {
				batch := group.replicationSyncPayloadBatch()
				if group.syncPayloadArena == nil || group.syncPayloadRecordStart != 0 || group.syncPayloadRecordCount != uint32(len(tt.changes)) || len(group.syncPayloadIndexes) != 0 || len(group.syncPayloads) != 0 || len(group.payloads) != 0 || len(group.keys) != 0 {
					t.Fatalf("compact group storage = %v/%d/%d/%d/%d/%d/%d, want arena/0/%d/0/0/0/0", group.syncPayloadArena != nil, group.syncPayloadRecordStart, group.syncPayloadRecordCount, len(group.syncPayloadIndexes), len(group.syncPayloads), len(group.payloads), len(group.keys), len(tt.changes))
				}
				if batch.len() != len(tt.changes) {
					t.Fatalf("compact batch length = %d, want %d", batch.len(), len(tt.changes))
				}
				if len(group.syncPayloadArena.directRecords) != len(tt.changes) || len(group.syncPayloadArena.keys) != 0 || len(group.syncPayloadArena.records) != 0 {
					t.Fatalf("compact arena storage = %d direct/%d keys/%d indexed, want %d/0/0", len(group.syncPayloadArena.directRecords), len(group.syncPayloadArena.keys), len(group.syncPayloadArena.records), len(tt.changes))
				}
				for index := 0; index < batch.len(); index++ {
					payload := batch.payload(index)
					if payload.key != tt.changes[index].key || len(payload.binaryValue) == 0 {
						t.Fatalf("compact payload %d = %#v, want populated %q", index, payload, tt.changes[index].key)
					}
				}
				return
			}
			if len(group.syncPayloads) != 0 || len(group.payloads) != len(tt.changes) || len(group.keys) != len(tt.changes) {
				t.Fatalf("generic group storage = %d/%d/%d, want 0/%d/%d", len(group.syncPayloads), len(group.payloads), len(group.keys), len(tt.changes), len(tt.changes))
			}
			commands := make(map[string]string, len(group.payloads))
			for _, payload := range group.payloads {
				commands[payload.Key] = normalizedCommand(payload.Command)
			}
			if commands["session:1"] != replicationSetCompactCommand {
				t.Fatalf("session:1 command = %q, want compact set", commands["session:1"])
			}
			if tt.wantDeleted == 1 && commands["session:stale"] != "INTERNALDEL" {
				t.Fatalf("stale command = %q, want INTERNALDEL", commands["session:stale"])
			}
		})
	}
}

func TestPrepareReplicationDigestPackedTaskGroupMatchesScalarWire(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("session:1", "one")
	trie.UpsertString("session:2", "two")
	target := TopologyNode{ID: "node-b", Address: "http://node-b"}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a"})
	t.Cleanup(replicator.Close)

	changes := []replicationDigestChange{{key: "session:1"}, {key: "session:2"}}
	scalar, scalarChanged, scalarDeleted, err := replicator.prepareReplicationDigestTaskGroup(
		trie, target, changes, false, "fingerprint-a",
	)
	if err != nil {
		t.Fatalf("prepare scalar task group: %v", err)
	}
	arena := newReplicationSyncPayloadArena(len(changes))
	for _, change := range changes {
		if err := arena.appendKey(change.key); err != nil {
			t.Fatalf("append packed key: %v", err)
		}
	}
	packed, packedChanged, packedDeleted, nativeBatches, err := replicator.prepareReplicationDigestPackedTaskGroup(
		trie, target, arena, "fingerprint-a",
	)
	if err != nil {
		t.Fatalf("prepare packed task group: %v", err)
	}
	if scalarChanged != packedChanged || scalarDeleted != packedDeleted || nativeBatches != 1 {
		t.Fatalf("scalar/packed changed/deleted/batches = %d/%d and %d/%d/%d", scalarChanged, scalarDeleted, packedChanged, packedDeleted, nativeBatches)
	}
	scalarWire := appendReplicationSyncBatchProtoBatch(nil, scalar.replicationSyncPayloadBatch(), replicationSetCompactCommand, "node-a", 42, "fingerprint-a")
	packedWire := appendReplicationSyncBatchProtoBatch(nil, packed.replicationSyncPayloadBatch(), replicationSetCompactCommand, "node-a", 42, "fingerprint-a")
	if !bytes.Equal(packedWire, scalarWire) {
		t.Fatalf("packed wire differs from scalar\npacked: %x\nscalar: %x", packedWire, scalarWire)
	}
}

func TestPrepareReplicationDigestPackedTaskGroupMatchesScalarDeleteFallback(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("session:1", "one")
	trie.UpsertString("session:2", "two")
	target := TopologyNode{ID: "node-b", Address: "http://node-b"}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a"})
	t.Cleanup(replicator.Close)
	arena := newReplicationSyncPayloadArena(2)
	for _, key := range []string{"session:1", "session:2"} {
		if err := arena.appendKey(key); err != nil {
			t.Fatalf("append packed key: %v", err)
		}
	}
	trie.Delete("session:2")

	packed, packedChanged, packedDeleted, _, err := replicator.prepareReplicationDigestPackedTaskGroup(
		trie, target, arena, "fingerprint-a",
	)
	if err != nil {
		t.Fatalf("prepare packed task group: %v", err)
	}
	scalar, scalarChanged, scalarDeleted, err := replicator.prepareReplicationDigestTaskGroup(
		trie, target, []replicationDigestChange{{key: "session:1"}, {key: "session:2"}}, false, "fingerprint-a",
	)
	if err != nil {
		t.Fatalf("prepare scalar task group: %v", err)
	}
	if scalarChanged != packedChanged || scalarDeleted != packedDeleted {
		t.Fatalf("scalar/packed changed/deleted = %d/%d and %d/%d", scalarChanged, scalarDeleted, packedChanged, packedDeleted)
	}
	if !reflect.DeepEqual(packed.payloads, scalar.payloads) || !reflect.DeepEqual(packed.keys, scalar.keys) {
		t.Fatalf("packed delete fallback = %#v/%#v, want scalar %#v/%#v", packed.payloads, packed.keys, scalar.payloads, scalar.keys)
	}
}

func TestReplicationDigestKeySourceIteratorSkipsValueDigest(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("session:1", "one")
	topology := replicationTestTopology(t, "http://node-b")
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: NewElectionStore(topology, ElectionOptions{}),
	})
	t.Cleanup(replicator.Close)
	routing, ok := replicator.snapshotReplicationRouting()
	if !ok {
		t.Fatal("snapshotReplicationRouting() ok = false")
	}
	inventory := replicationDigestTargetInventory{
		target:   routing.nodes["node-b"],
		prefix:   "session:",
		pageSize: 10,
	}

	keySource := newReplicationDigestKeySourceIterator(context.Background(), trie, routing, "node-a", inventory)
	keyEntry, ok, err := keySource.next()
	keySource.close()
	if err != nil || !ok || keyEntry.key != "session:1" {
		t.Fatalf("key source entry = %#v/%v/%v, want session:1", keyEntry, ok, err)
	}
	if keyEntry.digest != (replicationDigest{}) {
		t.Fatalf("key source digest = %#v, want zero digest without value serialization", keyEntry.digest)
	}

	digestSource := newReplicationDigestSourceIterator(context.Background(), trie, routing, "node-a", inventory)
	digestEntry, ok, err := digestSource.next()
	digestSource.close()
	if err != nil || !ok || digestEntry.key != keyEntry.key {
		t.Fatalf("digest source entry = %#v/%v/%v, want session:1", digestEntry, ok, err)
	}
	if digestEntry.digest == (replicationDigest{}) {
		t.Fatal("digest source returned zero digest, want serialized value digest")
	}
}

func TestReplicationDigestKeySourceAppendsFallbackChangesDirectly(t *testing.T) {
	trie := newTestTrie(t)
	for _, key := range []string{"other:1", "session:1", "session:2", "session:3"} {
		trie.UpsertString(key, "value-"+key)
	}
	topology := replicationTestTopology(t, "http://node-b")
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: NewElectionStore(topology, ElectionOptions{}),
	})
	t.Cleanup(replicator.Close)
	routing, ok := replicator.snapshotReplicationRouting()
	if !ok {
		t.Fatal("snapshotReplicationRouting() ok = false")
	}
	inventory := replicationDigestTargetInventory{
		target:   routing.nodes["node-b"],
		prefix:   "session:",
		pageSize: 2,
	}
	source := newReplicationDigestKeySourceIterator(context.Background(), trie, routing, "node-a", inventory)
	defer source.close()

	changes, done, err := source.appendFallbackChanges(nil, 2)
	if err != nil || done {
		t.Fatalf("first appendFallbackChanges() done/error = %v/%v, want false/nil", done, err)
	}
	if got := replicationDigestChangeKeys(changes); !reflect.DeepEqual(got, []string{"session:1", "session:2"}) {
		t.Fatalf("first fallback changes = %#v, want session:1/session:2", got)
	}
	if cap(source.entries) != 0 {
		t.Fatalf("source intermediate entry capacity = %d, want no intermediate allocation", cap(source.entries))
	}

	changes, done, err = source.appendFallbackChanges(changes, 2)
	if err != nil || !done {
		t.Fatalf("second appendFallbackChanges() done/error = %v/%v, want true/nil", done, err)
	}
	if got := replicationDigestChangeKeys(changes); !reflect.DeepEqual(got, []string{"session:1", "session:2", "session:3"}) {
		t.Fatalf("all fallback changes = %#v, want three ordered session keys", got)
	}
	if cap(source.entries) != 0 {
		t.Fatalf("source intermediate entry capacity after completion = %d, want zero", cap(source.entries))
	}
}

func TestReplicationDigestSourceIteratorPrevalidatesInvariantScope(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("session:1", "one")
	oneShardTopology := replicationTestTopology(t, "http://node-b")
	oneShardReplicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: oneShardTopology,
		Election: NewElectionStore(oneShardTopology, ElectionOptions{}),
	})
	t.Cleanup(oneShardReplicator.Close)
	oneShardRouting, ok := oneShardReplicator.snapshotReplicationRouting()
	if !ok {
		t.Fatal("one-shard snapshotReplicationRouting() ok = false")
	}

	for _, test := range []struct {
		name      string
		inventory replicationDigestTargetInventory
		wantMode  bool
		wantEntry bool
	}{
		{
			name: "entire one-shard scope",
			inventory: replicationDigestTargetInventory{
				target: oneShardRouting.nodes["node-b"], prefix: "session:", pageSize: 10,
			},
			wantMode: true, wantEntry: true,
		},
		{
			name: "bucket-filtered scope",
			inventory: replicationDigestTargetInventory{
				target: oneShardRouting.nodes["node-b"], prefix: "session:", pageSize: 10, hasBuckets: true,
			},
		},
		{
			name: "different target",
			inventory: replicationDigestTargetInventory{
				target: TopologyNode{ID: "node-c", Address: "http://node-c"}, prefix: "session:", pageSize: 10,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			iterator := newReplicationDigestKeySourceIterator(context.Background(), trie, oneShardRouting, "node-a", test.inventory)
			defer iterator.close()
			iterator.prevalidateScope()
			got := iterator.mode&replicationDigestSourceInvariantScope != 0
			if got != test.wantMode {
				t.Fatalf("invariant scope mode = %v, want %v", got, test.wantMode)
			}
			entry, found, err := iterator.next()
			if err != nil || found != test.wantEntry {
				t.Fatalf("iterator.next() = %#v/%v/%v, want found %v", entry, found, err, test.wantEntry)
			}
		})
	}

	multiShardTopology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a"},
			{ID: "node-b", Address: "http://node-b"},
		},
		Shards: []TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}},
			{ID: 1, Primary: "node-a", Replicas: []string{"node-b"}},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore(multi-shard): %v", err)
	}
	multiShardReplicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: multiShardTopology,
		Election: NewElectionStore(multiShardTopology, ElectionOptions{}),
	})
	t.Cleanup(multiShardReplicator.Close)
	multiShardRouting, ok := multiShardReplicator.snapshotReplicationRouting()
	if !ok {
		t.Fatal("multi-shard snapshotReplicationRouting() ok = false")
	}
	inventory := replicationDigestTargetInventory{
		target: multiShardRouting.nodes["node-b"], prefix: "session:", pageSize: 10,
	}
	iterator := newReplicationDigestKeySourceIterator(context.Background(), trie, multiShardRouting, "node-a", inventory)
	defer iterator.close()
	iterator.prevalidateScope()
	if iterator.mode&replicationDigestSourceInvariantScope != 0 {
		t.Fatal("multi-shard scopeIsInvariant = true, want dynamic per-key routing")
	}
	entry, found, err := iterator.next()
	if err != nil || !found || entry.key != "session:1" {
		t.Fatalf("multi-shard iterator.next() = %#v/%v/%v, want session:1/true/nil", entry, found, err)
	}
}

func replicationDigestChangeKeys(changes []replicationDigestChange) []string {
	keys := make([]string, len(changes))
	for index := range changes {
		keys[index] = changes[index].key
	}
	return keys
}

func TestReplicationRoutingSnapshotMatchesDynamicRouting(t *testing.T) {
	topology, err := NewTopologyStore(ClusterTopology{
		Version:     1,
		Self:        "node-a",
		BucketCount: 16,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a"},
			{ID: "node-b", Address: "http://node-b"},
			{ID: "node-c", Address: "http://node-c"},
		},
		Shards: []TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b", "node-c"}},
			{ID: 1, Primary: "node-b", Replicas: []string{"node-a", "node-c"}},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	election := NewElectionStore(topology, ElectionOptions{})
	if err := election.MarkOffline("node-c"); err != nil {
		t.Fatalf("MarkOffline(node-c) error = %v", err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
	})

	snapshot, ok := replicator.snapshotReplicationRouting()
	if !ok {
		t.Fatal("snapshotReplicationRouting() ok = false, want routing snapshot")
	}
	for _, key := range []string{keyForShard(t, topology, 0), keyForShard(t, topology, 1)} {
		wantRoute, wantOK := replicator.routeForKey(key)
		gotRoute, gotOK := snapshot.routeForKey(key)
		if gotOK != wantOK || !reflect.DeepEqual(gotRoute, wantRoute) {
			t.Fatalf("snapshot route %q = %#v/%v, want %#v/%v", key, gotRoute, gotOK, wantRoute, wantOK)
		}
		gotTargets := snapshot.replicationTargets(gotRoute, replicator.self)
		wantTargets := replicator.replicationTargets(wantRoute)
		if !reflect.DeepEqual(gotTargets, wantTargets) {
			t.Fatalf("snapshot targets %q = %#v, want %#v", key, gotTargets, wantTargets)
		}
	}
	if snapshot.fingerprint == "" || snapshot.fingerprint != topology.Fingerprint() {
		t.Fatalf("snapshot fingerprint = %q, want %q", snapshot.fingerprint, topology.Fingerprint())
	}
}

func TestReplicationRoutingSnapshotReusesPrecomputedTargets(t *testing.T) {
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a"},
			{ID: "node-b", Address: "http://node-b"},
			{ID: "node-c", Address: "http://node-c"},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-c", "node-b"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a", Topology: topology})
	snapshot, ok := replicator.snapshotReplicationRouting()
	if !ok {
		t.Fatal("snapshotReplicationRouting() ok = false")
	}
	route, ok := snapshot.routeForKey("session:1")
	if !ok {
		t.Fatal("routeForKey() ok = false")
	}

	first := snapshot.replicationTargets(route, replicator.self)
	second := snapshot.replicationTargets(route, replicator.self)
	if len(first) != 2 || first[0].ID != "node-b" || first[1].ID != "node-c" {
		t.Fatalf("precomputed targets = %#v, want node-b then node-c", first)
	}
	if &first[0] != &second[0] {
		t.Fatalf("target backing arrays differ: %p != %p, want immutable snapshot reuse", &first[0], &second[0])
	}
}

func TestReplicationSyncPayloadGroupsStayCompactWhenSplit(t *testing.T) {
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a", ReplicationBatchMaxBytes: -1})
	targets := []TopologyNode{
		{ID: "node-b", Address: "http://node-b"},
		{ID: "node-c", Address: "http://node-c"},
	}
	groups := make([]replicationTaskGroup, 0, len(targets))
	indexes := make(map[TopologyNode]int, len(targets))
	for _, key := range []string{"session:1", "session:2", "session:3"} {
		groups = replicator.appendReplicationSyncPayloadToTargetGroups(
			groups, indexes, 3, targets, key, []byte("binary-"+key), "fingerprint-a",
		)
	}
	if len(groups) != len(targets) {
		t.Fatalf("groups = %d, want %d targets", len(groups), len(targets))
	}
	for idx, group := range groups {
		if len(group.payloads) != 0 || len(group.keys) != 0 || len(group.payloadBytes) != 0 {
			t.Fatalf("group %d generic storage = %d/%d/%d, want empty", idx, len(group.payloads), len(group.keys), len(group.payloadBytes))
		}
		if len(group.syncPayloads) != 3 || !group.deferredMetadata || group.metadataSource != "node-a" || group.metadataTopology != "fingerprint-a" {
			t.Fatalf("group %d = %#v, want three compact deferred payloads", idx, group)
		}
		for payloadIdx, payload := range group.syncPayloads {
			if payload.payloadBytes != 0 {
				t.Fatalf("group %d payload %d estimated bytes = %d, want disabled without byte cap", idx, payloadIdx, payload.payloadBytes)
			}
		}
	}

	split := splitReplicationTaskGroupByMaxBytes(groups[0], 180)
	if len(split) < 2 {
		t.Fatalf("split groups = %d, want byte cap to create multiple groups", len(split))
	}
	total := 0
	for idx, group := range split {
		total += len(group.syncPayloads)
		if len(group.payloads) != 0 || !group.deferredMetadata || group.metadataSource != "node-a" || group.metadataTopology != "fingerprint-a" {
			t.Fatalf("split group %d = %#v, want compact metadata-preserving group", idx, group)
		}
	}
	if total != 3 {
		t.Fatalf("split payload total = %d, want 3", total)
	}
}

func TestReplicationSyncArenaIndexesPayloadsAcrossTargetGroups(t *testing.T) {
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a"})
	targets := []TopologyNode{
		{ID: "node-b", Address: "http://node-b"},
		{ID: "node-c", Address: "http://node-c"},
	}
	arena := newReplicationSyncPayloadArena(2)
	groups := make([]replicationTaskGroup, 0, len(targets))
	indexes := make(map[TopologyNode]int, len(targets))
	var err error
	groups, err = replicator.appendReplicationSyncArenaPayloadToTargetGroups(
		groups, indexes, 2, targets, arena, "session:1", []byte("binary-one"), "fingerprint-a",
	)
	if err != nil {
		t.Fatalf("append first arena payload: %v", err)
	}
	groups, err = replicator.appendReplicationSyncArenaPayloadToTargetGroups(
		groups, indexes, 2, targets, arena, "session:2", []byte("binary-two"), "fingerprint-a",
	)
	if err != nil {
		t.Fatalf("append second arena payload: %v", err)
	}

	if len(arena.records) != 2 || len(groups) != 2 {
		t.Fatalf("arena records/groups = %d/%d, want 2/2", len(arena.records), len(groups))
	}
	for idx, group := range groups {
		if len(group.syncPayloads) != 0 || group.syncPayloadArena != arena {
			t.Fatalf("group %d inline payloads/arena = %d/%p, want 0/%p", idx, len(group.syncPayloads), group.syncPayloadArena, arena)
		}
		if !reflect.DeepEqual(group.syncPayloadIndexes, []uint32{0, 1}) {
			t.Fatalf("group %d indexes = %#v, want [0 1]", idx, group.syncPayloadIndexes)
		}
		batch := group.replicationSyncPayloadBatch()
		if batch.len() != 2 {
			t.Fatalf("group %d batch len = %d, want 2", idx, batch.len())
		}
		for payloadIdx, want := range []replicationSyncPayload{
			{key: "session:1", binaryValue: []byte("binary-one")},
			{key: "session:2", binaryValue: []byte("binary-two")},
		} {
			got := batch.payload(payloadIdx)
			if got.key != want.key || !bytes.Equal(got.binaryValue, want.binaryValue) {
				t.Fatalf("group %d payload %d = %#v, want %#v", idx, payloadIdx, got, want)
			}
		}
	}

	split := splitReplicationTaskGroupByMaxBytes(groups[0], 180)
	if len(split) != 2 || split[0].replicationSyncPayloadBatch().len() != 1 || split[1].replicationSyncPayloadBatch().len() != 1 {
		t.Fatalf("split arena groups = %#v, want two one-payload groups", split)
	}
	for idx, group := range split {
		if group.syncPayloadArena != arena || len(group.syncPayloads) != 0 {
			t.Fatalf("split group %d lost arena ownership: %#v", idx, group)
		}
	}

	body, contentType, contentEncoding, err := replicationSyncBatchRequestBodyBatch(
		groups[0].replicationSyncPayloadBatch(), replicationSetCompactCommand, "node-a", 42, "fingerprint-a", 0,
	)
	if err != nil {
		t.Fatalf("replicationSyncBatchRequestBodyBatch() error = %v", err)
	}
	if contentType != commandWireContentTypeProtobuf || contentEncoding != "" {
		t.Fatalf("arena content type/encoding = %q/%q, want protobuf/uncompressed", contentType, contentEncoding)
	}
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("ReadAll(arena body) error = %v", err)
	}
	decoded, err := decodeCommandRequestProto(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("decode arena protobuf: %v", err)
	}
	if len(decoded.Batch) != 2 || decoded.Batch[0].Key != "session:1" || decoded.Batch[1].Key != "session:2" {
		t.Fatalf("decoded arena batch = %#v, want both indexed payloads", decoded.Batch)
	}
}

func TestReplicationSyncArenaCapsEagerAllocation(t *testing.T) {
	arena := newReplicationSyncPayloadArena(math.MaxInt)
	if cap(arena.records) != maxReplicationSyncArenaInitialEntries {
		t.Fatalf("record capacity = %d, want capped %d", cap(arena.records), maxReplicationSyncArenaInitialEntries)
	}
	if cap(arena.keys) != maxReplicationSyncArenaInitialEntries*12 || cap(arena.values) != maxReplicationSyncArenaInitialEntries*24 {
		t.Fatalf("key/value capacity = %d/%d, want bounded estimates", cap(arena.keys), cap(arena.values))
	}

	directArena := newReplicationSyncPayloadDirectArena(math.MaxInt)
	if cap(directArena.directRecords) != maxReplicationSyncArenaInitialEntries || cap(directArena.values) != maxReplicationSyncArenaInitialEntries*24 {
		t.Fatalf("direct record/value capacity = %d/%d, want bounded estimates", cap(directArena.directRecords), cap(directArena.values))
	}
	if directArena.keys != nil || directArena.records != nil {
		t.Fatalf("direct arena allocated copied-key/indexed-record storage: %#v", directArena)
	}
}

func TestReplicationSyncEntriesPageAdvancesAfterEmptyKey(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("", "empty")
	trie.UpsertString("session:1", "value")

	var keys []string
	page, err := replicationSyncEntriesPage(trie, "", "", false, 1, func(entry Entry) error {
		keys = append(keys, entry.Key)
		return nil
	})
	if err != nil {
		t.Fatalf("replicationSyncEntriesPage(first) error = %v", err)
	}
	if page.scanned != 1 || !reflect.DeepEqual(keys, []string{""}) || !page.hasMore || page.nextAfterKey != "" {
		t.Fatalf("first page = %#v, want empty key with more entries", page)
	}

	keys = keys[:0]
	page, err = replicationSyncEntriesPage(trie, "", page.nextAfterKey, true, 1, func(entry Entry) error {
		keys = append(keys, entry.Key)
		return nil
	})
	if err != nil {
		t.Fatalf("replicationSyncEntriesPage(second) error = %v", err)
	}
	if page.scanned != 1 || !reflect.DeepEqual(keys, []string{"session:1"}) || page.hasMore || page.nextAfterKey != "session:1" {
		t.Fatalf("second page = %#v, want session key without more entries", page)
	}
}

func TestReplicationSyncCursorVisitsEachEntryOnceAcrossPages(t *testing.T) {
	trie := newTestTrie(t)
	for idx := 0; idx < 10; idx++ {
		trie.UpsertString("session:"+strconv.Itoa(idx), "value")
	}

	cursor := &replicationSyncCursor{packedKeys: true}
	defer cursor.close(trie)
	afterKey := ""
	hasAfterKey := false
	var keys []string
	for {
		page, err := replicationSyncEntriesPageWithCursor(trie, "session:", afterKey, hasAfterKey, 3, cursor, func(entry Entry) error {
			keys = append(keys, entry.Key)
			return nil
		})
		if err != nil {
			t.Fatalf("replicationSyncEntriesPageWithCursor() error = %v", err)
		}
		if !page.hasMore {
			break
		}
		afterKey = page.nextAfterKey
		hasAfterKey = true
	}
	if want := []string{"session:0", "session:1", "session:2", "session:3", "session:4", "session:5", "session:6", "session:7", "session:8", "session:9"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("cursor keys = %#v, want %#v", keys, want)
	}
	if cursor.visited != len(keys) || cursor.restarts != 0 {
		t.Fatalf("cursor visits/restarts = %d/%d, want %d/0", cursor.visited, cursor.restarts, len(keys))
	}
}

func TestReplicationSyncReadOnlyCursorAllowsConcurrentReads(t *testing.T) {
	trie := newTestTrie(t)
	for index := 0; index < 300; index++ {
		trie.UpsertString(fmt.Sprintf("session:%03d", index), "value")
	}

	cursor := &replicationSyncCursor{packedKeys: true, sharedReadOnly: true}
	defer cursor.close(trie)
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	scanDone := make(chan error, 1)
	go func() {
		first := true
		_, err := replicationSyncEntriesPageWithCursor(trie, "session:", "", false, 300, cursor, func(Entry) error {
			if first {
				first = false
				close(entered)
				<-release
			}
			return nil
		})
		scanDone <- err
	}()
	<-entered

	readDone := make(chan string, 1)
	go func() { readDone <- trie.GetString("session:000") }()
	select {
	case got := <-readDone:
		if got != "value" {
			t.Fatalf("concurrent read = %q, want value", got)
		}
	case <-time.After(250 * time.Millisecond):
		releaseOnce.Do(func() { close(release) })
		<-scanDone
		t.Fatal("concurrent read blocked behind read-only replication scan")
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-scanDone; err != nil {
		t.Fatalf("read-only replication scan error = %v", err)
	}
}

func TestReplicationSyncReadOnlyCursorStillBlocksWriters(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("session:scan", "value")

	cursor := &replicationSyncCursor{packedKeys: true, sharedReadOnly: true}
	defer cursor.close(trie)
	entered := make(chan struct{})
	release := make(chan struct{})
	scanDone := make(chan error, 1)
	go func() {
		_, err := replicationSyncEntriesPageWithCursor(trie, "session:", "", false, 1, cursor, func(Entry) error {
			close(entered)
			<-release
			return nil
		})
		scanDone <- err
	}()
	<-entered

	writeDone := make(chan struct{})
	go func() {
		trie.UpsertString("writer:key", "value")
		close(writeDone)
	}()
	select {
	case <-writeDone:
		close(release)
		<-scanDone
		t.Fatal("writer completed before the consistent replication scan released its lock")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-scanDone; err != nil {
		t.Fatalf("read-only replication scan error = %v", err)
	}
	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("writer remained blocked after the replication scan completed")
	}
}

func TestReplicationSyncReadOnlyCursorWithTTLRetainsExclusivePath(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("session:scan", "value")
	trie.UpsertString("session:read", "value")
	if !trie.Expire("session:scan", time.Hour) {
		t.Fatal("Expire(session:scan) = false")
	}

	cursor := &replicationSyncCursor{packedKeys: true, sharedReadOnly: true}
	defer cursor.close(trie)
	entered := make(chan struct{})
	release := make(chan struct{})
	scanDone := make(chan error, 1)
	go func() {
		_, err := replicationSyncEntriesPageWithCursor(trie, "session:", "", false, 2, cursor, func(Entry) error {
			select {
			case <-entered:
			default:
				close(entered)
				<-release
			}
			return nil
		})
		scanDone <- err
	}()
	<-entered

	readDone := make(chan struct{})
	go func() {
		_ = trie.GetString("session:read")
		close(readDone)
	}()
	select {
	case <-readDone:
		close(release)
		<-scanDone
		t.Fatal("read bypassed replication scan while TTL cleanup was active")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-scanDone; err != nil {
		t.Fatalf("TTL replication scan error = %v", err)
	}
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("read remained blocked after TTL replication scan completed")
	}
}

func TestReplicationSyncReadOnlyCursorSerializesScans(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("session:scan", "value")

	firstCursor := &replicationSyncCursor{packedKeys: true, sharedReadOnly: true}
	secondCursor := &replicationSyncCursor{packedKeys: true, sharedReadOnly: true}
	defer firstCursor.close(trie)
	defer secondCursor.close(trie)
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := replicationSyncEntriesPageWithCursor(trie, "session:", "", false, 1, firstCursor, func(Entry) error {
			close(firstEntered)
			<-releaseFirst
			return nil
		})
		firstDone <- err
	}()
	<-firstEntered

	secondEntered := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		_, err := replicationSyncEntriesPageWithCursor(trie, "session:", "", false, 1, secondCursor, func(Entry) error {
			close(secondEntered)
			return nil
		})
		secondDone <- err
	}()
	select {
	case <-secondEntered:
		close(releaseFirst)
		<-firstDone
		<-secondDone
		t.Fatal("second read-only replication scan overlapped the first scan")
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first read-only replication scan error = %v", err)
	}
	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		t.Fatal("second read-only replication scan remained blocked after the first completed")
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second read-only replication scan error = %v", err)
	}
}

func TestReplicationSyncReadOnlyCursorCleansExpiredEntries(t *testing.T) {
	trie := newTestTrie(t)
	now := time.Unix(1700000000, 0)
	trie.now = func() time.Time { return now }
	trie.UpsertString("session:expired", "expired")
	trie.UpsertString("session:live", "live")
	if !trie.ExpireAt("session:expired", now.Add(time.Hour)) {
		t.Fatal("ExpireAt(session:expired) = false")
	}
	now = now.Add(2 * time.Hour)

	cursor := &replicationSyncCursor{packedKeys: true, sharedReadOnly: true}
	defer cursor.close(trie)
	var keys []string
	page, err := replicationSyncEntriesPageWithCursor(trie, "session:", "", false, 2, cursor, func(entry Entry) error {
		keys = append(keys, strings.Clone(entry.Key))
		return nil
	})
	if err != nil {
		t.Fatalf("read-only replication scan error = %v", err)
	}
	if page.hasMore || !reflect.DeepEqual(keys, []string{"session:live"}) {
		t.Fatalf("read-only replication scan = %#v keys=%#v, want only live entry", page, keys)
	}
	trie.mu.RLock()
	_, tracked := trie.expires["session:expired"]
	trie.mu.RUnlock()
	if tracked {
		t.Fatal("expired replication entry remained tracked after scan cleanup")
	}
}

func TestReplicationSyncReadOnlyCursorUsesKeyOnlyScanWithCounterStripes(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureCounterWriteStripes(64); err != nil {
		t.Fatalf("ConfigureCounterWriteStripes() error = %v", err)
	}
	for index := 0; index < 3; index++ {
		trie.UpsertCounter(fmt.Sprintf("counter:%d", index), int32(index+1))
	}

	cursor := &replicationSyncCursor{packedKeys: true, sharedReadOnly: true}
	defer cursor.close(trie)
	var keys []string
	page, err := replicationSyncEntriesPageWithCursor(trie, "counter:", "", false, 3, cursor, func(entry Entry) error {
		if !entry.Value.Empty() {
			t.Fatalf("key-only scan value for %q = %+v, want empty", entry.Key, entry.Value)
		}
		keys = append(keys, strings.Clone(entry.Key))
		return nil
	})
	if err != nil {
		t.Fatalf("key-only replication scan error = %v", err)
	}
	if page.hasMore || !reflect.DeepEqual(keys, []string{"counter:0", "counter:1", "counter:2"}) {
		t.Fatalf("key-only replication scan = %#v keys=%#v", page, keys)
	}
}

func TestReplicationSyncKeyOnlyScanWithConcurrentStripedCounterWrites(t *testing.T) {
	const keyCount = 1000
	trie := newTestTrie(t)
	if err := trie.ConfigureCounterWriteStripes(64); err != nil {
		t.Fatalf("ConfigureCounterWriteStripes() error = %v", err)
	}
	for index := 0; index < keyCount; index++ {
		trie.UpsertCounter(fmt.Sprintf("counter:%04d", index), int32(index))
	}

	stop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for {
			select {
			case <-stop:
				return
			default:
				trie.IncrementCounter("counter:0500", 1)
			}
		}
	}()
	for iteration := 0; iteration < 20; iteration++ {
		cursor := &replicationSyncCursor{packedKeys: true, sharedReadOnly: true}
		page, err := replicationSyncEntriesPageWithCursor(trie, "counter:", "", false, keyCount, cursor, func(entry Entry) error {
			if !entry.Value.Empty() {
				t.Fatalf("concurrent key-only scan value for %q = %+v, want empty", entry.Key, entry.Value)
			}
			return nil
		})
		cursor.close(trie)
		if err != nil || page.hasMore || page.scanned != keyCount {
			close(stop)
			<-writerDone
			t.Fatalf("concurrent key-only scan = %#v/%v, want %d entries", page, err, keyCount)
		}
	}
	close(stop)
	<-writerDone
	if writes := trie.CounterWriteStripingStats().FastPathWrites; writes == 0 {
		t.Fatal("concurrent scan completed without a striped counter fast-path write")
	}
}

func TestReplicationSyncCursorRestartsAfterMutation(t *testing.T) {
	trie := newTestTrie(t)
	for _, key := range []string{"session:1", "session:3", "session:5"} {
		trie.UpsertString(key, "value")
	}

	cursor := &replicationSyncCursor{packedKeys: true}
	defer cursor.close(trie)
	var keys []string
	page, err := replicationSyncEntriesPageWithCursor(trie, "session:", "", false, 2, cursor, func(entry Entry) error {
		keys = append(keys, entry.Key)
		return nil
	})
	if err != nil || !page.hasMore {
		t.Fatalf("first page = %#v/%v, want more entries", page, err)
	}
	trie.UpsertString("session:4", "inserted")
	page, err = replicationSyncEntriesPageWithCursor(trie, "session:", page.nextAfterKey, true, 2, cursor, func(entry Entry) error {
		keys = append(keys, entry.Key)
		return nil
	})
	if err != nil || page.hasMore {
		t.Fatalf("second page = %#v/%v, want final page", page, err)
	}
	if want := []string{"session:1", "session:3", "session:4", "session:5"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("cursor keys after mutation = %#v, want %#v", keys, want)
	}
	if cursor.restarts != 1 {
		t.Fatalf("cursor restarts = %d, want 1", cursor.restarts)
	}
}

func TestPartitionReplicationSyncCursorVisitsOnceAndRestartsAfterMutation(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(16); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 257; index++ {
		trie.UpsertString(fmt.Sprintf("partition-sync:%04d", index), "value")
	}

	cursor := &replicationSyncCursor{packedKeys: true}
	defer cursor.close(trie)
	afterKey := ""
	hasAfterKey := false
	keys := make([]string, 0, 258)
	for pageIndex := 0; ; pageIndex++ {
		page, err := replicationSyncEntriesPageWithCursor(trie, "partition-sync:", afterKey, hasAfterKey, 31, cursor, func(entry Entry) error {
			keys = append(keys, entry.Key)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if pageIndex == 0 {
			trie.UpsertString("partition-sync:0031a", "inserted")
		}
		if !page.hasMore {
			break
		}
		afterKey = page.nextAfterKey
		hasAfterKey = true
	}
	if len(keys) != 258 {
		t.Fatalf("partition cursor returned %d keys, want 258", len(keys))
	}
	for index := 1; index < len(keys); index++ {
		if keys[index-1] >= keys[index] {
			t.Fatalf("partition cursor order at %d = %q then %q", index, keys[index-1], keys[index])
		}
	}
	if cursor.restarts != 1 {
		t.Fatalf("partition cursor restarts = %d, want 1", cursor.restarts)
	}
	if cursor.visited < len(keys) {
		t.Fatalf("partition cursor visits = %d, want at least %d", cursor.visited, len(keys))
	}
}

func TestPartitionReplicationSyncCursorEncodesChildValues(t *testing.T) {
	source := newTestTrie(t)
	if err := source.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	source.UpsertString("partition-sync:string", "value")
	source.UpsertCounter("partition-sync:counter", 42)

	var payloads []CacheCommandRequest
	cursor := &replicationSyncCursor{}
	page, err := replicationSyncEntriesPageWithCursor(source, "partition-sync:", "", false, 10, cursor, func(entry Entry) error {
		data, ok, err := source.appendCommandDumpScannedEntryBinaryWithoutStatsLocked(nil, entry)
		if err != nil {
			return err
		}
		if ok {
			payloads = append(payloads, CacheCommandRequest{Command: replicationSetCompactCommand, Key: entry.Key, BinaryValue: data})
		}
		return nil
	})
	cursor.close(source)
	if err != nil || page.hasMore || len(payloads) != 2 {
		t.Fatalf("partition payload page = %#v/%v, payloads=%d", page, err, len(payloads))
	}

	target := newTestTrie(t)
	for index, payload := range payloads {
		operation, err := commandReplicationValueOperation(payload.Key, payload.BinaryValue)
		if err != nil {
			t.Fatalf("decode payload %d: %v", index, err)
		}
		response := executePreparedInternalReplicationCommand(target, payload, &operation)
		if !response.OK {
			t.Fatalf("apply payload %q = %#v", payload.Key, response)
		}
	}
	if target.GetString("partition-sync:string") != "value" || target.GetCounter("partition-sync:counter") != 42 {
		t.Fatalf("replicated values = %q/%d", target.GetString("partition-sync:string"), target.GetCounter("partition-sync:counter"))
	}
}

func TestPartitionReplicationSyncAllPagedRoundTrip(t *testing.T) {
	source := newTestTrie(t)
	if err := source.ConfigureLocalPartitions(16); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 257; index++ {
		source.UpsertString(fmt.Sprintf("partition-roundtrip:%04d", index), "value-"+strconv.Itoa(index))
	}
	targetTrie := newTestTrie(t)
	var targetHandler http.Handler
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		targetHandler.ServeHTTP(writer, request)
	}))
	defer target.Close()
	topology := replicationTestTopology(t, target.URL)
	targetHandler = NewMonitoringHandler(targetTrie, MonitoringOptions{
		NodeName:          "node-b",
		Topology:          topology,
		ReplicationSafety: NewReplicationSafetyStore(),
	}).Handler()
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: NewElectionStore(topology, ElectionOptions{}),
		Client:   target.Client(),
	})
	defer replicator.Close()

	result := replicator.syncAllPaged(context.Background(), source, "partition-roundtrip:", 31)
	if result.Skipped || result.Entries != 257 || len(result.Targets) == 0 {
		t.Fatalf("partition sync result = %#v", result)
	}
	for index := 0; index < 257; index++ {
		key := fmt.Sprintf("partition-roundtrip:%04d", index)
		if got := targetTrie.GetString(key); got != "value-"+strconv.Itoa(index) {
			t.Fatalf("target %q = %q", key, got)
		}
	}
}

func TestReplicationSyncEntriesPageCapturesPointInTimeValues(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("session:1", "one")
	trie.UpsertString("session:2", "two")

	var payloads []CacheCommandRequest
	page, err := replicationSyncEntriesPage(trie, "session:", "", false, 2, func(entry Entry) error {
		data, ok, err := trie.commandDumpScannedEntryBinaryLocked(entry)
		if err != nil {
			return err
		}
		if ok {
			payloads = append(payloads, CacheCommandRequest{Command: replicationSetCompactCommand, Key: entry.Key, BinaryValue: data})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("replicationSyncEntriesPage() error = %v", err)
	}
	if page.scanned != 2 || len(payloads) != 2 || page.hasMore {
		t.Fatalf("payload page = %#v, want two complete point-in-time payloads", page)
	}
	trie.Delete("session:1")
	trie.Delete("session:2")

	restored := newTestTrie(t)
	for idx, payload := range payloads {
		operation, err := commandReplicationValueOperation(payload.Key, payload.BinaryValue)
		if err != nil {
			t.Fatalf("payload %d decode error = %v", idx, err)
		}
		if response := executePreparedInternalReplicationCommand(restored, payload, &operation); !response.OK {
			t.Fatalf("payload %d restore = %#v", idx, response)
		}
	}
	for key, want := range map[string]string{"session:1": "one", "session:2": "two"} {
		if got, ok, err := restored.GetStringChecked(key); err != nil || !ok || got != want {
			t.Fatalf("restored %s = %q/%v/%v, want %q/true/nil", key, got, ok, err, want)
		}
	}
}

func TestHTTPReplicatorSyncDoesNotRecordUserReads(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = mustDecodeReplicationTestCommand(t, w, r)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	enableTestKeyStats(t, trie)
	trie.UpsertString("session:1", "one")
	trie.UpsertString("session:2", "two")
	before := trie.Stats()
	beforeKey, ok := trie.StatsForKey("session:1")
	if !ok {
		t.Fatal("StatsForKey(session:1) ok = false")
	}

	topology := replicationTestTopology(t, target.URL)
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: NewElectionStore(topology, ElectionOptions{}),
		Client:   target.Client(),
	})
	result := replicator.syncAllPaged(context.Background(), trie, "session:", 10)
	if result.Skipped || result.Entries != 2 || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("syncAllPaged() = %#v, want two entries sent successfully", result)
	}
	if after := trie.Stats(); !reflect.DeepEqual(after, before) {
		t.Fatalf("stats after internal sync = %#v, want unchanged %#v", after, before)
	}
	if afterKey, ok := trie.StatsForKey("session:1"); !ok || !reflect.DeepEqual(afterKey, beforeKey) {
		t.Fatalf("key stats after internal sync = %#v/%v, want unchanged %#v/true", afterKey, ok, beforeKey)
	}

	if _, ok, err := trie.commandDumpEntryBinary("session:1"); err != nil || !ok {
		t.Fatalf("commandDumpEntryBinary(session:1) = ok %v error %v, want true/nil", ok, err)
	}
	if after := trie.Stats(); after.Reads != before.Reads+1 || after.Hits != before.Hits+1 {
		t.Fatalf("stats after explicit dump = %#v, want one additional hit", after)
	}
	if afterKey, ok := trie.StatsForKey("session:1"); !ok || afterKey.Reads != beforeKey.Reads+1 || afterKey.Hits != beforeKey.Hits+1 {
		t.Fatalf("key stats after explicit dump = %#v/%v, want one additional hit", afterKey, ok)
	}
}

func TestHTTPReplicatorSyncAllSkipsExpiredEntries(t *testing.T) {
	requests := make(chan CacheCommandRequest, 2)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		if rejectReplicationDigestTestCommand(t, w, r, request) {
			return
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	base := time.Unix(800, 0)
	trie.now = func() time.Time { return base }
	trie.UpsertString("session:expired", "old")
	trie.UpsertString("session:live", "new")
	if !trie.Expire("session:expired", time.Second) {
		t.Fatal("Expire(session:expired) = false, want true")
	}
	if !trie.Expire("session:live", 10*time.Second) {
		t.Fatal("Expire(session:live) = false, want true")
	}
	trie.now = func() time.Time { return base.Add(2 * time.Second) }

	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	result := replicator.SyncAll(context.Background(), trie, "session:")
	if result.Skipped || result.Entries != 1 || len(result.Targets) != 1 || result.Targets[0].Key != "session:live" {
		t.Fatalf("sync result = %#v, want only live session entry", result)
	}
	request := <-requests
	if !isTypedReplicationSetPayload(request, "session:live") {
		t.Fatalf("sync request = %#v, want live typed binary snapshot", request)
	}
	select {
	case request := <-requests:
		t.Fatalf("unexpected sync request = %#v", request)
	default:
	}
}

func TestHTTPReplicatorSyncAllChecksTargetWhenSourceHasNoEntries(t *testing.T) {
	trie := newTestTrie(t)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		if normalizedCommand(request.Command) != replicationDigestCommand {
			t.Fatalf("empty source command = %q, want %s", request.Command, replicationDigestCommand)
		}
		format, _ := commandWireFormatFromContentType(r.Header.Get("Content-Type"))
		writeCommandResponseWire(w, r, http.StatusOK, CacheCommandResponse{OK: true, Message: replicationDigestFinalMessage}, format)
	}))
	defer target.Close()
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
	})

	result := replicator.SyncAll(context.Background(), trie, "missing:")
	if result.Skipped || result.Entries != 0 || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("empty sync result = %#v, want successful target digest check", result)
	}
	if got := replicator.LastResult(); !reflect.DeepEqual(got, result) {
		t.Fatalf("LastResult() = %#v, want empty sync result %#v", got, result)
	}
}

func TestPostReplicationCommandDrainsErrorResponseBody(t *testing.T) {
	body := newTrackingReadCloser(strings.Repeat("error body ", 32))
	client := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Status:     "503 Service Unavailable",
				Header:     make(http.Header),
				Body:       body,
				Request:    request,
			}, nil
		}),
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Client: client})

	result := replicator.postReplicationCommand(context.Background(), TopologyNode{
		ID:      "node-b",
		Address: "127.0.0.1:8080",
	}, CacheCommandRequest{Command: "INTERNALSET", Key: "session:1", Value: "{}"})
	if result.OK || result.Status != http.StatusServiceUnavailable || result.Error != "503 Service Unavailable" {
		t.Fatalf("postReplicationCommand() = %#v, want 503 error result", result)
	}
	if !body.drained || !body.closed {
		t.Fatalf("response body drained=%v closed=%v, want both true", body.drained, body.closed)
	}
}

func TestPostReplicationCommandRejectsOversizedResponseBody(t *testing.T) {
	body := `{"ok":true,"message":"` + strings.Repeat("x", maxHTTPReplicationResponseBytes) + `"}`
	client := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(body)),
				Request:    request,
			}, nil
		}),
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Client: client})

	result := replicator.postReplicationCommand(context.Background(), TopologyNode{
		ID:      "node-b",
		Address: "127.0.0.1:8080",
	}, CacheCommandRequest{Command: "INTERNALSET", Key: "session:1", Value: "{}"})
	if result.OK || result.Status != http.StatusOK || !strings.Contains(result.Error, "replication response is too large") {
		t.Fatalf("postReplicationCommand() = %#v, want oversized response error", result)
	}
}

func TestDecodeReplicationCommandResponseRejectsOversizedTrailingWhitespace(t *testing.T) {
	body := `{"ok":true,"message":"ok"}` + strings.Repeat(" ", maxHTTPReplicationResponseBytes+1)
	_, err := decodeReplicationCommandResponse(strings.NewReader(body))
	if !errors.Is(err, errReplicationResponseTooLarge) {
		t.Fatalf("decodeReplicationCommandResponse() error = %v, want response too large", err)
	}
}

func TestPostReplicationCommandRejectsTrailingResponseJSON(t *testing.T) {
	client := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"ok":true,"message":"ok"}{"ok":true,"message":"ok"}`)),
				Request:    request,
			}, nil
		}),
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Client: client})

	result := replicator.postReplicationCommand(context.Background(), TopologyNode{
		ID:      "node-b",
		Address: "127.0.0.1:8080",
	}, CacheCommandRequest{Command: "INTERNALSET", Key: "session:1", Value: "{}"})
	if result.OK || result.Status != http.StatusOK || !strings.Contains(result.Error, "invalid command response JSON") {
		t.Fatalf("postReplicationCommand() = %#v, want trailing JSON error", result)
	}
}

func TestHTTPReplicatorCompressesLargeReplicationRequests(t *testing.T) {
	follower := newTestTrie(t)
	var contentEncoding string
	handler := NewMonitoringHandler(follower, MonitoringOptions{}).Handler()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentEncoding = r.Header.Get("Content-Encoding")
		handler.ServeHTTP(w, r)
	}))
	defer target.Close()

	leader := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})
	large := strings.Repeat("x", minCompressedReplicationRequestBytes)
	write := CacheCommandRequest{Command: "SETSTR", Key: "session:large", Value: large}
	response := leader.ExecuteCommand(write)
	if !response.OK {
		t.Fatalf("leader SETSTR response = %#v, want ok", response)
	}

	result := replicator.ReplicateCommand(context.Background(), leader, write, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("large replication result = %#v, want one ok target", result)
	}
	if contentEncoding != "gzip" {
		t.Fatalf("replication Content-Encoding = %q, want gzip", contentEncoding)
	}
	if got := follower.GetString("session:large"); got != large {
		t.Fatalf("follower value length = %d, want %d", len(got), len(large))
	}
}

func TestReplicationRequestBodyStreamsLargeStringPayload(t *testing.T) {
	payload := CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:large",
		Value:   strings.Repeat("x", minCompressedReplicationRequestBytes),
	}
	body, contentEncoding, err := replicationRequestBody(payload)
	if err != nil {
		t.Fatalf("replicationRequestBody() error = %v", err)
	}
	if contentEncoding != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", contentEncoding)
	}
	if _, ok := body.(*jsonwire.StreamingGzipJSONBody); !ok {
		t.Fatalf("replicationRequestBody() body = %T, want streaming gzip body", body)
	}

	compressed, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("ReadAll(compressed body) error = %v", err)
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("NewReader(compressed body) error = %v", err)
	}
	defer reader.Close()

	var decoded CacheCommandRequest
	if err := json.NewDecoder(reader).Decode(&decoded); err != nil {
		t.Fatalf("Decode(compressed body) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, payload) {
		t.Fatalf("decoded payload = %#v, want %#v", decoded, payload)
	}
}

func TestEstimatedReplicationRequestBytesUsesExactOptionalIntSize(t *testing.T) {
	priority := int64(3)
	ttl := int64(60)
	unix := int64(-7)
	base := CacheCommandRequest{Command: "INTERNALSET", Key: "session:ttl", Value: "value"}
	withOptionals := base
	withOptionals.Priority = &priority
	withOptionals.TTLSeconds = &ttl
	withOptionals.UnixSeconds = &unix

	wantExtra := jsonwire.EstimateJSONValueBytes(priority, minCompressedReplicationRequestBytes) +
		jsonwire.EstimateJSONValueBytes(ttl, minCompressedReplicationRequestBytes) +
		jsonwire.EstimateJSONValueBytes(unix, minCompressedReplicationRequestBytes)
	gotExtra := estimatedReplicationRequestBytes(withOptionals) - estimatedReplicationRequestBytes(base)
	if gotExtra != wantExtra {
		t.Fatalf("estimated optional int bytes = %d, want exact numeric bytes %d", gotExtra, wantExtra)
	}
	if got := addEstimatedOptionalCommandInt64(minCompressedReplicationRequestBytes-1, &priority, minCompressedReplicationRequestBytes); got != minCompressedReplicationRequestBytes {
		t.Fatalf("addEstimatedOptionalCommandInt64(capped) = %d, want threshold", got)
	}
}

func TestReplicationRequestBodyStreamsEscapedLargeStringPayload(t *testing.T) {
	payload := CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:escaped",
		Value:   strings.Repeat("\n", minCompressedReplicationRequestBytes/2),
	}
	body, contentEncoding, err := replicationRequestBody(payload)
	if err != nil {
		t.Fatalf("replicationRequestBody(escaped value) error = %v", err)
	}
	if contentEncoding != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", contentEncoding)
	}
	if _, ok := body.(*jsonwire.StreamingGzipJSONBody); !ok {
		t.Fatalf("replicationRequestBody(escaped value) body = %T, want streaming gzip body", body)
	}

	reader, err := gzip.NewReader(body)
	if err != nil {
		t.Fatalf("NewReader(streaming gzip body) error = %v", err)
	}
	defer reader.Close()

	var decoded CacheCommandRequest
	if err := json.NewDecoder(reader).Decode(&decoded); err != nil {
		t.Fatalf("Decode(streaming escaped body) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, payload) {
		t.Fatalf("decoded escaped payload = %#v, want %#v", decoded, payload)
	}
}

func TestReplicationRequestBodyLeavesSmallPayloadPlain(t *testing.T) {
	payload := CacheCommandRequest{
		Command: "INTERNALDEL",
		Key:     "session:small",
	}
	body, contentEncoding, err := replicationRequestBody(payload)
	if err != nil {
		t.Fatalf("replicationRequestBody() error = %v", err)
	}
	if contentEncoding != "" {
		t.Fatalf("Content-Encoding = %q, want empty", contentEncoding)
	}

	var decoded CacheCommandRequest
	if err := json.NewDecoder(body).Decode(&decoded); err != nil {
		t.Fatalf("Decode(plain body) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, payload) {
		t.Fatalf("decoded plain payload = %#v, want %#v", decoded, payload)
	}
}

func TestReplicationRequestBodyStreamsLargeStructuredPayload(t *testing.T) {
	values := make(Slice, 0, minCompressedReplicationRequestBytes/4)
	for len(values) < cap(values) {
		values = append(values, strings.Repeat("value", 4))
	}
	payload := CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:structured",
		Values:  values,
	}
	body, contentEncoding, err := replicationRequestBody(payload)
	if err != nil {
		t.Fatalf("replicationRequestBody() error = %v", err)
	}
	if contentEncoding != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", contentEncoding)
	}
	if _, ok := body.(*jsonwire.StreamingGzipJSONBody); !ok {
		t.Fatalf("replicationRequestBody() body = %T, want streaming gzip body", body)
	}

	reader, err := gzip.NewReader(body)
	if err != nil {
		t.Fatalf("NewReader(streaming gzip body) error = %v", err)
	}
	defer reader.Close()

	var decoded CacheCommandRequest
	if err := json.NewDecoder(reader).Decode(&decoded); err != nil {
		t.Fatalf("Decode(streaming gzip body) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, payload) {
		t.Fatalf("decoded streaming payload = %#v, want %#v", decoded, payload)
	}
}

func TestReplicationRequestBodyStreamsLargePairPayload(t *testing.T) {
	pairs := make(Map, minCompressedReplicationRequestBytes/128)
	for len(pairs) < minCompressedReplicationRequestBytes/128 {
		key := strings.Repeat("profile:", 2) + string(rune('a'+len(pairs)/26)) + string(rune('a'+len(pairs)%26))
		pairs[key] = strings.Repeat("value", 32)
	}
	payload := CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:pairs",
		Pairs:   pairs,
	}
	body, contentEncoding, err := replicationRequestBody(payload)
	if err != nil {
		t.Fatalf("replicationRequestBody() error = %v", err)
	}
	if contentEncoding != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", contentEncoding)
	}
	if _, ok := body.(*jsonwire.StreamingGzipJSONBody); !ok {
		t.Fatalf("replicationRequestBody() body = %T, want streaming gzip body", body)
	}

	reader, err := gzip.NewReader(body)
	if err != nil {
		t.Fatalf("NewReader(streaming gzip body) error = %v", err)
	}
	defer reader.Close()

	var decoded CacheCommandRequest
	if err := json.NewDecoder(reader).Decode(&decoded); err != nil {
		t.Fatalf("Decode(streaming gzip body) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, payload) {
		t.Fatalf("decoded streaming payload = %#v, want %#v", decoded, payload)
	}
}

func TestReplicationRequestBodyReportsMarshalErrors(t *testing.T) {
	body, contentEncoding, err := replicationRequestBody(CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:bad",
		Values:  Slice{func() {}},
	})
	if err == nil {
		t.Fatal("replicationRequestBody(unsupported) error = nil, want error")
	}
	if body != nil {
		t.Fatalf("replicationRequestBody(unsupported) body = %T, want nil", body)
	}
	if contentEncoding != "" {
		t.Fatalf("Content-Encoding = %q, want empty on marshal error", contentEncoding)
	}
}

func waitForReplicationLastResult(t *testing.T, replicator *HTTPReplicator, accept func(ReplicationResult) bool) ReplicationResult {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	var result ReplicationResult
	for time.Now().Before(deadline) {
		result = replicator.LastResult()
		if accept(result) {
			return result
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("LastResult() = %#v did not satisfy predicate", result)
	return ReplicationResult{}
}

func TestHTTPReplicatorReplicatesBloomFilterMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATEBF", Key: "seen", Value: "1000", Subkey: "0.001"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATEBF response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDBF", Key: "seen", Values: Slice{"alpha", "beta"}}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDBF response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("Bloom filter replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		entry := mustDecodeTypedReplicationSnapshot(t, request, "seen")
		if entry.Type != "bloom_filter" || entry.BloomFilter == nil {
			t.Fatalf("replicated Bloom snapshot = %#v, want bloom_filter payload", entry)
		}
	default:
		t.Fatal("Bloom filter mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "HASBF", Key: "seen", Value: "alpha"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "HASBF", Key: "seen", Value: "alpha"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("Bloom filter read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesXorFilterMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	if response := trie.ExecuteCommand(CacheCommandRequest{Command: "CREATEXF", Key: "seen", Value: "8"}); !response.OK {
		t.Fatalf("CREATEXF response = %#v, want ok", response)
	}
	if response := trie.ExecuteCommand(CacheCommandRequest{Command: "ADDXF", Key: "seen", Values: Slice{"alpha", "beta"}}); !response.OK {
		t.Fatalf("ADDXF response = %#v, want ok", response)
	}
	build := CacheCommandRequest{Command: "BUILDXF", Key: "seen"}
	response := trie.ExecuteCommand(build)
	if !response.OK {
		t.Fatalf("BUILDXF response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, build, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("XOR filter replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		entry := mustDecodeTypedReplicationSnapshot(t, request, "seen")
		if entry.Type != "xor_filter" || entry.XorFilter == nil || !entry.XorFilter.Built {
			t.Fatalf("replicated XOR snapshot = %#v, want built xor_filter payload", entry)
		}
	default:
		t.Fatal("XOR filter mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "HASXF", Key: "seen", Value: "alpha"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "HASXF", Key: "seen", Value: "alpha"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("XOR filter read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesRadixTreeMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	if response := trie.ExecuteCommand(CacheCommandRequest{Command: "CREATERT", Key: "index"}); !response.OK {
		t.Fatalf("CREATERT response = %#v, want ok", response)
	}
	put := CacheCommandRequest{Command: "PUTRT", Key: "index", Subkey: "user:100/profile", Value: "active"}
	response := trie.ExecuteCommand(put)
	if !response.OK {
		t.Fatalf("PUTRT response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, put, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("radix tree replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		entry := mustDecodeTypedReplicationSnapshot(t, request, "index")
		if entry.Type != "radix_tree" || entry.RadixTree == nil || entry.RadixTree.Count != 1 {
			t.Fatalf("replicated radix snapshot = %#v, want radix_tree payload", entry)
		}
	default:
		t.Fatal("radix tree mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "HASRT", Key: "index", Subkey: "user:100/profile"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "HASRT", Key: "index", Subkey: "user:100/profile"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("radix tree read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesCuckooFilterMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATECF", Key: "seen", Value: "128", Subkey: "0.001"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATECF response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDCF", Key: "seen", Values: Slice{"alpha", "beta"}}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDCF response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("Cuckoo filter replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		entry := mustDecodeTypedReplicationSnapshot(t, request, "seen")
		if entry.Type != "cuckoo_filter" || entry.CuckooFilter == nil {
			t.Fatalf("replicated Cuckoo snapshot = %#v, want cuckoo_filter payload", entry)
		}
	default:
		t.Fatal("Cuckoo filter mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "HASCF", Key: "seen", Value: "alpha"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "HASCF", Key: "seen", Value: "alpha"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("Cuckoo filter read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesRoaringBitmapMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATERB", Key: "ids"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATERB response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDRB", Key: "ids", Values: Slice{json.Number("1"), json.Number("65543")}}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDRB response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("Roaring bitmap replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		entry := mustDecodeTypedReplicationSnapshot(t, request, "ids")
		if entry.Type != "roaring_bitmap" || entry.RoaringBitmap == nil {
			t.Fatalf("replicated Roaring bitmap snapshot = %#v, want roaring_bitmap payload", entry)
		}
	default:
		t.Fatal("Roaring bitmap mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "HASRB", Key: "ids", Value: "1"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "HASRB", Key: "ids", Value: "1"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("Roaring bitmap read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesSparseBitsetMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATESB", Key: "ids"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATESB response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDSB", Key: "ids", Values: Slice{json.Number("1"), json.Number("65543"), json.Number("18446744073709551615")}}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDSB response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("sparse bitset replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		entry := mustDecodeTypedReplicationSnapshot(t, request, "ids")
		if entry.Type != "sparse_bitset" || entry.SparseBitset == nil {
			t.Fatalf("replicated sparse bitset snapshot = %#v, want sparse_bitset payload", entry)
		}
	default:
		t.Fatal("sparse bitset mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "HASSB", Key: "ids", Value: "1"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "HASSB", Key: "ids", Value: "1"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("sparse bitset read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesCountMinSketchMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATECMS", Key: "freq", Value: "128", Subkey: "4"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATECMS response = %#v, want ok", response)
	}
	increment := CacheCommandRequest{Command: "INCRCMS", Key: "freq", Value: "alpha", Subkey: "3"}
	response := trie.ExecuteCommand(increment)
	if !response.OK {
		t.Fatalf("INCRCMS response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, increment, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("Count-Min Sketch replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		entry := mustDecodeTypedReplicationSnapshot(t, request, "freq")
		if entry.Type != "count_min_sketch" || entry.CountMinSketch == nil {
			t.Fatalf("replicated Count-Min Sketch snapshot = %#v, want count_min_sketch payload", entry)
		}
	default:
		t.Fatal("Count-Min Sketch mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "ESTCMS", Key: "freq", Value: "alpha"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "ESTCMS", Key: "freq", Value: "alpha"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("Count-Min Sketch read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesHyperLogLogMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATEHLL", Key: "card", Value: "10"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATEHLL response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDHLL", Key: "card", Values: Slice{"alpha", "beta"}}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDHLL response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("HyperLogLog replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		entry := mustDecodeTypedReplicationSnapshot(t, request, "card")
		if entry.Type != "hyperloglog" || entry.HyperLogLog == nil {
			t.Fatalf("replicated HyperLogLog snapshot = %#v, want hyperloglog payload", entry)
		}
	default:
		t.Fatal("HyperLogLog mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "COUNTHLL", Key: "card"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "COUNTHLL", Key: "card"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("HyperLogLog read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesTopKMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATETOPK", Key: "top", Value: "3"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATETOPK response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDTOPK", Key: "top", Value: "alpha", Subkey: "5"}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDTOPK response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("Top-K replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		entry := mustDecodeTypedReplicationSnapshot(t, request, "top")
		if entry.Type != "top_k" || entry.TopK == nil {
			t.Fatalf("replicated Top-K snapshot = %#v, want top_k payload", entry)
		}
	default:
		t.Fatal("Top-K mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "GETTOPK", Key: "top"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "GETTOPK", Key: "top"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("Top-K read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesReservoirSampleMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATERS", Key: "sample", Value: "3"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATERS response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDRS", Key: "sample", Values: Slice{"alpha", "beta", "gamma", "delta"}}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDRS response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("reservoir sample replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		entry := mustDecodeTypedReplicationSnapshot(t, request, "sample")
		if entry.Type != "reservoir_sample" || entry.ReservoirSample == nil {
			t.Fatalf("replicated reservoir sample snapshot = %#v, want reservoir_sample payload", entry)
		}
	default:
		t.Fatal("reservoir sample mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "GETRS", Key: "sample"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "GETRS", Key: "sample"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("reservoir sample read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesQuantileSketchMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATEQ", Key: "latency", Value: "0.01"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATEQ response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDQ", Key: "latency", Values: Slice{json.Number("10"), json.Number("20"), json.Number("30")}}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDQ response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("quantile sketch replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		entry := mustDecodeTypedReplicationSnapshot(t, request, "latency")
		if entry.Type != "quantile_sketch" || entry.QuantileSketch == nil {
			t.Fatalf("replicated quantile sketch snapshot = %#v, want quantile_sketch payload", entry)
		}
	default:
		t.Fatal("quantile sketch mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "ESTQ", Key: "latency", Value: "0.5"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "ESTQ", Key: "latency", Value: "0.5"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("quantile sketch read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesFenwickTreeMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATEFW", Key: "scores", Value: "8"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATEFW response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDFW", Key: "scores", Value: "2", Subkey: "5"}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDFW response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("Fenwick tree replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		entry := mustDecodeTypedReplicationSnapshot(t, request, "scores")
		if entry.Type != "fenwick_tree" || entry.FenwickTree == nil {
			t.Fatalf("replicated Fenwick tree snapshot = %#v, want fenwick_tree payload", entry)
		}
	default:
		t.Fatal("Fenwick tree mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "SUMFW", Key: "scores", Value: "2"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "SUMFW", Key: "scores", Value: "2"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("Fenwick tree read replication result = %#v, want skipped read command", result)
	}
}

func TestReplicationEndpointNormalizesAddresses(t *testing.T) {
	got, err := replicationEndpoint("127.0.0.1:8080/base/")
	if err != nil {
		t.Fatalf("replicationEndpoint() error = %v", err)
	}
	if got != "http://127.0.0.1:8080/base/api/commands" {
		t.Fatalf("endpoint = %q, want normalized API path", got)
	}
}

func replicationTestTopology(t testing.TB, replicaAddress string) *TopologyStore {
	t.Helper()
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://127.0.0.1:1"},
			{ID: "node-b", Address: replicaAddress},
		},
		Shards: []TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	return topology
}

func replicationTestTopologyWithReplicas(t *testing.T, replicaBAddress string, replicaCAddress string) *TopologyStore {
	t.Helper()
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://127.0.0.1:1"},
			{ID: "node-b", Address: replicaBAddress},
			{ID: "node-c", Address: replicaCAddress},
		},
		Shards: []TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b", "node-c"}},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	return topology
}
