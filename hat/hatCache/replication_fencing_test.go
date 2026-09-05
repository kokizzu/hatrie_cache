package hatCache

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"strings"
	"testing"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func fencedReplicationTopology(token uint64) ClusterTopology {
	return ClusterTopology{
		Version:      1,
		Mode:         TopologyModeSharded,
		Self:         "node-a",
		FencingToken: token,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a"},
			{ID: "node-b", Address: "http://node-b"},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	}
}

func TestReplicationFencingTokenRejectsStaleWriter(t *testing.T) {
	topology, err := NewTopologyStore(fencedReplicationTopology(42))
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}

	request := CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "key",
		Value:   "value",
		Pairs: Map{
			replicationMetaSourceNode:   "node-a",
			replicationMetaSequence:     "1",
			replicationMetaFencingToken: "41",
		},
	}
	_, response, handled, rejected := checkReplicationSafety(request, topology, NewReplicationSafetyStore())
	if !handled || !rejected {
		t.Fatalf("checkReplicationSafety() handled=%v rejected=%v, want both true", handled, rejected)
	}
	if response.OK || !strings.Contains(response.Message, "replication fencing token mismatch") {
		t.Fatalf("stale writer response = %#v, want fencing-token rejection", response)
	}

	request.Pairs[replicationMetaFencingToken] = "42"
	_, response, _, rejected = checkReplicationSafety(request, topology, NewReplicationSafetyStore())
	if rejected || !response.OK && response.Message != "" {
		t.Fatalf("matching writer response = %#v rejected=%v, want admission", response, rejected)
	}

	delete(request.Pairs, replicationMetaFencingToken)
	_, response, handled, rejected = checkReplicationSafety(request, topology, NewReplicationSafetyStore())
	if !handled || !rejected || response.OK || !strings.Contains(response.Message, "replication fencing token mismatch") {
		t.Fatalf("unfenced writer response = %#v handled=%v rejected=%v, want rejection", response, handled, rejected)
	}
}

func TestReplicatorAnnotatesFencingToken(t *testing.T) {
	topology, err := NewTopologyStore(fencedReplicationTopology(42))
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	replicator := &HTTPReplicator{self: "node-a", topology: topology}
	annotated := replicator.annotateReplicationPayload(CacheCommandRequest{Command: "INTERNALSET", Key: "key", Value: "value"})
	token, present, err := replicationFencingToken(annotated)
	if err != nil || !present || token != 42 {
		t.Fatalf("annotated fencing token = %d present=%v error=%v, want 42", token, present, err)
	}
}

func TestTopologyStoreRejectsRegressingFencingToken(t *testing.T) {
	store, err := NewTopologyStore(fencedReplicationTopology(42))
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}

	if err := store.Set(fencedReplicationTopology(41)); err == nil || !strings.Contains(strings.ToLower(err.Error()), "fencing token") {
		t.Fatalf("TopologyStore.Set(regressing token) error = %v, want fencing-token error", err)
	}
	if got := store.Get().FencingToken; got != 42 {
		t.Fatalf("topology token after rejected update = %d, want 42", got)
	}
	if err := store.Set(fencedReplicationTopology(43)); err != nil {
		t.Fatalf("TopologyStore.Set(advancing token) error = %v", err)
	}
	if got := store.Get().FencingToken; got != 43 {
		t.Fatalf("topology token after advancing update = %d, want 43", got)
	}
}

func TestReplicationBatchEnvelopePreservesFencingToken(t *testing.T) {
	payloads := []CacheCommandRequest{
		{
			Command: "INTERNALSET",
			Key:     "one",
			Value:   "1",
			Pairs: Map{
				replicationMetaSourceNode:          "node-a",
				replicationMetaSequence:            "1",
				replicationMetaTopologyFingerprint: "fp",
				replicationMetaFencingToken:        "42",
				"application":                      "keep",
			},
		},
		{
			Command: "INTERNALSET",
			Key:     "two",
			Value:   "2",
			Pairs: Map{
				replicationMetaSourceNode:          "node-a",
				replicationMetaSequence:            "2",
				replicationMetaTopologyFingerprint: "fp",
				replicationMetaFencingToken:        "42",
			},
		},
	}

	envelope, err := replicationBatchEnvelopePayload(payloads)
	if err != nil {
		t.Fatalf("replicationBatchEnvelopePayload() error = %v", err)
	}
	token, present, err := replicationFencingToken(envelope)
	if err != nil || !present || token != 42 {
		t.Fatalf("envelope fencing token = %d present=%v error=%v, want 42", token, present, err)
	}
	if _, ok := envelope.Batch[0].Pairs[replicationMetaFencingToken]; ok {
		t.Fatal("child payload retained fencing metadata")
	}
	if got := envelope.Batch[0].Pairs["application"]; got != "keep" {
		t.Fatalf("child application metadata = %#v, want keep", got)
	}

	payloads[1].Pairs[replicationMetaFencingToken] = "43"
	if _, err := replicationBatchEnvelopePayload(payloads); err == nil || !strings.Contains(err.Error(), "metadata mismatch") {
		t.Fatalf("mismatched fencing token error = %v, want metadata mismatch", err)
	}
}

func TestGRPCTopologyFencingTokenRoundTrip(t *testing.T) {
	original := fencedReplicationTopology(42)
	got := clusterTopologyFromProto(grpcClusterTopology(original))
	if got.FencingToken != original.FencingToken {
		t.Fatalf("gRPC topology fencing token = %d, want %d", got.FencingToken, original.FencingToken)
	}
}

func TestReplicationGRPCStreamFlightPreservesFencingToken(t *testing.T) {
	flight := &replicationGRPCStreamFlight{
		jobs: []*replicationGRPCStreamJob{{
			source:              "node-a",
			topologyFingerprint: "fingerprint",
			fencingToken:        42,
			payloads: replicationSyncPayloadBatch{inline: []replicationSyncPayload{{
				key: "key", binaryValue: []byte("value"),
			}}},
		}},
		entries: 1,
	}
	flight.buildRequest(1)
	if got := flight.request.GetFencingToken(); got != 42 {
		t.Fatalf("gRPC stream fencing token = %d, want 42", got)
	}
}

func TestGRPCReplicationStreamRejectsMismatchedFencingToken(t *testing.T) {
	topology, err := NewTopologyStore(fencedReplicationTopology(42))
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	server := NewCacheGRPCServer(newTestTrie(t), CacheGRPCOptions{
		NodeName:          "node-b",
		Topology:          topology,
		ReplicationSafety: NewReplicationSafetyStore(),
	})
	ack := server.applyReplicationStreamBatch(context.Background(), &hatriecachev1.ReplicationStreamBatch{
		Source:              "node-a",
		Sequence:            1,
		TopologyFingerprint: topology.Fingerprint(),
		FencingToken:        41,
		Keys:                []string{"key"},
		BinaryValues:        [][]byte{{1}},
	})
	if ack.GetOk() || !strings.Contains(ack.GetMessage(), "replication fencing token mismatch") {
		t.Fatalf("gRPC stream acknowledgement = %#v, want fencing-token rejection", ack)
	}
}

func TestReplicationSyncBatchWirePreservesFencingToken(t *testing.T) {
	payloads := replicationSyncPayloadBatch{inline: []replicationSyncPayload{{
		key: "key", binaryValue: []byte("value"),
	}}}
	for _, test := range []struct {
		name                 string
		compressionThreshold int
		wantEncoding         string
	}{
		{name: "identity"},
		{name: "gzip", compressionThreshold: 1, wantEncoding: "gzip"},
	} {
		t.Run(test.name, func(t *testing.T) {
			body, contentType, contentEncoding, err := replicationSyncBatchRequestBodyBatchWithFencingToken(
				payloads, replicationSetCompactCommand, "node-a", 1, "fingerprint", 42, test.compressionThreshold,
			)
			if err != nil {
				t.Fatalf("replicationSyncBatchRequestBodyBatchWithFencingToken() error = %v", err)
			}
			if contentType != commandWireContentTypeProtobuf || contentEncoding != test.wantEncoding {
				t.Fatalf("content type/encoding = %q/%q, want protobuf/%q", contentType, contentEncoding, test.wantEncoding)
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
			decoded, err := decodeCommandRequestProto(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatalf("decodeCommandRequestProto() error = %v", err)
			}
			token, present, err := replicationFencingToken(decoded)
			if err != nil || !present || token != 42 {
				t.Fatalf("decoded fencing token = %d present=%v error=%v, want 42", token, present, err)
			}
		})
	}
}
