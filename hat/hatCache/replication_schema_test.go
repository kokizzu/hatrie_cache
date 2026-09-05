package hatCache

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSchema"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func replicationSchemaFixture() hatSchema.Schema {
	return hatSchema.Schema{
		Version: 7,
		Sources: map[string]hatSchema.Source{
			"users": {
				Name:    "users",
				Columns: []hatSchema.Column{{Name: "id", Type: hatSchema.TypeInteger}, {Name: "email", Type: hatSchema.TypeText}},
			},
		},
	}
}

func schemaReplicationRequest(contract ReplicationSchemaContract) CacheCommandRequest {
	return CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "key",
		Value:   `{"type":"string","string":"value"}`,
		Pairs: Map{
			replicationMetaSourceNode:        "node-a",
			replicationMetaSequence:          "1",
			replicationMetaSchemaVersion:     contract.Version,
			replicationMetaSchemaFingerprint: contract.Fingerprint,
		},
	}
}

func TestReplicationSchemaContractRoundTripAndAnnotation(t *testing.T) {
	contract := NewReplicationSchemaContract(replicationSchemaFixture())
	if !contract.Configured() {
		t.Fatalf("schema contract = %#v, want configured", contract)
	}
	replicator := &HTTPReplicator{self: "node-a", replicationSchema: contract}
	annotated := replicator.annotateReplicationPayload(CacheCommandRequest{Command: "INTERNALSET", Key: "key", Value: "value"})
	got, present, err := replicationSchemaMetadata(annotated)
	if err != nil || !present || got != contract {
		t.Fatalf("annotated schema contract = %#v present=%v error=%v, want %#v", got, present, err, contract)
	}
}

func TestReplicationSchemaCompatibilityRejectsMissingOrMismatchedContract(t *testing.T) {
	contract := NewReplicationSchemaContract(replicationSchemaFixture())
	topology, err := NewTopologyStore(SingleNodeTopology("node-b", "http://node-b"))
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	options := commandExecutionOptions{
		Topology:                   topology,
		ReplicationSafety:          NewReplicationSafetyStore(),
		replicationSchema:          contract,
		requireSchemaCompatibility: true,
	}
	trie := newTestTrie(t)

	missing := schemaReplicationRequest(contract)
	delete(missing.Pairs, replicationMetaSchemaVersion)
	delete(missing.Pairs, replicationMetaSchemaFingerprint)
	response, rejected := executeCacheCommand(context.Background(), trie, missing, options)
	if !rejected || response.OK || !strings.Contains(response.Message, "schema contract") {
		t.Fatalf("missing schema contract response = %#v rejected=%v, want rejection", response, rejected)
	}

	mismatched := schemaReplicationRequest(contract)
	mismatched.Pairs[replicationMetaSchemaFingerprint] = "different"
	response, rejected = executeCacheCommand(context.Background(), trie, mismatched, options)
	if !rejected || response.OK || !strings.Contains(response.Message, "schema contract") {
		t.Fatalf("mismatched schema contract response = %#v rejected=%v, want rejection", response, rejected)
	}

	matching := schemaReplicationRequest(contract)
	response, rejected = executeCacheCommand(context.Background(), trie, matching, options)
	if rejected || !response.OK {
		t.Fatalf("matching schema contract response = %#v rejected=%v, want success", response, rejected)
	}
}

func TestReplicationSchemaCompatibilityIsDisabledByDefault(t *testing.T) {
	contract := NewReplicationSchemaContract(replicationSchemaFixture())
	topology, err := NewTopologyStore(SingleNodeTopology("node-b", "http://node-b"))
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	request := schemaReplicationRequest(contract)
	request.Pairs[replicationMetaSchemaFingerprint] = "different"
	response, rejected := executeCacheCommand(context.Background(), newTestTrie(t), request, commandExecutionOptions{
		Topology:          topology,
		ReplicationSafety: NewReplicationSafetyStore(),
	})
	if rejected || !response.OK {
		t.Fatalf("schema compatibility default response = %#v rejected=%v, want success", response, rejected)
	}
}

func TestReplicationBatchSchemaContractIsInheritedByChildren(t *testing.T) {
	contract := NewReplicationSchemaContract(replicationSchemaFixture())
	topology, err := NewTopologyStore(SingleNodeTopology("node-b", "http://node-b"))
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	request := replicationBatchEnvelopePayloadWithMetadataAndSchemaAndFencingToken(
		[]CacheCommandRequest{{Command: "INTERNALSET", Key: "key", Value: `{"type":"string","string":"value"}`}},
		"node-a", 1, "", contract, 0,
	)
	response, rejected := executeCacheCommand(context.Background(), newTestTrie(t), request, commandExecutionOptions{
		Topology:                   topology,
		ReplicationSafety:          NewReplicationSafetyStore(),
		replicationSchema:          contract,
		requireSchemaCompatibility: true,
	})
	if rejected || !response.OK {
		t.Fatalf("schema-compatible batch response = %#v rejected=%v, want success", response, rejected)
	}
}

func TestReplicationSyncBatchWirePreservesSchemaContract(t *testing.T) {
	contract := NewReplicationSchemaContract(replicationSchemaFixture())
	body, contentType, contentEncoding, err := replicationSyncBatchRequestBodyBatchWithMetadata(
		replicationSyncPayloadBatch{inline: []replicationSyncPayload{{key: "key", binaryValue: []byte("value")}}},
		replicationSetCompactCommand,
		replicationSyncBatchMetadata{source: "node-a", sequence: 1, fingerprint: "topology", fencingToken: 0, schema: contract},
		0,
	)
	if err != nil {
		t.Fatalf("replicationSyncBatchRequestBodyBatchWithMetadata() error = %v", err)
	}
	if contentType != commandWireContentTypeProtobuf || contentEncoding != "" {
		t.Fatalf("schema wire content type/encoding = %q/%q, want protobuf/identity", contentType, contentEncoding)
	}
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("ReadAll(schema wire body) error = %v", err)
	}
	decoded, err := decodeCommandRequestProto(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("decodeCommandRequestProto() error = %v", err)
	}
	got, present, err := replicationSchemaMetadata(decoded)
	if err != nil || !present || got != contract {
		t.Fatalf("decoded schema contract = %#v present=%v error=%v, want %#v", got, present, err, contract)
	}
}

func TestReplicationSyncBatchCompressedWirePreservesSchemaContract(t *testing.T) {
	contract := NewReplicationSchemaContract(replicationSchemaFixture())
	body, contentType, contentEncoding, err := replicationSyncBatchRequestBodyBatchWithMetadata(
		replicationSyncPayloadBatch{inline: []replicationSyncPayload{{key: "key", binaryValue: []byte("value")}}},
		replicationSetCompactCommand,
		replicationSyncBatchMetadata{source: "node-a", sequence: 1, fingerprint: "topology", schema: contract},
		1,
	)
	if err != nil {
		t.Fatalf("compressed replicationSyncBatchRequestBodyBatchWithMetadata() error = %v", err)
	}
	if contentType != commandWireContentTypeProtobuf || contentEncoding != "gzip" {
		t.Fatalf("compressed schema wire content type/encoding = %q/%q, want protobuf/gzip", contentType, contentEncoding)
	}
	compressed, err := gzip.NewReader(body)
	if err != nil {
		t.Fatalf("gzip.NewReader() error = %v", err)
	}
	data, err := io.ReadAll(compressed)
	if err != nil {
		t.Fatalf("ReadAll(compressed schema wire body) error = %v", err)
	}
	if err := compressed.Close(); err != nil {
		t.Fatalf("gzip reader close error = %v", err)
	}
	decoded, err := decodeCommandRequestProto(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("decodeCommandRequestProto(compressed) error = %v", err)
	}
	got, present, err := replicationSchemaMetadata(decoded)
	if err != nil || !present || got != contract {
		t.Fatalf("decoded compressed schema contract = %#v present=%v error=%v, want %#v", got, present, err, contract)
	}
}

func TestReplicationGRPCStreamFlightPreservesSchemaContract(t *testing.T) {
	flight := &replicationGRPCStreamFlight{jobs: []*replicationGRPCStreamJob{{
		source:              "node-a",
		topologyFingerprint: "topology",
		schemaVersion:       7,
		schemaFingerprint:   "schema",
		payloads: replicationSyncPayloadBatch{inline: []replicationSyncPayload{{
			key: "key", binaryValue: []byte("value"),
		}}},
	}}, entries: 1}
	flight.buildRequest(1)
	if got := flight.request.GetSchemaVersion(); got != 7 {
		t.Fatalf("gRPC schema version = %d, want 7", got)
	}
	if got := flight.request.GetSchemaFingerprint(); got != "schema" {
		t.Fatalf("gRPC schema fingerprint = %q, want schema", got)
	}
}

func TestGRPCReplicationStreamRejectsMismatchedSchemaContract(t *testing.T) {
	contract := NewReplicationSchemaContract(replicationSchemaFixture())
	topology, err := NewTopologyStore(SingleNodeTopology("node-b", "http://node-b"))
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	server := NewCacheGRPCServer(newTestTrie(t), CacheGRPCOptions{
		Topology:                              topology,
		ReplicationSafety:                     NewReplicationSafetyStore(),
		ReplicationSchema:                     contract,
		RequireReplicationSchemaCompatibility: true,
	})
	ack := server.applyReplicationStreamBatch(context.Background(), &hatriecachev1.ReplicationStreamBatch{
		Source:              "node-a",
		Sequence:            1,
		TopologyFingerprint: topology.Fingerprint(),
		SchemaVersion:       contract.Version,
		SchemaFingerprint:   "different",
		Keys:                []string{"key"},
		BinaryValues:        [][]byte{{1}},
	})
	if ack.GetOk() || !strings.Contains(ack.GetMessage(), "schema contract") {
		t.Fatalf("gRPC schema mismatch ack = %#v, want rejection", ack)
	}
}
