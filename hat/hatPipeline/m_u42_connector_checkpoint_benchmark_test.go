package hatPipeline

import (
	"context"
	"encoding/json"
	"testing"
)

type connectorCheckpointBenchmarkConnector struct{}

func (connectorCheckpointBenchmarkConnector) Start(context.Context) error  { return nil }
func (connectorCheckpointBenchmarkConnector) Pause(context.Context) error  { return nil }
func (connectorCheckpointBenchmarkConnector) Resume(context.Context) error { return nil }
func (connectorCheckpointBenchmarkConnector) Stop(context.Context) error   { return nil }

type connectorCheckpointBenchmarkStore struct {
	payload []byte
}

func (s *connectorCheckpointBenchmarkStore) Load(context.Context) ([]byte, error) {
	return s.payload, nil
}

func (s *connectorCheckpointBenchmarkStore) Save(_ context.Context, payload []byte) error {
	s.payload = payload
	return nil
}

type connectorCheckpointJSON struct {
	ConnectorID string `json:"connector_id"`
	Sequence    uint64 `json:"sequence"`
	Generation  uint64 `json:"generation"`
	Offset      []byte `json:"offset"`
	Frontier    []byte `json:"frontier"`
}

var (
	connectorCheckpointBenchmarkBytes       []byte
	connectorCheckpointBenchmarkDecoded     ConnectorCheckpoint
	connectorCheckpointBenchmarkJSONDecoded connectorCheckpointJSON
)

func benchmarkConnectorCheckpointValue() ConnectorCheckpoint {
	return ConnectorCheckpoint{
		ConnectorID: "orders-eu",
		Sequence:    17,
		Generation:  23,
		Offset:      []byte("partition=3;offset=922337"),
		Frontier:    []byte("region-eu;watermark=1720000000"),
	}
}

func benchmarkConnectorCheckpointJSONValue() connectorCheckpointJSON {
	checkpoint := benchmarkConnectorCheckpointValue()
	return connectorCheckpointJSON{
		ConnectorID: checkpoint.ConnectorID,
		Sequence:    checkpoint.Sequence,
		Generation:  checkpoint.Generation,
		Offset:      checkpoint.Offset,
		Frontier:    checkpoint.Frontier,
	}
}

func BenchmarkConnectorCheckpointEncode(b *testing.B) {
	checkpoint := benchmarkConnectorCheckpointValue()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := EncodeConnectorCheckpoint(checkpoint)
		if err != nil {
			b.Fatal(err)
		}
		connectorCheckpointBenchmarkBytes = payload
	}
	b.SetBytes(int64(len(connectorCheckpointBenchmarkBytes)))
	b.ReportMetric(float64(len(connectorCheckpointBenchmarkBytes)), "wire-bytes/op")
}

func BenchmarkConnectorCheckpointEncodeJSON(b *testing.B) {
	checkpoint := benchmarkConnectorCheckpointJSONValue()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := json.Marshal(checkpoint)
		if err != nil {
			b.Fatal(err)
		}
		connectorCheckpointBenchmarkBytes = payload
	}
	b.SetBytes(int64(len(connectorCheckpointBenchmarkBytes)))
	b.ReportMetric(float64(len(connectorCheckpointBenchmarkBytes)), "wire-bytes/op")
}

func BenchmarkConnectorCheckpointDecode(b *testing.B) {
	payload, err := EncodeConnectorCheckpoint(benchmarkConnectorCheckpointValue())
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(payload)))
	b.ReportMetric(float64(len(payload)), "wire-bytes/op")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoded, err := DecodeConnectorCheckpoint(payload)
		if err != nil {
			b.Fatal(err)
		}
		connectorCheckpointBenchmarkDecoded = decoded
	}
}

func BenchmarkConnectorCheckpointDecodeJSON(b *testing.B) {
	payload, err := json.Marshal(benchmarkConnectorCheckpointJSONValue())
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(payload)))
	b.ReportMetric(float64(len(payload)), "wire-bytes/op")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded connectorCheckpointJSON
		if err := json.Unmarshal(payload, &decoded); err != nil {
			b.Fatal(err)
		}
		connectorCheckpointBenchmarkJSONDecoded = decoded
	}
}

func BenchmarkConnectorLifecyclePauseResumeBaseline(b *testing.B) {
	registry, err := benchmarkConnectorRegistry()
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := registry.Pause(ctx, "orders"); err != nil {
			b.Fatal(err)
		}
		if err := registry.Resume(ctx, "orders"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConnectorLifecyclePauseResumeCheckpoint(b *testing.B) {
	registry, err := benchmarkConnectorRegistry()
	if err != nil {
		b.Fatal(err)
	}
	store := &connectorCheckpointBenchmarkStore{}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		checkpoint, err := registry.PauseWithCheckpoint(ctx, "orders", ConnectorCheckpoint{
			Sequence: uint64(i + 1),
			Offset:   []byte("partition=3;offset=922337"),
			Frontier: []byte("region-eu;watermark=1720000000"),
		}, store)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := registry.ResumeFromCheckpoint(ctx, "orders", store, func(context.Context, ConnectorCheckpoint) error { return nil }); err != nil {
			b.Fatal(err)
		}
		connectorCheckpointBenchmarkDecoded = checkpoint
	}
}

func benchmarkConnectorRegistry() (*ConnectorRegistry, error) {
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		return nil, err
	}
	if err := registry.Register("orders", connectorCheckpointBenchmarkConnector{}); err != nil {
		return nil, err
	}
	if err := registry.Start(context.Background(), "orders"); err != nil {
		return nil, err
	}
	return registry, nil
}
