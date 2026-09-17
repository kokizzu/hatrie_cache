package hatPipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

var (
	mu01DecodedSnapshotSink ConnectorRegistrySnapshot
	mu01StatusSnapshotSink  ConnectorStatus
	mu01StateSnapshotSink   ConnectorRegistrySnapshot
	mu01BinarySnapshotSink  []byte
	mu01JSONSnapshotSink    []byte
)

func BenchmarkMU01ConnectorRegistryStatusSnapshot(b *testing.B) {
	registry := newMU01BenchmarkRegistry(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		statuses := registry.Snapshot()
		if len(statuses) == 0 {
			b.Fatal("Snapshot() returned no statuses")
		}
		mu01StatusSnapshotSink = statuses[len(statuses)-1]
	}
}

func BenchmarkMU01ConnectorRegistryStateSnapshot(b *testing.B) {
	registry := newMU01BenchmarkRegistry(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot := registry.SnapshotState()
		if len(snapshot.Connectors) == 0 {
			b.Fatal("SnapshotState() returned no connectors")
		}
		mu01StateSnapshotSink = snapshot
	}
}

func BenchmarkMU01ConnectorRegistryBinarySnapshot(b *testing.B) {
	registry := newMU01BenchmarkRegistry(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := registry.MarshalSnapshot()
		if err != nil {
			b.Fatal(err)
		}
		mu01BinarySnapshotSink = payload
	}
	b.SetBytes(int64(len(mu01BinarySnapshotSink)))
}

func BenchmarkMU01ConnectorRegistryBinaryCodec(b *testing.B) {
	registry := newMU01BenchmarkRegistry(b)
	snapshot := registry.SnapshotState()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := marshalConnectorRegistrySnapshot(snapshot)
		if err != nil {
			b.Fatal(err)
		}
		mu01BinarySnapshotSink = payload
	}
	b.SetBytes(int64(len(mu01BinarySnapshotSink)))
}

func BenchmarkMU01ConnectorRegistryJSONCodec(b *testing.B) {
	registry := newMU01BenchmarkRegistry(b)
	snapshot := registry.SnapshotState()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := json.Marshal(snapshot)
		if err != nil {
			b.Fatal(err)
		}
		mu01JSONSnapshotSink = payload
	}
	b.SetBytes(int64(len(mu01JSONSnapshotSink)))
}

func BenchmarkMU01ConnectorRegistryBinaryDecode(b *testing.B) {
	registry := newMU01BenchmarkRegistry(b)
	payload, err := registry.MarshalSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot, err := UnmarshalConnectorRegistrySnapshot(payload)
		if err != nil {
			b.Fatal(err)
		}
		mu01DecodedSnapshotSink = snapshot
	}
}

func BenchmarkMU01ConnectorRegistryJSONDecode(b *testing.B) {
	registry := newMU01BenchmarkRegistry(b)
	snapshot := registry.SnapshotState()
	payload, err := json.Marshal(snapshot)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded ConnectorRegistrySnapshot
		if err := json.Unmarshal(payload, &decoded); err != nil {
			b.Fatal(err)
		}
		mu01DecodedSnapshotSink = decoded
	}
}

func TestMU01ConnectorSnapshotBenchmarkFixtureWireSize(t *testing.T) {
	registry := newMU01BenchmarkRegistry(t)
	binaryPayload, err := registry.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	jsonPayload, err := json.Marshal(registry.SnapshotState())
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	t.Logf("64-connector snapshot wire bytes: binary=%d json=%d", len(binaryPayload), len(jsonPayload))
}

type mu01BenchmarkReporter interface {
	Helper()
	Fatal(...any)
}

func newMU01BenchmarkRegistry(b mu01BenchmarkReporter) *ConnectorRegistry {
	b.Helper()
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{HistoryLimit: 8})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		id := fmt.Sprintf("connector-%03d", i)
		if err := registry.Register(id, benchmarkLifecycleConnector{}); err != nil {
			b.Fatal(err)
		}
		if err := registry.Start(context.Background(), id); err != nil {
			b.Fatal(err)
		}
		if i%2 == 0 {
			if err := registry.Pause(context.Background(), id); err != nil {
				b.Fatal(err)
			}
		}
	}
	return registry
}
