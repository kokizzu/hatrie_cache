package hatPipeline

import (
	"encoding/json"
	"testing"
)

func BenchmarkC154fSchemaBarrierJSONSnapshotBaseline(b *testing.B) {
	barrier := c154fBarrierBenchmarkFixture(b)
	payload, err := json.Marshal(barrier.Snapshot())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(payload)), "wire-bytes")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, err := json.Marshal(barrier.Snapshot())
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(payload)))
	}
	b.ReportMetric(float64(len(payload)), "wire-bytes")
}

func BenchmarkC154fSchemaBarrierBinarySnapshot(b *testing.B) {
	barrier := c154fBarrierBenchmarkFixture(b)
	payload, err := barrier.MarshalSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(payload)), "wire-bytes")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, err := barrier.MarshalSnapshot()
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(payload)))
	}
	b.ReportMetric(float64(len(payload)), "wire-bytes")
}

func BenchmarkC154fSchemaBarrierJSONRestoreBaseline(b *testing.B) {
	barrier := c154fBarrierBenchmarkFixture(b)
	payload, err := json.Marshal(barrier.Snapshot())
	if err != nil {
		b.Fatal(err)
	}
	target, err := NewSchemaMigrationBarrier(SchemaMigrationBarrierOptions{MaxBarriers: 64, MaxDependencies: 8})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(payload)), "wire-bytes")
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := restoreC154fJSONSnapshot(target, payload); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(payload)), "wire-bytes")
}

func BenchmarkC154fSchemaBarrierBinaryRestore(b *testing.B) {
	barrier := c154fBarrierBenchmarkFixture(b)
	payload, err := barrier.MarshalSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(payload)), "wire-bytes")
	target, err := NewSchemaMigrationBarrier(SchemaMigrationBarrierOptions{MaxBarriers: 64, MaxDependencies: 8})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := target.RestoreSnapshot(payload); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(payload)), "wire-bytes")
}

func restoreC154fJSONSnapshot(barrier *SchemaMigrationBarrier, payload []byte) error {
	var statuses []SchemaMigrationBarrierStatus
	if err := json.Unmarshal(payload, &statuses); err != nil {
		return err
	}
	entries := make(map[string]*schemaMigrationBarrierEntry, len(statuses))
	for _, status := range statuses {
		if err := validateSchemaMigrationBarrierSnapshotStatus(status); err != nil {
			return err
		}
		dependencies := make(map[string]struct{}, len(status.Dependencies))
		for _, dependency := range status.Dependencies {
			dependencies[dependency] = struct{}{}
		}
		acknowledged := make(map[string]struct{}, len(status.AcknowledgedDependencies))
		for _, dependency := range status.AcknowledgedDependencies {
			acknowledged[dependency] = struct{}{}
		}
		entries[status.ID] = &schemaMigrationBarrierEntry{
			status:       status,
			dependencies: dependencies,
			acknowledged: acknowledged,
		}
	}
	barrier.mu.Lock()
	barrier.barriers = entries
	barrier.mu.Unlock()
	return nil
}
