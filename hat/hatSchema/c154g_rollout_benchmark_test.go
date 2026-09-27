package hatSchema

import (
	"context"
	"testing"
)

var c154gRolloutBenchmarkSink RollingSchemaPhase
var c154gCheckpointBenchmarkSink RollingSchemaCheckpoint

func BenchmarkC154gRollingSchemaRunBaseline(b *testing.B) {
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"node-a", "node-b", "node-c", "node-d"})
	if err != nil {
		b.Fatal(err)
	}
	install := RollingSchemaInstallFunc(func(context.Context, string, Schema) error { return nil })
	activate := RollingSchemaActivateFunc(func(context.Context, string, Schema) error { return nil })
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		deployment := plan.Begin()
		if err := plan.Run(context.Background(), deployment, install, activate); err != nil {
			b.Fatal(err)
		}
		phase, ok := deployment.Phase("node-d")
		if !ok {
			b.Fatal("benchmark node disappeared")
		}
		c154gRolloutBenchmarkSink = phase
	}
}

func BenchmarkC154gRollingSchemaRunWithCheckpoint(b *testing.B) {
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"node-a", "node-b", "node-c", "node-d"})
	if err != nil {
		b.Fatal(err)
	}
	install := RollingSchemaInstallFunc(func(context.Context, string, Schema) error { return nil })
	activate := RollingSchemaActivateFunc(func(context.Context, string, Schema) error { return nil })
	persist := RollingSchemaCheckpointPersistFunc(func(_ context.Context, checkpoint RollingSchemaCheckpoint) error {
		c154gCheckpointBenchmarkSink = checkpoint
		return nil
	})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		deployment := plan.Begin()
		if err := plan.RunWithCheckpoint(context.Background(), deployment, persist, install, activate); err != nil {
			b.Fatal(err)
		}
		if !deployment.Complete() {
			b.Fatal("benchmark deployment did not complete")
		}
	}
}
