package hatSchema

import "testing"

var rollingSchemaCheckpointBenchmarkSink interface{}

func benchmarkRollingSchemaCheckpointFixture(b *testing.B) (RollingSchemaPlan, RollingSchemaCheckpoint, []byte) {
	b.Helper()
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"node-a", "node-b", "node-c", "node-d"})
	if err != nil {
		b.Fatal(err)
	}
	deployment := plan.Begin()
	for _, node := range plan.Nodes() {
		if err := deployment.Prepare(node); err != nil {
			b.Fatal(err)
		}
		if err := deployment.Activate(node); err != nil {
			b.Fatal(err)
		}
	}
	checkpoint, err := plan.Checkpoint(deployment)
	if err != nil {
		b.Fatal(err)
	}
	wire, err := checkpoint.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	return plan, checkpoint, wire
}

func BenchmarkRollingSchemaManualRecovery(b *testing.B) {
	plan, _, _ := benchmarkRollingSchemaCheckpointFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		deployment := plan.Begin()
		for _, node := range plan.Nodes() {
			if err := deployment.Prepare(node); err != nil {
				b.Fatal(err)
			}
			if err := deployment.Activate(node); err != nil {
				b.Fatal(err)
			}
		}
		rollingSchemaCheckpointBenchmarkSink = deployment
	}
}

func BenchmarkRollingSchemaCheckpointRestore(b *testing.B) {
	plan, checkpoint, _ := benchmarkRollingSchemaCheckpointFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		deployment, err := plan.Restore(checkpoint)
		if err != nil {
			b.Fatal(err)
		}
		rollingSchemaCheckpointBenchmarkSink = deployment
	}
}

func BenchmarkRollingSchemaCheckpointMarshal(b *testing.B) {
	_, checkpoint, _ := benchmarkRollingSchemaCheckpointFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		wire, err := checkpoint.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		rollingSchemaCheckpointBenchmarkSink = wire
	}
}

func BenchmarkRollingSchemaCheckpointUnmarshal(b *testing.B) {
	_, _, wire := benchmarkRollingSchemaCheckpointFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		checkpoint, err := DecodeRollingSchemaCheckpoint(wire)
		if err != nil {
			b.Fatal(err)
		}
		rollingSchemaCheckpointBenchmarkSink = checkpoint
	}
}

func BenchmarkRollingSchemaCheckpointRoundTrip(b *testing.B) {
	plan, checkpoint, _ := benchmarkRollingSchemaCheckpointFixture(b)
	wire, err := checkpoint.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		decoded, err := DecodeRollingSchemaCheckpoint(wire)
		if err != nil {
			b.Fatal(err)
		}
		deployment, err := plan.Restore(decoded)
		if err != nil {
			b.Fatal(err)
		}
		rollingSchemaCheckpointBenchmarkSink = deployment
	}
}
