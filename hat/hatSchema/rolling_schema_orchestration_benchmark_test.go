package hatSchema

import (
	"context"
	"testing"
)

var rollingSchemaOrchestrationBaselineSink RollingSchemaPhase

func BenchmarkRollingSchemaManualTransitions(b *testing.B) {
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"node-a", "node-b", "node-c", "node-d"})
	if err != nil {
		b.Fatal(err)
	}
	nodes := plan.Nodes()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		deployment := plan.Begin()
		for _, node := range nodes {
			if err := deployment.Prepare(node); err != nil {
				b.Fatal(err)
			}
			if err := deployment.Activate(node); err != nil {
				b.Fatal(err)
			}
		}
		phase, ok := deployment.Phase(nodes[index%len(nodes)])
		if !ok {
			b.Fatal("benchmark node disappeared")
		}
		rollingSchemaOrchestrationBaselineSink = phase
	}
}

func BenchmarkRollingSchemaDeploymentRun(b *testing.B) {
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"node-a", "node-b", "node-c", "node-d"})
	if err != nil {
		b.Fatal(err)
	}
	install := RollingSchemaInstallFunc(func(context.Context, string, Schema) error { return nil })
	activate := RollingSchemaActivateFunc(func(context.Context, string, Schema) error { return nil })
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		deployment := plan.Begin()
		if err := plan.Run(context.Background(), deployment, install, activate); err != nil {
			b.Fatal(err)
		}
		phase, ok := deployment.Phase(plan.nodes[index%len(plan.nodes)])
		if !ok {
			b.Fatal("benchmark node disappeared")
		}
		rollingSchemaOrchestrationBaselineSink = phase
	}
}
