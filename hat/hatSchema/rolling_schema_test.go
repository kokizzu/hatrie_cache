package hatSchema

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestRollingSchemaDeploymentTransitionsDeterministically(t *testing.T) {
	previous := rollingSchemaFixture(1, false)
	next := rollingSchemaFixture(2, true)
	plan, err := NewRollingSchemaPlan(previous, next, []string{"node-b", " node-a "})
	if err != nil {
		t.Fatalf("NewRollingSchemaPlan() error = %v", err)
	}
	previous.Sources["users"].Columns[0].Name = "mutated"
	next.Sources["users"].Columns[1].Name = "mutated"
	if got := plan.Nodes(); !reflect.DeepEqual(got, []string{"node-a", "node-b"}) {
		t.Fatalf("plan.Nodes() = %#v, want sorted independent nodes", got)
	}
	if got := plan.PreviousSchema().Sources["users"].Columns[0].Name; got != "id" {
		t.Fatalf("plan previous schema was aliased: column = %q", got)
	}
	if got := plan.NextSchema().Sources["users"].Columns[1].Name; got != "name" {
		t.Fatalf("plan next schema was aliased: column = %q", got)
	}

	deployment := plan.Begin()
	if got, ok := deployment.Phase("node-a"); !ok || got != RollingSchemaPhasePending {
		t.Fatalf("initial node-a phase = %v/%v, want pending/true", got, ok)
	}
	if err := deployment.Activate("node-a"); !errors.Is(err, ErrRollingSchemaTransition) {
		t.Fatalf("Activate() before Prepare error = %v, want transition error", err)
	}
	if err := deployment.Prepare("node-a"); err != nil {
		t.Fatalf("Prepare(node-a) error = %v", err)
	}
	if err := deployment.Prepare("node-a"); err != nil {
		t.Fatalf("idempotent Prepare(node-a) error = %v", err)
	}
	if err := deployment.Activate("node-a"); err != nil {
		t.Fatalf("Activate(node-a) error = %v", err)
	}
	if err := deployment.Activate("node-a"); err != nil {
		t.Fatalf("idempotent Activate(node-a) error = %v", err)
	}
	if deployment.Complete() {
		t.Fatal("deployment complete after only one node activated")
	}
	if err := deployment.Prepare("node-b"); err != nil {
		t.Fatalf("Prepare(node-b) error = %v", err)
	}
	if err := deployment.Activate("node-b"); err != nil {
		t.Fatalf("Activate(node-b) error = %v", err)
	}
	if !deployment.Complete() {
		t.Fatal("deployment incomplete after every node activated")
	}
	want := []RollingSchemaNode{
		{Node: "node-a", Phase: RollingSchemaPhaseActive},
		{Node: "node-b", Phase: RollingSchemaPhaseActive},
	}
	if got := deployment.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("deployment.Snapshot() = %#v, want %#v", got, want)
	}
	if _, ok := deployment.Phase("missing"); ok {
		t.Fatal("Phase(missing) reported an unknown node")
	}
	if err := deployment.Prepare("missing"); !errors.Is(err, ErrRollingSchemaNodeUnknown) {
		t.Fatalf("Prepare(missing) error = %v, want unknown-node error", err)
	}
}

func TestRollingSchemaDeploymentRejectsInvalidPlans(t *testing.T) {
	previous := rollingSchemaFixture(1, false)
	unsafe := rollingSchemaFixture(2, false)
	unsafe.Sources["users"].Columns[0].Type = TypeText
	cases := []struct {
		name    string
		next    Schema
		nodes   []string
		wantErr error
	}{
		{name: "unsafe type change", next: unsafe, nodes: []string{"node-a"}, wantErr: ErrRollingSchemaPlanInvalid},
		{name: "duplicate node", next: rollingSchemaFixture(2, true), nodes: []string{"node-a", "node-a"}, wantErr: ErrRollingSchemaPlanInvalid},
		{name: "empty node", next: rollingSchemaFixture(2, true), nodes: []string{""}, wantErr: ErrRollingSchemaPlanInvalid},
		{name: "no nodes", next: rollingSchemaFixture(2, true), nodes: nil, wantErr: ErrRollingSchemaPlanInvalid},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewRollingSchemaPlan(previous, test.next, test.nodes); !errors.Is(err, test.wantErr) {
				t.Fatalf("NewRollingSchemaPlan() error = %v, want %v", err, test.wantErr)
			}
		})
	}
	var zero RollingSchemaPlan
	if got := zero.Begin().Snapshot(); got != nil {
		t.Fatalf("zero plan snapshot = %#v, want nil", got)
	}
}

func TestRollingSchemaDeploymentSupportsConcurrentNodeTransitions(t *testing.T) {
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"a", "b", "c", "d"})
	if err != nil {
		t.Fatal(err)
	}
	deployment := plan.Begin()
	var waitGroup sync.WaitGroup
	for _, node := range plan.Nodes() {
		node := node
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			if err := deployment.Prepare(node); err != nil {
				t.Errorf("Prepare(%q) error = %v", node, err)
				return
			}
			if err := deployment.Activate(node); err != nil {
				t.Errorf("Activate(%q) error = %v", node, err)
			}
		}()
	}
	waitGroup.Wait()
	if !deployment.Complete() {
		t.Fatalf("concurrent deployment snapshot = %#v, want complete", deployment.Snapshot())
	}
}

func rollingSchemaFixture(version uint64, addName bool) Schema {
	columns := []Column{{Name: "id", Type: TypeInteger}}
	if addName {
		columns = append(columns, Column{Name: "name", Type: TypeText})
	}
	return Schema{Version: version, Sources: map[string]Source{
		"users": {Name: "users", Columns: columns},
	}}
}

var rollingSchemaDeploymentBenchmarkSink RollingSchemaPhase

func BenchmarkRollingSchemaDeploymentPhase(b *testing.B) {
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"node-a", "node-b", "node-c", "node-d"})
	if err != nil {
		b.Fatal(err)
	}
	deployment := plan.Begin()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		phase, ok := deployment.Phase(plan.nodes[index%len(plan.nodes)])
		if !ok {
			b.Fatal("benchmark node disappeared")
		}
		rollingSchemaDeploymentBenchmarkSink = phase
	}
}
