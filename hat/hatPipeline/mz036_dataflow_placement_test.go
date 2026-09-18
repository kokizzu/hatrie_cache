package hatPipeline

import (
	"errors"
	"reflect"
	"testing"
)

func TestDataflowOperatorPlacementSpreadsReplicasAndIsDeterministic(t *testing.T) {
	workers := []DataflowPlacementWorker{
		{ID: "worker-c", FailureDomain: "zone-c", Capacity: 2},
		{ID: "worker-a", FailureDomain: "zone-a", Capacity: 2},
		{ID: "worker-d", FailureDomain: "zone-a", Capacity: 2},
		{ID: "worker-b", FailureDomain: "zone-b", Capacity: 2},
	}
	operators := []DataflowPlacementOperator{
		{ID: "source", Replicas: 2, MinFailureDomains: 2, AllowedFailureDomains: []string{"zone-b", "zone-c"}},
		{ID: "join", Replicas: 3, MinFailureDomains: 3},
	}
	first, err := PlanDataflowOperatorPlacement(operators, workers, DataflowPlacementOptions{})
	if err != nil {
		t.Fatalf("PlanDataflowOperatorPlacement() error = %v", err)
	}
	want := []DataflowPlacementAssignment{
		{OperatorID: "join", Replica: 0, WorkerID: "worker-a", FailureDomain: "zone-a"},
		{OperatorID: "join", Replica: 1, WorkerID: "worker-b", FailureDomain: "zone-b"},
		{OperatorID: "join", Replica: 2, WorkerID: "worker-c", FailureDomain: "zone-c"},
		{OperatorID: "source", Replica: 0, WorkerID: "worker-b", FailureDomain: "zone-b"},
		{OperatorID: "source", Replica: 1, WorkerID: "worker-c", FailureDomain: "zone-c"},
	}
	if !reflect.DeepEqual(first.Assignments, want) {
		t.Fatalf("Assignments = %#v, want %#v", first.Assignments, want)
	}
	second, err := PlanDataflowOperatorPlacement([]DataflowPlacementOperator{operators[1], operators[0]}, workers, DataflowPlacementOptions{})
	if err != nil {
		t.Fatalf("reordered PlanDataflowOperatorPlacement() error = %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("reordered plan = %#v, want deterministic plan %#v", second, first)
	}
}

func TestDataflowOperatorPlacementRejectsUnsatisfiedConstraints(t *testing.T) {
	workers := []DataflowPlacementWorker{
		{ID: "a", FailureDomain: "zone-a", Capacity: 2},
		{ID: "b", FailureDomain: "zone-a", Capacity: 2},
	}
	if _, err := PlanDataflowOperatorPlacement([]DataflowPlacementOperator{{ID: "join", Replicas: 2, MinFailureDomains: 2}}, workers, DataflowPlacementOptions{}); !errors.Is(err, ErrDataflowPlacementUnsatisfied) {
		t.Fatalf("same-domain plan error = %v, want unsatisfied", err)
	}
	if _, err := PlanDataflowOperatorPlacement([]DataflowPlacementOperator{{ID: "join", Replicas: 3}}, workers, DataflowPlacementOptions{}); !errors.Is(err, ErrDataflowPlacementUnsatisfied) {
		t.Fatalf("capacity plan error = %v, want unsatisfied", err)
	}
	if _, err := PlanDataflowOperatorPlacement([]DataflowPlacementOperator{{ID: "join", Replicas: 1, AllowedFailureDomains: []string{"zone-z"}}}, workers, DataflowPlacementOptions{}); !errors.Is(err, ErrDataflowPlacementUnsatisfied) {
		t.Fatalf("allowed-domain plan error = %v, want unsatisfied", err)
	}
}

func TestDataflowOperatorPlacementValidatesInputsAndBounds(t *testing.T) {
	if _, err := PlanDataflowOperatorPlacement(nil, nil, DataflowPlacementOptions{}); !errors.Is(err, ErrDataflowPlacementInvalid) {
		t.Fatalf("empty plan error = %v, want invalid", err)
	}
	if _, err := PlanDataflowOperatorPlacement([]DataflowPlacementOperator{{ID: "op"}}, []DataflowPlacementWorker{{ID: "worker", FailureDomain: "zone", Capacity: -1}}, DataflowPlacementOptions{}); !errors.Is(err, ErrDataflowPlacementInvalid) {
		t.Fatalf("negative capacity error = %v, want invalid", err)
	}
	if _, err := PlanDataflowOperatorPlacement([]DataflowPlacementOperator{{ID: "op", Replicas: 2, MinFailureDomains: 3}}, []DataflowPlacementWorker{{ID: "worker", FailureDomain: "zone", Capacity: 2}}, DataflowPlacementOptions{}); !errors.Is(err, ErrDataflowPlacementInvalid) {
		t.Fatalf("too many domains error = %v, want invalid", err)
	}
	if _, err := PlanDataflowOperatorPlacement([]DataflowPlacementOperator{{ID: "op"}, {ID: "op"}}, []DataflowPlacementWorker{{ID: "worker", FailureDomain: "zone", Capacity: 2}}, DataflowPlacementOptions{}); !errors.Is(err, ErrDataflowPlacementInvalid) {
		t.Fatalf("duplicate operator error = %v, want invalid", err)
	}
}
