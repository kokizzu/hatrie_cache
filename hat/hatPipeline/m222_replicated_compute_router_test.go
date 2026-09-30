package hatPipeline_test

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"hatrie_cache/hat/hatPipeline"
)

func TestM222ReplicatedComputeRouterSelectsHealthyReplica(t *testing.T) {
	router, err := hatPipeline.NewReplicatedComputeRouter(m222ReplicaPlan())
	if err != nil {
		t.Fatalf("NewReplicatedComputeRouter() error = %v", err)
	}

	assignment, err := router.Select("orders-index")
	if err != nil {
		t.Fatalf("initial Select() error = %v", err)
	}
	if assignment.WorkerID != "worker-a" || assignment.Replica != 0 {
		t.Fatalf("initial assignment = %#v, want worker-a replica 0", assignment)
	}

	if err := router.MarkWorkerUnhealthy("worker-a"); err != nil {
		t.Fatalf("MarkWorkerUnhealthy(worker-a) error = %v", err)
	}
	assignment, err = router.Select("orders-index")
	if err != nil {
		t.Fatalf("fallback Select() error = %v", err)
	}
	if assignment.WorkerID != "worker-b" || assignment.Replica != 1 {
		t.Fatalf("fallback assignment = %#v, want worker-b replica 1", assignment)
	}

	if err := router.MarkWorkerUnhealthy("worker-b"); err != nil {
		t.Fatalf("MarkWorkerUnhealthy(worker-b) error = %v", err)
	}
	assignment, err = router.Select("orders-index")
	if err != nil {
		t.Fatalf("second fallback Select() error = %v", err)
	}
	if assignment.WorkerID != "worker-c" || assignment.Replica != 2 {
		t.Fatalf("second fallback assignment = %#v, want worker-c replica 2", assignment)
	}

	if err := router.MarkWorkerUnhealthy("worker-c"); err != nil {
		t.Fatalf("MarkWorkerUnhealthy(worker-c) error = %v", err)
	}
	if _, err := router.Select("orders-index"); !errors.Is(err, hatPipeline.ErrReplicatedComputeRouterUnavailable) {
		t.Fatalf("all-unhealthy Select() error = %v, want %v", err, hatPipeline.ErrReplicatedComputeRouterUnavailable)
	}

	if err := router.MarkWorkerHealthy("worker-a"); err != nil {
		t.Fatalf("MarkWorkerHealthy(worker-a) error = %v", err)
	}
	assignment, err = router.Select("orders-index")
	if err != nil {
		t.Fatalf("recovered Select() error = %v", err)
	}
	if assignment.WorkerID != "worker-a" || assignment.Replica != 0 {
		t.Fatalf("recovered assignment = %#v, want worker-a replica 0", assignment)
	}
}

func TestM222ReplicatedComputeRouterSharesWorkerHealthAcrossOperators(t *testing.T) {
	plan := hatPipeline.DataflowPlacementPlan{Assignments: []hatPipeline.DataflowPlacementAssignment{
		{OperatorID: "a", Replica: 0, WorkerID: "worker-a", FailureDomain: "az-a"},
		{OperatorID: "a", Replica: 1, WorkerID: "worker-b", FailureDomain: "az-b"},
		{OperatorID: "b", Replica: 0, WorkerID: "worker-b", FailureDomain: "az-b"},
		{OperatorID: "b", Replica: 1, WorkerID: "worker-c", FailureDomain: "az-c"},
	}}
	router, err := hatPipeline.NewReplicatedComputeRouter(plan)
	if err != nil {
		t.Fatalf("NewReplicatedComputeRouter() error = %v", err)
	}
	if err := router.MarkWorkerUnhealthy("worker-b"); err != nil {
		t.Fatalf("MarkWorkerUnhealthy(worker-b) error = %v", err)
	}
	for operator, want := range map[string]string{"a": "worker-a", "b": "worker-c"} {
		assignment, err := router.Select(operator)
		if err != nil {
			t.Fatalf("Select(%q) error = %v", operator, err)
		}
		if assignment.WorkerID != want {
			t.Fatalf("Select(%q) worker = %q, want %q", operator, assignment.WorkerID, want)
		}
	}
}

func TestM222ReplicatedComputeRouterValidationAndWorkerStates(t *testing.T) {
	if _, err := hatPipeline.NewReplicatedComputeRouter(hatPipeline.DataflowPlacementPlan{}); !errors.Is(err, hatPipeline.ErrReplicatedComputeRouterInvalid) {
		t.Fatalf("empty plan error = %v, want %v", err, hatPipeline.ErrReplicatedComputeRouterInvalid)
	}
	router, err := hatPipeline.NewReplicatedComputeRouter(m222ReplicaPlan())
	if err != nil {
		t.Fatalf("NewReplicatedComputeRouter() error = %v", err)
	}
	if _, err := router.Select("missing"); !errors.Is(err, hatPipeline.ErrReplicatedComputeRouterOperatorNotFound) {
		t.Fatalf("missing operator error = %v, want %v", err, hatPipeline.ErrReplicatedComputeRouterOperatorNotFound)
	}
	if err := router.MarkWorkerUnhealthy("missing"); !errors.Is(err, hatPipeline.ErrReplicatedComputeRouterWorkerNotFound) {
		t.Fatalf("missing worker error = %v, want %v", err, hatPipeline.ErrReplicatedComputeRouterWorkerNotFound)
	}
	if err := router.MarkWorkerUnhealthy("worker-b"); err != nil {
		t.Fatalf("MarkWorkerUnhealthy(worker-b) error = %v", err)
	}
	states := router.WorkerStates()
	wantStates := []hatPipeline.ReplicatedComputeWorkerState{
		{WorkerID: "worker-a", Healthy: true},
		{WorkerID: "worker-b", Healthy: false},
		{WorkerID: "worker-c", Healthy: true},
	}
	if !reflect.DeepEqual(states, wantStates) {
		t.Fatalf("WorkerStates() = %#v, want %#v", states, wantStates)
	}
}

func TestM222ReplicatedComputeRouterRejectsDuplicateAndGappedReplicas(t *testing.T) {
	tests := map[string]hatPipeline.DataflowPlacementPlan{
		"duplicate worker": {Assignments: []hatPipeline.DataflowPlacementAssignment{
			{OperatorID: "index", Replica: 0, WorkerID: "worker-a"},
			{OperatorID: "index", Replica: 1, WorkerID: "worker-a"},
		}},
		"gapped replica": {Assignments: []hatPipeline.DataflowPlacementAssignment{
			{OperatorID: "index", Replica: 1, WorkerID: "worker-a"},
		}},
	}
	for name, plan := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := hatPipeline.NewReplicatedComputeRouter(plan); !errors.Is(err, hatPipeline.ErrReplicatedComputeRouterInvalid) {
				t.Fatalf("error = %v, want %v", err, hatPipeline.ErrReplicatedComputeRouterInvalid)
			}
		})
	}
}

func TestM222ReplicatedComputeRouterConcurrentHealthChanges(t *testing.T) {
	router, err := hatPipeline.NewReplicatedComputeRouter(m222ReplicaPlan())
	if err != nil {
		t.Fatalf("NewReplicatedComputeRouter() error = %v", err)
	}
	var group sync.WaitGroup
	for worker := range []string{"worker-a", "worker-b", "worker-c"} {
		workerID := []string{"worker-a", "worker-b", "worker-c"}[worker]
		group.Add(1)
		go func() {
			defer group.Done()
			for iteration := 0; iteration < 1000; iteration++ {
				if iteration&1 == 0 {
					if err := router.MarkWorkerUnhealthy(workerID); err != nil {
						t.Errorf("MarkWorkerUnhealthy(%q) error = %v", workerID, err)
					}
				} else if err := router.MarkWorkerHealthy(workerID); err != nil {
					t.Errorf("MarkWorkerHealthy(%q) error = %v", workerID, err)
				}
				_, _ = router.Select("orders-index")
			}
		}()
	}
	group.Wait()
}

func m222ReplicaPlan() hatPipeline.DataflowPlacementPlan {
	return hatPipeline.DataflowPlacementPlan{Assignments: []hatPipeline.DataflowPlacementAssignment{
		{OperatorID: "orders-index", Replica: 0, WorkerID: "worker-a", FailureDomain: "az-a"},
		{OperatorID: "orders-index", Replica: 1, WorkerID: "worker-b", FailureDomain: "az-b"},
		{OperatorID: "orders-index", Replica: 2, WorkerID: "worker-c", FailureDomain: "az-c"},
	}}
}
