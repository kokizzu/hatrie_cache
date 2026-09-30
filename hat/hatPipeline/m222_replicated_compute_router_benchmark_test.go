package hatPipeline

import "testing"

func BenchmarkM222ExistingReplicaScan(b *testing.B) {
	assignments := m222BenchmarkAssignments()
	unhealthy := make([]bool, len(assignments))
	unhealthy[0] = true
	unhealthy[1] = true
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		selected := -1
		for replica, assignment := range assignments {
			if !unhealthy[replica] && assignment.OperatorID == "index" {
				selected = replica
				break
			}
		}
		if selected < 0 {
			b.Fatal("no healthy replica")
		}
	}
}

func BenchmarkM222ReplicaRouterSelect(b *testing.B) {
	router, err := NewReplicatedComputeRouter(DataflowPlacementPlan{Assignments: m222BenchmarkAssignments()})
	if err != nil {
		b.Fatal(err)
	}
	if err := router.MarkWorkerUnhealthy("worker-a"); err != nil {
		b.Fatal(err)
	}
	if err := router.MarkWorkerUnhealthy("worker-b"); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := router.Select("index"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM222ReplicaRouterHealthToggle(b *testing.B) {
	router, err := NewReplicatedComputeRouter(DataflowPlacementPlan{Assignments: m222BenchmarkAssignments()})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		worker := "worker-a"
		if index&1 != 0 {
			worker = "worker-b"
		}
		if err := router.MarkWorkerUnhealthy(worker); err != nil {
			b.Fatal(err)
		}
		if err := router.MarkWorkerHealthy(worker); err != nil {
			b.Fatal(err)
		}
	}
}

func m222BenchmarkAssignments() []DataflowPlacementAssignment {
	assignments := make([]DataflowPlacementAssignment, 32)
	for replica := range assignments {
		assignments[replica] = DataflowPlacementAssignment{
			OperatorID:    "index",
			Replica:       replica,
			WorkerID:      "worker-" + string(rune('a'+replica)),
			FailureDomain: "az-" + string(rune('a'+replica)),
		}
	}
	return assignments
}
