package hatPipeline

import "testing"

var mz036PlacementBenchmarkSink int

func BenchmarkMZ036NaiveRoundRobin(b *testing.B) {
	workers, operators := mz036BenchmarkInput()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		assignmentCount := 0
		for _, operator := range operators {
			for replica := 0; replica < operator.Replicas; replica++ {
				worker := workers[assignmentCount%len(workers)]
				assignmentCount += replica + len(worker.ID) + len(worker.FailureDomain)
			}
		}
		mz036PlacementBenchmarkSink = assignmentCount
	}
}

func BenchmarkMZ036PlacementSolver(b *testing.B) {
	workers, operators := mz036BenchmarkInput()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		plan, err := PlanDataflowOperatorPlacement(operators, workers, DataflowPlacementOptions{})
		if err != nil {
			b.Fatal(err)
		}
		mz036PlacementBenchmarkSink = len(plan.Assignments)
	}
}

func mz036BenchmarkInput() ([]DataflowPlacementWorker, []DataflowPlacementOperator) {
	workers := make([]DataflowPlacementWorker, 16)
	for index := range workers {
		workers[index] = DataflowPlacementWorker{
			ID:            string(rune('a' + index)),
			FailureDomain: "zone-" + string(rune('a'+index%4)),
			Capacity:      64,
		}
	}
	operators := make([]DataflowPlacementOperator, 32)
	for index := range operators {
		operators[index] = DataflowPlacementOperator{
			ID:                "operator-" + string(rune('a'+index)),
			Replicas:          3,
			MinFailureDomains: 3,
		}
	}
	return workers, operators
}
