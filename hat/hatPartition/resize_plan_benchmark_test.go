package hatPartition

import "testing"

var partitionResizeBenchmarkSink int
var partitionResizeBenchmarkSourceSink int

func BenchmarkPartitionIndex(b *testing.B) {
	const key = "region:ap-southeast-1/customer/00000042"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		partitionResizeBenchmarkSink = Index(key, 16)
	}
}

func BenchmarkPartitionResizePlanTarget(b *testing.B) {
	plan, err := PlanSplit(16)
	if err != nil {
		b.Fatal(err)
	}
	const key = "region:ap-southeast-1/customer/00000042"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		partitionResizeBenchmarkSink = plan.TargetPartition(key)
	}
}

func BenchmarkPartitionResizePlanRoute(b *testing.B) {
	plan, err := PlanSplit(16)
	if err != nil {
		b.Fatal(err)
	}
	const key = "region:ap-southeast-1/customer/00000042"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		partitionResizeBenchmarkSourceSink, partitionResizeBenchmarkSink, _ = plan.RouteKey(key)
	}
}

func BenchmarkPartitionResizePlanTwoLookups(b *testing.B) {
	plan, err := PlanSplit(16)
	if err != nil {
		b.Fatal(err)
	}
	const key = "region:ap-southeast-1/customer/00000042"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		partitionResizeBenchmarkSourceSink = plan.SourcePartition(key)
		partitionResizeBenchmarkSink = plan.TargetPartition(key)
	}
}
