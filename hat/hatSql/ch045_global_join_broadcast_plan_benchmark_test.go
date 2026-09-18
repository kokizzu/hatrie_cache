package hatSql

import (
	"strconv"
	"testing"
)

func BenchmarkCH045NaivePerWorkerSubqueryAccounting(b *testing.B) {
	const workers = 8
	b.ReportAllocs()
	b.ResetTimer()
	fetches := 0
	for operation := 0; operation < b.N; operation++ {
		for worker := 0; worker < workers; worker++ {
			fetches++
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(fetches)/float64(b.N), "remote-fetches/op")
}

func BenchmarkCH045GlobalJoinBroadcastPlanHit(b *testing.B) {
	planner, err := NewGlobalJoinBroadcastPlanner(GlobalJoinBroadcastPlannerOptions{Workers: 8})
	if err != nil {
		b.Fatal(err)
	}
	request := GlobalJoinBroadcastRequest{Fingerprint: "global:customer", Epoch: 1, Rows: 1024, Bytes: 1 << 20}
	if _, err := planner.Plan(request); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	cacheHits := 0
	for operation := 0; operation < b.N; operation++ {
		plan, err := planner.Plan(request)
		if err != nil {
			b.Fatal(err)
		}
		if plan.CacheHit {
			cacheHits++
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(cacheHits)/float64(b.N), "cache-hits/op")
	b.ReportMetric(float64(request.Bytes*8), "fanout-bytes/op")
}

func BenchmarkCH045GlobalJoinBroadcastPlanCold(b *testing.B) {
	requests := make([]GlobalJoinBroadcastRequest, 1024)
	for index := range requests {
		requests[index] = GlobalJoinBroadcastRequest{
			Fingerprint: "global:customer:" + strconv.Itoa(index),
			Epoch:       1,
			Rows:        1024,
			Bytes:       1 << 20,
		}
	}
	planner, err := NewGlobalJoinBroadcastPlanner(GlobalJoinBroadcastPlannerOptions{Workers: 8, MaxCachedPlans: 1})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	fetches := 0
	for operation := 0; operation < b.N; operation++ {
		plan, err := planner.Plan(requests[operation%len(requests)])
		if err != nil {
			b.Fatal(err)
		}
		fetches += plan.RemoteSubqueryExecutions
	}
	b.StopTimer()
	b.ReportMetric(float64(fetches)/float64(b.N), "remote-fetches/op")
}
