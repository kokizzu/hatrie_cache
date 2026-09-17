package hatSql

import "testing"

var mu024PriorityBenchmarkSink int

func BenchmarkMU024AfterLegacyFIFOSelection(b *testing.B) {
	pool := &sqlClusterAdmissionPoolState{
		limits:  SQLClusterAdmissionPool{CPUUnits: 1, MaxRunning: 1},
		waiters: make([]*sqlClusterAdmissionWaiter, 32),
	}
	for index := range pool.waiters {
		pool.waiters[index] = &sqlClusterAdmissionWaiter{
			request: SQLClusterAdmissionRequest{CPUUnits: 1},
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		selected := pool.selectWaiterLocked()
		if selected < 0 {
			b.Fatal("legacy selector found no waiter")
		}
		mu024PriorityBenchmarkSink += selected
	}
}

func BenchmarkMU024AfterPrioritySelection(b *testing.B) {
	pool := &sqlClusterAdmissionPoolState{
		limits:             SQLClusterAdmissionPool{CPUUnits: 1, MaxRunning: 1},
		waiters:            make([]*sqlClusterAdmissionWaiter, 32),
		prioritizedWaiters: 32,
	}
	for index := range pool.waiters {
		pool.waiters[index] = &sqlClusterAdmissionWaiter{
			request: SQLClusterAdmissionRequest{
				CPUUnits:      1,
				WorkloadClass: SQLClusterWorkloadCompute,
				Priority:      index,
			},
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		selected := pool.selectWaiterLocked()
		if selected < 0 {
			b.Fatal("priority selector found no waiter")
		}
		mu024PriorityBenchmarkSink += selected
	}
}
