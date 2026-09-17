package hatSql

import "testing"

type mu024FIFOBenchmarkWaiter struct {
	priority int
	ready    bool
}

func BenchmarkMU024BeforeFIFOSelection(b *testing.B) {
	waiters := make([]mu024FIFOBenchmarkWaiter, 32)
	for index := range waiters {
		waiters[index] = mu024FIFOBenchmarkWaiter{priority: index, ready: true}
	}
	b.ReportAllocs()
	b.ResetTimer()
	var sink int
	for index := 0; index < b.N; index++ {
		selected := -1
		for waiterIndex, waiter := range waiters {
			if waiter.ready {
				selected = waiterIndex
				break
			}
		}
		if selected < 0 {
			b.Fatal("FIFO selector found no waiter")
		}
		sink += selected
	}
	if sink == -1 {
		b.Fatal("unreachable benchmark sink")
	}
}
