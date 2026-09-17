package hatSql

import "testing"

var mu023DirectExecutionSink uint64

//go:noinline
func mu023DirectExecution() {
	mu023DirectExecutionSink++
}

func BenchmarkMU023BeforeDirectExecution(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		mu023DirectExecution()
	}
}
