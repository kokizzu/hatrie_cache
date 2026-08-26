package hatCache

import "testing"

var benchmarkCommandPrioritySink int64
var benchmarkCommandCountSink uint32

func BenchmarkCommandRequestFieldParsers(b *testing.B) {
	priority := int64(42)
	for _, benchmark := range []struct {
		name    string
		request CacheCommandRequest
		run     func(*testing.B, CacheCommandRequest)
	}{
		{name: "PriorityPointer", request: CacheCommandRequest{Priority: &priority}, run: benchmarkCommandPriorityParser},
		{name: "PrioritySubkey", request: CacheCommandRequest{Subkey: "42"}, run: benchmarkCommandPriorityParser},
		{name: "CountDefault", request: CacheCommandRequest{}, run: benchmarkCommandCountParser},
		{name: "CountSubkey", request: CacheCommandRequest{Subkey: "42"}, run: benchmarkCommandCountParser},
		{name: "CountPairs", request: CacheCommandRequest{Pairs: Map{"count": "42"}}, run: benchmarkCommandCountParser},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			benchmark.run(b, benchmark.request)
		})
	}
}

func benchmarkCommandPriorityParser(b *testing.B, request CacheCommandRequest) {
	b.ReportAllocs()
	for b.Loop() {
		benchmarkCommandPrioritySink, _ = commandPriority(request)
	}
}

func benchmarkCommandCountParser(b *testing.B, request CacheCommandRequest) {
	b.ReportAllocs()
	for b.Loop() {
		benchmarkCommandCountSink, _ = commandCountMinSketchIncrement(request)
	}
}

func BenchmarkCommandPriorityRequestExecution(b *testing.B) {
	priority := int64(42)
	for _, benchmark := range []struct {
		name string
		push CacheCommandRequest
		pop  CacheCommandRequest
	}{
		{
			name: "ExactPointer",
			push: CacheCommandRequest{Command: "PUSHPQ", Key: "priority:pointer", Value: "value", Priority: &priority},
			pop:  CacheCommandRequest{Command: "POPPQ", Key: "priority:pointer"},
		},
		{
			name: "GenericPointer",
			push: CacheCommandRequest{Command: "pushpq", Key: "priority:pointer", Value: "value", Priority: &priority},
			pop:  CacheCommandRequest{Command: "poppq", Key: "priority:pointer"},
		},
		{
			name: "ExactSubkey",
			push: CacheCommandRequest{Command: "PUSHPQ", Key: "priority:subkey", Value: "value", Subkey: "42"},
			pop:  CacheCommandRequest{Command: "POPPQ", Key: "priority:subkey"},
		},
		{
			name: "GenericSubkey",
			push: CacheCommandRequest{Command: "pushpq", Key: "priority:subkey", Value: "value", Subkey: "42"},
			pop:  CacheCommandRequest{Command: "poppq", Key: "priority:subkey"},
		},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			ht := CreateHatTrie()
			defer ht.Destroy()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				benchmarkExecuteCommand(b, ht, benchmark.push)
				benchmarkExecuteCommand(b, ht, benchmark.pop)
			}
		})
	}
}
