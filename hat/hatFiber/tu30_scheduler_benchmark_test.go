package hatFiber

import (
	"context"
	"runtime"
	"sync"
	"testing"
)

func BenchmarkTU30StacklessScheduler(b *testing.B) {
	const (
		fiberCount = 256
		stepCount  = 8
	)

	scheduler, err := New(Options{MaxFibers: fiberCount})
	if err != nil {
		b.Fatal(err)
	}
	states := make([]int, fiberCount)
	functions := make([]StepFunc, fiberCount)
	for index := range functions {
		fiberIndex := index
		functions[index] = func(context.Context) (Step, error) {
			states[fiberIndex]++
			if states[fiberIndex] == stepCount {
				return StepDone, nil
			}
			return StepYield, nil
		}
	}
	identifiers := make([]FiberID, fiberCount)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for index := range states {
			states[index] = 0
			identifier, err := scheduler.Spawn(functions[index])
			if err != nil {
				b.Fatal(err)
			}
			identifiers[index] = identifier
		}
		stats, err := scheduler.Run(ctx, 0)
		if err != nil || stats.Completed != fiberCount {
			b.Fatalf("Run() stats = %#v, error = %v", stats, err)
		}
		for _, identifier := range identifiers {
			if err := scheduler.Reap(identifier); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkTU30GoroutineYieldControl(b *testing.B) {
	const (
		fiberCount = 256
		stepCount  = 8
	)

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		start := make(chan struct{})
		var waitGroup sync.WaitGroup
		waitGroup.Add(fiberCount)
		for index := 0; index < fiberCount; index++ {
			go func() {
				defer waitGroup.Done()
				<-start
				for step := 0; step < stepCount; step++ {
					runtime.Gosched()
				}
			}()
		}
		close(start)
		waitGroup.Wait()
	}
}
