package hatFiber

import (
	"context"
	"runtime"
	"sync"
	"testing"
)

func BenchmarkT235GoroutineWorkerBaseline(b *testing.B) {
	const (
		taskCount = 128
		stepCount = 8
	)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		start := make(chan struct{})
		var waitGroup sync.WaitGroup
		waitGroup.Add(taskCount)
		for task := 0; task < taskCount; task++ {
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

func BenchmarkT235StacklessWorkerBaseline(b *testing.B) {
	const (
		taskCount = 128
		stepCount = 8
	)
	scheduler, err := New(Options{MaxFibers: taskCount})
	if err != nil {
		b.Fatal(err)
	}
	states := make([]int, taskCount)
	functions := make([]StepFunc, taskCount)
	for task := range functions {
		taskIndex := task
		functions[task] = func(context.Context) (Step, error) {
			states[taskIndex]++
			if states[taskIndex] == stepCount {
				return StepDone, nil
			}
			return StepYield, nil
		}
	}
	identifiers := make([]FiberID, taskCount)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for task := range states {
			states[task] = 0
			identifier, err := scheduler.Spawn(functions[task])
			if err != nil {
				b.Fatal(err)
			}
			identifiers[task] = identifier
		}
		stats, err := scheduler.Run(context.Background(), 0)
		if err != nil || stats.Completed != taskCount {
			b.Fatalf("Run() stats = %#v, error = %v", stats, err)
		}
		for _, identifier := range identifiers {
			if err := scheduler.Reap(identifier); err != nil {
				b.Fatal(err)
			}
		}
	}
}
