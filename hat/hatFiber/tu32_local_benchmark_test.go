package hatFiber

import (
	"context"
	"runtime"
	"testing"
)

type localAccessBenchmarkState struct {
	local     *Local[int]
	remaining int
	sum       int
}

func (state *localAccessBenchmarkState) step(context.Context) (Step, error) {
	value, _, err := state.local.Get()
	if err != nil {
		return 0, err
	}
	value++
	if err := state.local.Set(value); err != nil {
		return 0, err
	}
	state.sum += value
	state.remaining--
	if state.remaining == 0 {
		return StepDone, nil
	}
	return StepYield, nil
}

type mapAccessBenchmarkState struct {
	scheduler *Scheduler
	values    map[FiberID]int
	remaining int
	sum       int
}

func (state *mapAccessBenchmarkState) step(context.Context) (Step, error) {
	identifier := state.scheduler.Current()
	value := state.values[identifier] + 1
	state.values[identifier] = value
	state.sum += value
	state.remaining--
	if state.remaining == 0 {
		return StepDone, nil
	}
	return StepYield, nil
}

func BenchmarkTU32FiberLocalSetGet(b *testing.B) {
	scheduler, err := New(Options{MaxFibers: 1})
	if err != nil {
		b.Fatal(err)
	}
	local, err := NewLocal[int](scheduler)
	if err != nil {
		b.Fatal(err)
	}
	state := &localAccessBenchmarkState{local: local, remaining: b.N}
	identifier, err := scheduler.Spawn(StepFunc(state.step))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	if _, err := scheduler.Run(context.Background(), 0); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	if state.sum == 0 {
		b.Fatal("local benchmark did not execute")
	}
	if err := scheduler.Reap(identifier); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkTU32FiberMapSetGet(b *testing.B) {
	scheduler, err := New(Options{MaxFibers: 1})
	if err != nil {
		b.Fatal(err)
	}
	state := &mapAccessBenchmarkState{
		scheduler: scheduler,
		values:    make(map[FiberID]int, 1),
		remaining: b.N,
	}
	identifier, err := scheduler.Spawn(StepFunc(state.step))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	if _, err := scheduler.Run(context.Background(), 0); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	if state.sum == 0 {
		b.Fatal("map benchmark did not execute")
	}
	if err := scheduler.Reap(identifier); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkTU32FiberLocalSetup(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		scheduler, err := New(Options{MaxFibers: 64})
		if err != nil {
			b.Fatal(err)
		}
		local, err := NewLocal[int](scheduler)
		if err != nil {
			b.Fatal(err)
		}
		runtime.KeepAlive(local)
	}
}

func BenchmarkTU32FiberMapSetup(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		scheduler, err := New(Options{MaxFibers: 64})
		if err != nil {
			b.Fatal(err)
		}
		values := make(map[FiberID]int, 1)
		runtime.KeepAlive(scheduler)
		runtime.KeepAlive(values)
	}
}
