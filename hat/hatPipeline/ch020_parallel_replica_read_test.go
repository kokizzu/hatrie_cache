package hatPipeline

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCH020ParallelReplicaReadsPreserveOrderAndBoundConcurrency(t *testing.T) {
	const taskCount = 12
	var active atomic.Int32
	var maximum atomic.Int32
	tasks := make([]ParallelReplicaReadTask[string], taskCount)
	for index := range tasks {
		index := index
		tasks[index] = ParallelReplicaReadTask[string]{
			Partition: fmt.Sprintf("partition-%02d", index),
			Replica:   fmt.Sprintf("replica-%02d", index%3),
			Read: func(context.Context) ([]string, error) {
				current := active.Add(1)
				for {
					previous := maximum.Load()
					if current <= previous || maximum.CompareAndSwap(previous, current) {
						break
					}
				}
				time.Sleep(2 * time.Millisecond)
				active.Add(-1)
				return []string{fmt.Sprintf("row-%02d", index)}, nil
			},
		}
	}

	results, err := RunParallelReplicaReads(context.Background(), tasks, ParallelReplicaReadOptions{MaxConcurrency: 3})
	if err != nil {
		t.Fatalf("RunParallelReplicaReads() error = %v", err)
	}
	if got, want := maximum.Load(), int32(3); got != want {
		t.Fatalf("maximum concurrency = %d, want %d", got, want)
	}
	for index, result := range results {
		if result.Partition != tasks[index].Partition || result.Replica != tasks[index].Replica {
			t.Fatalf("result[%d] metadata = %#v, want partition/replica from input", index, result)
		}
		if !reflect.DeepEqual(result.Rows, []string{fmt.Sprintf("row-%02d", index)}) {
			t.Fatalf("result[%d].Rows = %#v", index, result.Rows)
		}
	}
}

func TestCH020ParallelReplicaReadsCancelsOtherTasksOnError(t *testing.T) {
	wantErr := errors.New("replica unavailable")
	bothStarted := make(chan struct{})
	canceled := make(chan struct{})
	var started atomic.Int32
	var startOnce sync.Once
	markStarted := func() {
		if started.Add(1) == 2 {
			startOnce.Do(func() { close(bothStarted) })
		}
	}
	tasks := []ParallelReplicaReadTask[string]{
		{
			Partition: "p0",
			Replica:   "r0",
			Read: func(context.Context) ([]string, error) {
				markStarted()
				<-bothStarted
				return nil, wantErr
			},
		},
		{
			Partition: "p1",
			Replica:   "r1",
			Read: func(ctx context.Context) ([]string, error) {
				markStarted()
				<-ctx.Done()
				close(canceled)
				return nil, ctx.Err()
			},
		},
	}

	result, err := RunParallelReplicaReads(context.Background(), tasks, ParallelReplicaReadOptions{MaxConcurrency: 2})
	if result != nil {
		t.Fatalf("result = %#v, want nil on error", result)
	}
	if !errors.Is(err, wantErr) || !strings.Contains(err.Error(), `partition "p0"`) {
		t.Fatalf("error = %v, want wrapped partition error", err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("second task did not observe cancellation")
	}
}

func TestCH020ParallelReplicaReadsRejectsInvalidAssignments(t *testing.T) {
	read := func(context.Context) ([]int, error) { return nil, nil }
	tests := []struct {
		name  string
		tasks []ParallelReplicaReadTask[int]
	}{
		{name: "empty partition", tasks: []ParallelReplicaReadTask[int]{{Replica: "r", Read: read}}},
		{name: "empty replica", tasks: []ParallelReplicaReadTask[int]{{Partition: "p", Read: read}}},
		{name: "missing reader", tasks: []ParallelReplicaReadTask[int]{{Partition: "p", Replica: "r"}}},
		{name: "duplicate partition", tasks: []ParallelReplicaReadTask[int]{{Partition: "p", Replica: "r1", Read: read}, {Partition: "p", Replica: "r2", Read: read}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := RunParallelReplicaReads(context.Background(), test.tasks, ParallelReplicaReadOptions{}); err == nil {
				t.Fatal("RunParallelReplicaReads() error = nil")
			}
		})
	}
	if _, err := RunParallelReplicaReads[int](context.Background(), nil, ParallelReplicaReadOptions{MaxConcurrency: -1}); err == nil {
		t.Fatal("negative MaxConcurrency error = nil")
	}
}

func TestCH020ParallelReplicaReadsHonorsCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	tasks := []ParallelReplicaReadTask[int]{
		{Partition: "p", Replica: "r", Read: func(ctx context.Context) ([]int, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}},
	}
	done := make(chan struct{})
	var err error
	go func() {
		_, err = RunParallelReplicaReads(ctx, tasks, ParallelReplicaReadOptions{})
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("task did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not return after cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestCH020ParallelReplicaReadsDoesNotRaceOnResultSlots(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	tasks := []ParallelReplicaReadTask[string]{
		{Partition: "p0", Replica: "r0", Read: func(context.Context) ([]string, error) {
			mu.Lock()
			calls = append(calls, "p0")
			mu.Unlock()
			return []string{"0"}, nil
		}},
		{Partition: "p1", Replica: "r1", Read: func(context.Context) ([]string, error) {
			mu.Lock()
			calls = append(calls, "p1")
			mu.Unlock()
			return []string{"1"}, nil
		}},
	}
	if _, err := RunParallelReplicaReads(context.Background(), tasks, ParallelReplicaReadOptions{MaxConcurrency: 2}); err != nil {
		t.Fatalf("RunParallelReplicaReads() error = %v", err)
	}
	if len(calls) != len(tasks) {
		t.Fatalf("reader calls = %#v, want %d calls", calls, len(tasks))
	}
}

func BenchmarkCH020SequentialReplicaReads(b *testing.B) {
	tasks := benchmarkCH020ReadTasks(32, 100*time.Microsecond)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := runCH020SequentialReplicaReads(tasks); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH020ParallelReplicaReads(b *testing.B) {
	tasks := benchmarkCH020ReadTasks(32, 100*time.Microsecond)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := RunParallelReplicaReads(context.Background(), tasks, ParallelReplicaReadOptions{MaxConcurrency: 8}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH020SequentialNoop(b *testing.B) {
	tasks := benchmarkCH020ReadTasks(32, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := runCH020SequentialReplicaReads(tasks); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH020ParallelNoop(b *testing.B) {
	tasks := benchmarkCH020ReadTasks(32, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := RunParallelReplicaReads(context.Background(), tasks, ParallelReplicaReadOptions{MaxConcurrency: 8}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkCH020ReadTasks(count int, delay time.Duration) []ParallelReplicaReadTask[int] {
	tasks := make([]ParallelReplicaReadTask[int], count)
	for index := range tasks {
		index := index
		tasks[index] = ParallelReplicaReadTask[int]{
			Partition: fmt.Sprintf("partition-%d", index),
			Replica:   fmt.Sprintf("replica-%d", index%8),
			Read: func(context.Context) ([]int, error) {
				if delay > 0 {
					time.Sleep(delay)
				}
				return []int{index}, nil
			},
		}
	}
	return tasks
}

func runCH020SequentialReplicaReads(tasks []ParallelReplicaReadTask[int]) error {
	results := make([]ParallelReplicaReadResult[int], len(tasks))
	for index, task := range tasks {
		rows, err := task.Read(context.Background())
		if err != nil {
			return err
		}
		results[index] = ParallelReplicaReadResult[int]{Partition: task.Partition, Replica: task.Replica, Rows: rows}
	}
	return nil
}
