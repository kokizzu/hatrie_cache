package hatJournal

import (
	"context"
	"sync/atomic"
	"testing"
)

var parallelReplayBenchmarkSink uint64

func BenchmarkParallelReplaySerialBaseline(b *testing.B) {
	benchmarkParallelReplay(b, 64, 64, func(tasks []ReplayTask) error {
		for _, task := range tasks {
			if err := task.Apply(context.Background()); err != nil {
				return err
			}
		}
		return nil
	})
}

func BenchmarkParallelReplayWorkers4(b *testing.B) {
	benchmarkParallelReplay(b, 64, 64, func(tasks []ReplayTask) error {
		return ParallelReplay(context.Background(), tasks, ReplayOptions{Workers: 4})
	})
}

func BenchmarkParallelReplaySerialDefault(b *testing.B) {
	benchmarkParallelReplay(b, 1, 4096, func(tasks []ReplayTask) error {
		return ParallelReplay(context.Background(), tasks, ReplayOptions{})
	})
}

func BenchmarkParallelReplayWorkers4SinglePartition(b *testing.B) {
	benchmarkParallelReplay(b, 1, 4096, func(tasks []ReplayTask) error {
		return ParallelReplay(context.Background(), tasks, ReplayOptions{Workers: 4})
	})
}

func BenchmarkParallelReplayTinySerialBaseline(b *testing.B) {
	benchmarkParallelReplayWork(b, 64, 64, 1, func(tasks []ReplayTask) error {
		for _, task := range tasks {
			if err := task.Apply(context.Background()); err != nil {
				return err
			}
		}
		return nil
	})
}

func BenchmarkParallelReplayTinyWorkers4(b *testing.B) {
	benchmarkParallelReplayWork(b, 64, 64, 1, func(tasks []ReplayTask) error {
		return ParallelReplay(context.Background(), tasks, ReplayOptions{Workers: 4})
	})
}

func benchmarkParallelReplay(b *testing.B, partitions, tasksPerPartition int, run func([]ReplayTask) error) {
	benchmarkParallelReplayWork(b, partitions, tasksPerPartition, 2048, run)
}

func benchmarkParallelReplayWork(b *testing.B, partitions, tasksPerPartition, work int, run func([]ReplayTask) error) {
	tasks := makeParallelReplayBenchmarkTasks(partitions, tasksPerPartition, work)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := run(tasks); err != nil {
			b.Fatal(err)
		}
	}
}

func makeParallelReplayBenchmarkTasks(partitions, tasksPerPartition, work int) []ReplayTask {
	tasks := make([]ReplayTask, 0, partitions*tasksPerPartition)
	sequence := uint64(1)
	for taskIndex := 0; taskIndex < tasksPerPartition; taskIndex++ {
		for partition := 0; partition < partitions; partition++ {
			currentSequence := sequence
			currentPartition := uint64(partition)
			tasks = append(tasks, ReplayTask{
				Sequence:  currentSequence,
				Partition: currentPartition,
				Apply: func(context.Context) error {
					value := currentSequence
					for iteration := 0; iteration < work; iteration++ {
						value = value*1664525 + uint64(iteration) + 1013904223
					}
					atomic.AddUint64(&parallelReplayBenchmarkSink, value)
					return nil
				},
			})
			sequence++
		}
	}
	return tasks
}
