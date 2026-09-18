package hatPipeline

import (
	"context"
	"testing"
)

var mz038ResizablePipelineBenchmarkSink int

func BenchmarkMZ038FixedPipeline(b *testing.B) {
	pipeline, err := NewPipeline(
		Stage[int]{Name: "increment", Workers: 4, Queue: 32, Process: mz038Increment},
		Stage[int]{Name: "double", Workers: 4, Queue: 32, Process: mz038Double},
	)
	if err != nil {
		b.Fatal(err)
	}
	mz038BenchmarkPipelineRuns(b, func(input <-chan int) (<-chan int, <-chan error) {
		return pipeline.Run(context.Background(), input)
	})
}

func BenchmarkMZ038ResizablePipelineFixedWorkers(b *testing.B) {
	pipeline, err := NewResizablePipeline(
		ResizablePipelineStage[int]{Name: "increment", Workers: 4, Queue: 32, Process: mz038Increment},
		ResizablePipelineStage[int]{Name: "double", Workers: 4, Queue: 32, Process: mz038Double},
	)
	if err != nil {
		b.Fatal(err)
	}
	mz038BenchmarkResizableRuns(b, pipeline, false)
}

func BenchmarkMZ038ResizablePipelineScaleUp(b *testing.B) {
	pipeline, err := NewResizablePipeline(
		ResizablePipelineStage[int]{Name: "increment", Workers: 1, Queue: 32, Process: mz038Increment},
		ResizablePipelineStage[int]{Name: "double", Workers: 1, Queue: 32, Process: mz038Double},
	)
	if err != nil {
		b.Fatal(err)
	}
	mz038BenchmarkResizableRuns(b, pipeline, true)
}

func mz038BenchmarkPipelineRuns(b *testing.B, run func(<-chan int) (<-chan int, <-chan error)) {
	const values = 256
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		input := make(chan int, values)
		for value := range values {
			input <- value
		}
		close(input)
		output, errors := run(input)
		count := 0
		for range output {
			count++
		}
		for err := range errors {
			if err != nil {
				b.Fatal(err)
			}
		}
		mz038ResizablePipelineBenchmarkSink = count
	}
}

func mz038BenchmarkResizableRuns(b *testing.B, pipeline *ResizablePipeline[int], scale bool) {
	const values = 256
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		input := make(chan int, values)
		for value := range values {
			input <- value
		}
		close(input)
		output, run, err := pipeline.Run(context.Background(), input)
		if err != nil {
			b.Fatal(err)
		}
		if scale {
			if err := run.ResizeStage(0, 4); err != nil {
				b.Fatal(err)
			}
			if err := run.ResizeStage(1, 4); err != nil {
				b.Fatal(err)
			}
		}
		count := 0
		for range output {
			count++
		}
		if err := run.Wait(); err != nil {
			b.Fatal(err)
		}
		for err := range run.Errors() {
			if err != nil {
				b.Fatal(err)
			}
		}
		mz038ResizablePipelineBenchmarkSink = count
	}
}

func mz038Increment(_ context.Context, value int) (int, error) {
	return value + 1, nil
}

func mz038Double(_ context.Context, value int) (int, error) {
	return value * 2, nil
}
