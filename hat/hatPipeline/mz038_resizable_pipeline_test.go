package hatPipeline

import (
	"context"
	"errors"
	"sort"
	"testing"
)

func TestMZ038ResizablePipelineScalesStagesAndPreservesValues(t *testing.T) {
	pipeline, err := NewResizablePipeline(
		ResizablePipelineStage[int]{
			Name:    "increment",
			Workers: 1,
			Queue:   8,
			Process: func(_ context.Context, value int) (int, error) { return value + 1, nil },
		},
		ResizablePipelineStage[int]{
			Name:    "double",
			Workers: 1,
			Queue:   8,
			Process: func(_ context.Context, value int) (int, error) { return value * 2, nil },
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	input := make(chan int)
	output, runErrors, err := pipeline.Run(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if err := runErrors.ResizeStage(0, 4); err != nil {
		t.Fatal(err)
	}
	if err := runErrors.ResizeStage(1, 3); err != nil {
		t.Fatal(err)
	}
	go func() {
		for value := 0; value < 256; value++ {
			input <- value
		}
		close(input)
	}()

	values := make([]int, 0, 256)
	for value := range output {
		values = append(values, value)
	}
	sort.Ints(values)
	if len(values) != 256 {
		t.Fatalf("output count = %d, want 256", len(values))
	}
	for index, value := range values {
		if value != (index+1)*2 {
			t.Fatalf("output[%d] = %d, want %d", index, value, (index+1)*2)
		}
	}
	if err := <-runErrors.Errors(); err != nil {
		t.Fatalf("pipeline error = %v", err)
	}
}

func TestMZ038ResizablePipelineDownscaleDrainsValues(t *testing.T) {
	pipeline, err := NewResizablePipeline(ResizablePipelineStage[int]{
		Workers: 4,
		Queue:   16,
		Process: func(_ context.Context, value int) (int, error) { return value, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	input := make(chan int, 256)
	for value := 0; value < 256; value++ {
		input <- value
	}
	close(input)
	output, run, err := pipeline.Run(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if err := run.ResizeStage(0, 1); err != nil {
		t.Fatal(err)
	}
	count := 0
	for range output {
		count++
	}
	if err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	if count != 256 {
		t.Fatalf("output count = %d, want 256", count)
	}
}

func TestMZ038ResizablePipelinePropagatesStageFailure(t *testing.T) {
	want := errors.New("stop")
	pipeline, err := NewResizablePipeline(ResizablePipelineStage[int]{
		Name:    "fail",
		Workers: 2,
		Queue:   2,
		Process: func(_ context.Context, value int) (int, error) {
			if value == 3 {
				return 0, want
			}
			return value, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := make(chan int)
	output, run, err := pipeline.Run(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for value := 0; value < 32; value++ {
			input <- value
		}
		close(input)
	}()
	for range output {
	}
	err = <-run.Errors()
	if !errors.Is(err, want) {
		t.Fatalf("pipeline error = %v, want %v", err, want)
	}
	if err := run.ResizeStage(0, 2); !errors.Is(err, ErrResizablePipelineClosed) {
		t.Fatalf("resize after failure = %v, want closed", err)
	}
}

func TestMZ038ResizablePipelineValidation(t *testing.T) {
	if _, err := NewResizablePipeline[int](); !errors.Is(err, ErrResizablePipelineInvalid) {
		t.Fatalf("empty pipeline error = %v", err)
	}
	if _, err := NewResizablePipeline(ResizablePipelineStage[int]{Workers: -1, Process: func(context.Context, int) (int, error) { return 0, nil }}); !errors.Is(err, ErrResizablePipelineInvalid) {
		t.Fatalf("negative worker error = %v", err)
	}
	if _, err := NewResizablePipeline(ResizablePipelineStage[int]{Queue: -1, Process: func(context.Context, int) (int, error) { return 0, nil }}); !errors.Is(err, ErrResizablePipelineInvalid) {
		t.Fatalf("negative queue error = %v", err)
	}
	pipeline, err := NewResizablePipeline(ResizablePipelineStage[int]{Process: func(context.Context, int) (int, error) { return 0, nil }})
	if err != nil {
		t.Fatal(err)
	}
	input := make(chan int)
	_, run, err := pipeline.Run(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if err := run.ResizeStage(-1, 2); !errors.Is(err, ErrResizablePipelineStage) {
		t.Fatalf("negative stage error = %v", err)
	}
	if err := run.ResizeStage(0, 0); !errors.Is(err, ErrResizablePipelineWorkerCount) {
		t.Fatalf("zero resize error = %v", err)
	}
	run.Cancel()
	if err := run.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel wait error = %v", err)
	}
}
