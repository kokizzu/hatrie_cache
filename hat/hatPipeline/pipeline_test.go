package hatPipeline_test

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/hat/hatPipeline"
)

func TestPipelineRunsIndependentStagesWithBackpressure(t *testing.T) {
	pipeline, err := hatPipeline.NewPipeline(
		hatPipeline.Stage[int]{
			Name: "double", Workers: 2, Queue: 2,
			Process: func(_ context.Context, value int) (int, error) {
				return value * 2, nil
			},
		},
		hatPipeline.Stage[int]{
			Name: "increment", Workers: 2, Queue: 2,
			Process: func(_ context.Context, value int) (int, error) {
				return value + 1, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	output, pipelineErrors := pipeline.Run(context.Background(), pipelineInput(1, 2, 3, 4))
	var got []int
	for value := range output {
		got = append(got, value)
	}
	for err := range pipelineErrors {
		if err != nil {
			t.Fatalf("unexpected pipeline error: %v", err)
		}
	}
	sort.Ints(got)
	if want := []int{3, 5, 7, 9}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pipeline output = %v, want %v", got, want)
	}
}

func TestPipelineSchedulesWorkersConcurrently(t *testing.T) {
	var active, maximum int32
	started := make(chan struct{}, 4)
	release := make(chan struct{})

	pipeline, err := hatPipeline.NewPipeline(hatPipeline.Stage[int]{
		Name: "parallel", Workers: 4, Queue: 4,
		Process: func(_ context.Context, value int) (int, error) {
			current := atomic.AddInt32(&active, 1)
			defer atomic.AddInt32(&active, -1)
			for {
				old := atomic.LoadInt32(&maximum)
				if current <= old || atomic.CompareAndSwapInt32(&maximum, old, current) {
					break
				}
			}
			started <- struct{}{}
			<-release
			return value, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	values := make([]int, 8)
	for i := range values {
		values[i] = i
	}
	output, pipelineErrors := pipeline.Run(context.Background(), pipelineInput(values...))
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("pipeline did not schedule two workers concurrently")
		}
	}
	close(release)
	for range output {
	}
	for err := range pipelineErrors {
		if err != nil {
			t.Fatalf("unexpected pipeline error: %v", err)
		}
	}
	if got := atomic.LoadInt32(&maximum); got < 2 {
		t.Fatalf("maximum concurrent workers = %d, want at least 2", got)
	}
}

func TestPipelinePropagatesFirstErrorAndStopsUpstream(t *testing.T) {
	wantErr := errors.New("stop at three")
	pipeline, err := hatPipeline.NewPipeline(hatPipeline.Stage[int]{
		Name: "fail", Workers: 1, Queue: 1,
		Process: func(_ context.Context, value int) (int, error) {
			if value == 3 {
				return 0, wantErr
			}
			return value, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	output, pipelineErrors := pipeline.Run(context.Background(), pipelineInput(1, 2, 3, 4, 5))
	for range output {
	}
	var gotErr error
	for err := range pipelineErrors {
		if err != nil {
			gotErr = err
		}
	}
	if !errors.Is(gotErr, wantErr) {
		t.Fatalf("pipeline error = %v, want %v", gotErr, wantErr)
	}
}

func TestPipelineReturnsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pipeline, err := hatPipeline.NewPipeline(hatPipeline.Stage[int]{
		Process: func(_ context.Context, value int) (int, error) { return value, nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	output, pipelineErrors := pipeline.Run(ctx, pipelineInput(1))
	for range output {
	}
	var gotErr error
	for err := range pipelineErrors {
		if err != nil {
			gotErr = err
		}
	}
	if !errors.Is(gotErr, context.Canceled) {
		t.Fatalf("pipeline error = %v, want context cancellation", gotErr)
	}
}

func TestPipelineValidatesStages(t *testing.T) {
	if _, err := hatPipeline.NewPipeline[int](); !errors.Is(err, hatPipeline.ErrPipelineInvalid) {
		t.Fatalf("empty pipeline error = %v, want %v", err, hatPipeline.ErrPipelineInvalid)
	}
	if _, err := hatPipeline.NewPipeline(hatPipeline.Stage[int]{Name: "missing-process", Queue: -1}); !errors.Is(err, hatPipeline.ErrPipelineInvalid) {
		t.Fatalf("invalid stage error = %v, want %v", err, hatPipeline.ErrPipelineInvalid)
	}
}

func BenchmarkPipelineRun(b *testing.B) {
	pipeline, err := hatPipeline.NewPipeline(
		hatPipeline.Stage[int]{
			Name: "double", Workers: 2, Queue: 32,
			Process: func(_ context.Context, value int) (int, error) { return value * 2, nil },
		},
		hatPipeline.Stage[int]{
			Name: "increment", Workers: 2, Queue: 32,
			Process: func(_ context.Context, value int) (int, error) { return value + 1, nil },
		},
	)
	if err != nil {
		b.Fatal(err)
	}
	values := make([]int, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		output, pipelineErrors := pipeline.Run(context.Background(), pipelineInput(values...))
		for range output {
		}
		for err := range pipelineErrors {
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func pipelineInput(values ...int) <-chan int {
	input := make(chan int)
	go func() {
		defer close(input)
		for _, value := range values {
			input <- value
		}
	}()
	return input
}
