package hatPipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	// DefaultResizablePipelineMaxStages bounds the number of stages in one
	// resizable pipeline.
	DefaultResizablePipelineMaxStages = 256
	// DefaultResizablePipelineMaxWorkers bounds a stage's worker target.
	DefaultResizablePipelineMaxWorkers = 1024
	// DefaultResizablePipelineMaxQueue bounds a stage scheduler queue.
	DefaultResizablePipelineMaxQueue = 1 << 20
)

var (
	// ErrResizablePipelineInvalid reports an invalid pipeline or stage.
	ErrResizablePipelineInvalid = errors.New("hatPipeline: invalid resizable pipeline")
	// ErrResizablePipelineClosed reports an operation after a run finished.
	ErrResizablePipelineClosed = errors.New("hatPipeline: resizable pipeline run is closed")
	// ErrResizablePipelineStage reports an invalid stage index.
	ErrResizablePipelineStage = errors.New("hatPipeline: invalid resizable pipeline stage")
	// ErrResizablePipelineWorkerCount reports an invalid resize target.
	ErrResizablePipelineWorkerCount = errors.New("hatPipeline: invalid resizable pipeline worker count")
)

// ResizablePipelineStage describes one stage whose worker count may be
// changed while a pipeline run is active. Queue bounds pending inputs for the
// stage; zero is an unbuffered handoff.
type ResizablePipelineStage[T any] struct {
	Name    string
	Workers int
	Queue   int
	Process func(context.Context, T) (T, error)
}

// ResizablePipeline is an opt-in dataflow pipeline with runtime stage scaling.
// The existing Pipeline remains immutable and keeps its original behavior.
type ResizablePipeline[T any] struct {
	stages []ResizablePipelineStage[T]
}

// ResizablePipelineRun owns one active pipeline execution. Output and Errors
// are closed after all stages stop. ResizeStage is safe to call concurrently
// with processing.
type ResizablePipelineRun[T any] struct {
	parent  context.Context
	ctx     context.Context
	cancel  context.CancelFunc
	output  <-chan T
	errors  chan error
	stages  []*resizablePipelineStageRuntime[T]
	stageWG sync.WaitGroup

	mu       sync.RWMutex
	finished bool
	reported bool
	firstErr error
}

// NewResizablePipeline validates and copies a sequence of dynamically
// resizable stages. Zero Workers selects one worker; positive worker counts
// and queues remain bounded.
func NewResizablePipeline[T any](stages ...ResizablePipelineStage[T]) (*ResizablePipeline[T], error) {
	if len(stages) == 0 || len(stages) > DefaultResizablePipelineMaxStages {
		return nil, ErrResizablePipelineInvalid
	}
	copied := make([]ResizablePipelineStage[T], len(stages))
	copy(copied, stages)
	for index := range copied {
		stage := &copied[index]
		if stage.Workers == 0 {
			stage.Workers = 1
		}
		if stage.Process == nil || stage.Workers < 1 || stage.Workers > DefaultResizablePipelineMaxWorkers || stage.Queue < 0 || stage.Queue > DefaultResizablePipelineMaxQueue {
			return nil, fmt.Errorf("%w: stage %d", ErrResizablePipelineInvalid, index+1)
		}
		if stage.Name == "" {
			stage.Name = fmt.Sprintf("stage-%d", index+1)
		}
	}
	return &ResizablePipeline[T]{stages: copied}, nil
}

// Run starts all stages and returns the final output channel, the run handle,
// and any setup error. The input channel may remain open until the caller
// cancels the context. A nil context is treated as context.Background().
func (pipeline *ResizablePipeline[T]) Run(parent context.Context, input <-chan T) (<-chan T, *ResizablePipelineRun[T], error) {
	if pipeline == nil || len(pipeline.stages) == 0 {
		return nil, nil, ErrResizablePipelineInvalid
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	run := &ResizablePipelineRun[T]{
		parent: parent,
		ctx:    ctx,
		cancel: cancel,
		errors: make(chan error, 1),
		stages: make([]*resizablePipelineStageRuntime[T], len(pipeline.stages)),
	}

	current := input
	for index, stage := range pipeline.stages {
		output := make(chan T, stage.Queue)
		runtime := newResizablePipelineStageRuntime(ctx, stage, current, output, run.report)
		run.stages[index] = runtime
		runtime.Start()
		run.stageWG.Add(1)
		go run.startStage(runtime)
		current = output
	}
	run.output = current
	go func() {
		run.stageWG.Wait()
		run.finish()
	}()
	return current, run, nil
}

// Output returns the final stage output channel, or nil for a nil run.
func (run *ResizablePipelineRun[T]) Output() <-chan T {
	if run == nil {
		return nil
	}
	return run.output
}

// Errors returns the first stage, context, or processing error. It is closed
// after every stage has stopped.
func (run *ResizablePipelineRun[T]) Errors() <-chan error {
	if run == nil {
		return nil
	}
	return run.errors
}

// ResizeStage changes one stage's target worker count. Workers finish their
// current item before retiring; queued values remain in order only by stage
// membership, not by completion order.
func (run *ResizablePipelineRun[T]) ResizeStage(index, workers int) error {
	if run == nil {
		return ErrResizablePipelineClosed
	}
	if index < 0 || index >= len(run.stages) {
		return ErrResizablePipelineStage
	}
	if workers < 1 || workers > DefaultResizablePipelineMaxWorkers {
		return ErrResizablePipelineWorkerCount
	}
	run.mu.RLock()
	finished := run.finished
	runtime := run.stages[index]
	run.mu.RUnlock()
	if finished {
		return ErrResizablePipelineClosed
	}
	if err := runtime.Resize(workers); err != nil {
		if errors.Is(err, ErrResizablePipelineClosed) {
			return ErrResizablePipelineClosed
		}
		if errors.Is(err, ErrResizablePipelineWorkerCount) {
			return ErrResizablePipelineWorkerCount
		}
		return err
	}
	return nil
}

// Cancel stops the run and causes queued work to be discarded.
func (run *ResizablePipelineRun[T]) Cancel() {
	if run != nil && run.cancel != nil {
		run.cancel()
	}
}

// Wait waits for all stages to stop and returns the first reported error.
// Consumers must continue reading Output when graceful processing is expected;
// a blocked output consumer can keep a worker from completing.
func (run *ResizablePipelineRun[T]) Wait() error {
	if run == nil {
		return ErrResizablePipelineClosed
	}
	run.stageWG.Wait()
	run.finish()
	run.mu.RLock()
	err := run.firstErr
	run.mu.RUnlock()
	return err
}

func (run *ResizablePipelineRun[T]) startStage(runtime *resizablePipelineStageRuntime[T]) {
	defer run.stageWG.Done()
	runtime.Wait()
}

type resizablePipelineStageRuntime[T any] struct {
	ctx    context.Context
	stage  ResizablePipelineStage[T]
	input  <-chan T
	output chan<- T
	report func(error)
	wake   atomic.Pointer[chan struct{}]

	workers       sync.WaitGroup
	targetWorkers atomic.Int64
	activeWorkers atomic.Int64

	mu         sync.RWMutex
	started    bool
	finished   bool
	outputOnce sync.Once
}

func newResizablePipelineStageRuntime[T any](
	ctx context.Context,
	stage ResizablePipelineStage[T],
	input <-chan T,
	output chan<- T,
	report func(error),
) *resizablePipelineStageRuntime[T] {
	wake := make(chan struct{})
	runtime := &resizablePipelineStageRuntime[T]{
		ctx:    ctx,
		stage:  stage,
		input:  input,
		output: output,
		report: report,
	}
	runtime.targetWorkers.Store(int64(stage.Workers))
	runtime.wake.Store(&wake)
	return runtime
}

func (runtime *resizablePipelineStageRuntime[T]) Start() {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.started {
		return
	}
	runtime.started = true
	for runtime.activeWorkers.Load() < runtime.targetWorkers.Load() {
		runtime.startWorkerLocked()
	}
}

func (runtime *resizablePipelineStageRuntime[T]) Resize(workers int) error {
	if workers < 1 || workers > DefaultResizablePipelineMaxWorkers {
		return ErrResizablePipelineWorkerCount
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.finished || runtime.ctx.Err() != nil {
		return ErrResizablePipelineClosed
	}
	runtime.targetWorkers.Store(int64(workers))
	oldWake := runtime.wake.Load()
	newWake := make(chan struct{})
	runtime.wake.Store(&newWake)
	if oldWake != nil {
		close(*oldWake)
	}
	for runtime.activeWorkers.Load() < int64(workers) {
		runtime.startWorkerLocked()
	}
	return nil
}

func (runtime *resizablePipelineStageRuntime[T]) startWorkerLocked() {
	runtime.activeWorkers.Add(1)
	runtime.workers.Add(1)
	go runtime.runWorker()
}

func (runtime *resizablePipelineStageRuntime[T]) Wait() {
	runtime.workers.Wait()
	runtime.mu.Lock()
	runtime.finished = true
	runtime.mu.Unlock()
	runtime.outputOnce.Do(func() {
		close(runtime.output)
	})
}

func (runtime *resizablePipelineStageRuntime[T]) claimRetirement() bool {
	for {
		target := runtime.targetWorkers.Load()
		active := runtime.activeWorkers.Load()
		if active <= target {
			return false
		}
		if runtime.activeWorkers.CompareAndSwap(active, active-1) {
			return true
		}
	}
}

func (runtime *resizablePipelineStageRuntime[T]) runWorker() {
	retired := false
	defer func() {
		if !retired {
			runtime.activeWorkers.Add(-1)
		}
		runtime.workers.Done()
	}()

	wakePointer := runtime.wake.Load()
	var wake <-chan struct{}
	if wakePointer != nil {
		wake = *wakePointer
	}
	for {
		if runtime.claimRetirement() {
			retired = true
			return
		}
		select {
		case <-runtime.ctx.Done():
			return
		case <-wake:
			wakePointer = runtime.wake.Load()
			if wakePointer != nil {
				wake = *wakePointer
			}
		case value, ok := <-runtime.input:
			if !ok || runtime.ctx.Err() != nil {
				return
			}
			result, err := runtime.stage.Process(runtime.ctx, value)
			if err != nil {
				runtime.report(fmt.Errorf("hatPipeline: stage %q: %w", runtime.stage.Name, err))
				return
			}
			select {
			case runtime.output <- result:
			case <-runtime.ctx.Done():
				return
			}
		}
	}
}

func (run *ResizablePipelineRun[T]) report(err error) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	run.mu.Lock()
	defer run.mu.Unlock()
	if run.finished || run.reported {
		return
	}
	run.reported = true
	run.firstErr = err
	run.errors <- err
	run.cancel()
}

func (run *ResizablePipelineRun[T]) finish() {
	run.mu.Lock()
	if run.finished {
		run.mu.Unlock()
		return
	}
	if !run.reported {
		if err := run.parent.Err(); err != nil {
			run.reported = true
			run.firstErr = err
			run.errors <- err
		}
	}
	run.finished = true
	close(run.errors)
	run.mu.Unlock()
	run.cancel()
}
