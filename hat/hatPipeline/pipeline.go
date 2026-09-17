// Package hatPipeline provides bounded, independently scheduled processing stages.
package hatPipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrPipelineInvalid reports an invalid pipeline or stage configuration.
var ErrPipelineInvalid = errors.New("hatPipeline: invalid pipeline")

// Stage transforms one value. Workers are run independently and Queue bounds
// the number of completed values waiting for the next stage.
type Stage[T any] struct {
	Name    string
	Workers int
	Queue   int
	Process func(context.Context, T) (T, error)
}

// PipelineStageDescription describes one stage without exposing its process
// function.
type PipelineStageDescription struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Workers int    `json:"workers"`
	Queue   int    `json:"queue"`
}

// PipelineDescription is a deterministic view of the linear dataflow graph
// represented by a Pipeline.
type PipelineDescription struct {
	Stages []PipelineStageDescription `json:"stages"`
	Edges  []DataflowEdge             `json:"edges"`
}

// Pipeline is an immutable sequence of stages.
type Pipeline[T any] struct {
	stages []Stage[T]
}

// NewPipeline validates and copies a sequence of stages. A zero Workers value
// uses one worker; a zero Queue value uses an unbuffered handoff.
func NewPipeline[T any](stages ...Stage[T]) (*Pipeline[T], error) {
	if len(stages) == 0 {
		return nil, ErrPipelineInvalid
	}

	copied := make([]Stage[T], len(stages))
	copy(copied, stages)
	for i := range copied {
		stage := &copied[i]
		if stage.Process == nil || stage.Queue < 0 {
			return nil, fmt.Errorf("%w: stage %d", ErrPipelineInvalid, i+1)
		}
		if stage.Workers <= 0 {
			stage.Workers = 1
		}
		if stage.Name == "" {
			stage.Name = fmt.Sprintf("stage-%d", i+1)
		}
	}
	return &Pipeline[T]{stages: copied}, nil
}

// Describe returns a detached description of the pipeline. Stage IDs are
// stable within the pipeline and do not depend on user-provided names.
func (p *Pipeline[T]) Describe() PipelineDescription {
	if p == nil {
		return PipelineDescription{}
	}
	edgeCapacity := len(p.stages) - 1
	if edgeCapacity < 0 {
		edgeCapacity = 0
	}
	description := PipelineDescription{
		Stages: make([]PipelineStageDescription, 0, len(p.stages)),
		Edges:  make([]DataflowEdge, 0, edgeCapacity),
	}
	for index, stage := range p.stages {
		id := fmt.Sprintf("stage-%d", index+1)
		description.Stages = append(description.Stages, PipelineStageDescription{
			ID:      id,
			Name:    stage.Name,
			Workers: stage.Workers,
			Queue:   stage.Queue,
		})
		if index > 0 {
			description.Edges = append(description.Edges, DataflowEdge{
				From: fmt.Sprintf("stage-%d", index),
				To:   id,
			})
		}
	}
	return description
}

// Run starts the pipeline and returns its final output and an error channel.
// The output channel is closed after all workers stop. The error channel
// receives at most the first processing or context error and is then closed.
func (p *Pipeline[T]) Run(ctx context.Context, input <-chan T) (<-chan T, <-chan error) {
	if p == nil || ctx == nil || len(p.stages) == 0 {
		return failedRun[T](ErrPipelineInvalid)
	}

	runCtx, cancel := context.WithCancel(ctx)
	run := &pipelineRun[T]{ctx: runCtx, cancel: cancel, errors: make(chan error, 1)}
	current := input
	var lastDone <-chan struct{}
	for i := range p.stages {
		current, lastDone = run.startStage(p.stages[i], current)
	}

	go func() {
		select {
		case <-lastDone:
			if err := ctx.Err(); err != nil {
				run.report(err)
			}
			run.finish()
		case <-ctx.Done():
			run.report(ctx.Err())
			<-lastDone
			run.finish()
		}
	}()
	return current, run.errors
}

type pipelineRun[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc
	errors chan error

	mu       sync.Mutex
	finished bool
	reported bool
}

func (r *pipelineRun[T]) report(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	if r.finished || r.reported {
		r.mu.Unlock()
		return
	}
	r.reported = true
	r.errors <- err
	r.mu.Unlock()
	r.cancel()
}

func (r *pipelineRun[T]) finish() {
	r.mu.Lock()
	if !r.finished {
		r.finished = true
		close(r.errors)
	}
	r.mu.Unlock()
	r.cancel()
}

func (r *pipelineRun[T]) startStage(stage Stage[T], input <-chan T) (<-chan T, <-chan struct{}) {
	output := make(chan T, stage.Queue)
	done := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(stage.Workers)
	for i := 0; i < stage.Workers; i++ {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-r.ctx.Done():
					return
				case value, ok := <-input:
					if !ok {
						return
					}
					if err := r.ctx.Err(); err != nil {
						return
					}
					result, err := stage.Process(r.ctx, value)
					if err != nil {
						r.report(fmt.Errorf("hatPipeline: stage %q: %w", stage.Name, err))
						return
					}
					select {
					case <-r.ctx.Done():
						return
					case output <- result:
					}
				}
			}
		}()
	}
	go func() {
		workers.Wait()
		close(output)
		close(done)
	}()
	return output, done
}

func failedRun[T any](err error) (<-chan T, <-chan error) {
	output := make(chan T)
	close(output)
	errors := make(chan error, 1)
	errors <- err
	close(errors)
	return output, errors
}
