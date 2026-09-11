package hatCache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	DefaultCommandJournalSinkBatchSize = 64
	MaxCommandJournalSinkBatchSize     = 65536
	DefaultCommandJournalSinkBatchWait = 5 * time.Millisecond
)

var (
	ErrNilCommandJournalSink        = errors.New("hatriecache: command journal sink is nil")
	ErrCommandJournalSinkBatchLimit = errors.New("hatriecache: command journal sink batch limit is invalid")
)

// CommandJournalSink receives committed journal records in bounded batches.
// Write must not retain records after returning. Delivery is at least once
// when a checkpoint store is configured: a successful write followed by a
// failed checkpoint save can be delivered again after restart.
type CommandJournalSink interface {
	Write(context.Context, []CommandJournalRecord) error
}

// CommandJournalSinkFunc adapts a function to CommandJournalSink.
type CommandJournalSinkFunc func(context.Context, []CommandJournalRecord) error

// Write implements CommandJournalSink.
func (sink CommandJournalSinkFunc) Write(ctx context.Context, records []CommandJournalRecord) error {
	return sink(ctx, records)
}

// CommandJournalSinkCheckpointStore persists the last sequence acknowledged
// by a sink. Save is called only after the sink accepts the complete batch.
type CommandJournalSinkCheckpointStore interface {
	Load(context.Context) (uint64, error)
	Save(context.Context, uint64) error
}

// CommandJournalSinkOptions configures a bounded command-journal sink runner.
// The subscription fields have the same defaults and limits as
// CommandJournalSubscribeOptions.
type CommandJournalSinkOptions struct {
	AfterSequence   uint64
	ReplayLimit     int
	Buffer          int
	PollInterval    time.Duration
	BatchSize       int
	BatchWait       time.Duration
	CheckpointStore CommandJournalSinkCheckpointStore
}

// CommandJournalSinkRunner owns one journal subscription and one sink
// delivery loop. Use Wait to observe its terminal result or Close to stop it.
type CommandJournalSinkRunner struct {
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	cancel   context.CancelFunc
	errMu    sync.RWMutex
	err      error
}

// StartCommandJournalSink starts an opt-in at-least-once sink runner. When a
// checkpoint store is present, its loaded sequence supersedes AfterSequence.
// The checkpoint is written only after a successful sink batch.
func (journal *CommandJournal) StartCommandJournalSink(ctx context.Context, sink CommandJournalSink, options CommandJournalSinkOptions) (*CommandJournalSinkRunner, error) {
	if journal == nil {
		return nil, ErrNilCommandJournal
	}
	if sink == nil {
		return nil, ErrNilCommandJournalSink
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	subscriptionOptions, batchSize, batchWait, err := normalizeCommandJournalSinkOptions(options)
	if err != nil {
		return nil, err
	}
	if options.CheckpointStore != nil {
		sequence, err := options.CheckpointStore.Load(ctx)
		if err != nil {
			return nil, err
		}
		subscriptionOptions.AfterSequence = sequence
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	subscription, err := journal.Subscribe(runCtx, subscriptionOptions)
	if err != nil {
		cancel()
		return nil, err
	}
	runner := &CommandJournalSinkRunner{
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		cancel: cancel,
	}
	go runner.run(runCtx, subscription, sink, batchSize, batchWait, options.CheckpointStore)
	return runner, nil
}

func normalizeCommandJournalSinkOptions(options CommandJournalSinkOptions) (CommandJournalSubscribeOptions, int, time.Duration, error) {
	subscriptionOptions := CommandJournalSubscribeOptions{
		AfterSequence: options.AfterSequence,
		ReplayLimit:   options.ReplayLimit,
		Buffer:        options.Buffer,
		PollInterval:  options.PollInterval,
	}
	if _, _, _, err := normalizeCommandJournalSubscriptionOptions(subscriptionOptions); err != nil {
		return CommandJournalSubscribeOptions{}, 0, 0, err
	}
	batchSize := options.BatchSize
	if batchSize == 0 {
		batchSize = DefaultCommandJournalSinkBatchSize
	}
	if batchSize < 1 || batchSize > MaxCommandJournalSinkBatchSize {
		return CommandJournalSubscribeOptions{}, 0, 0, fmt.Errorf("%w: must be between 1 and %d", ErrCommandJournalSinkBatchLimit, MaxCommandJournalSinkBatchSize)
	}
	batchWait := options.BatchWait
	if batchWait == 0 {
		batchWait = DefaultCommandJournalSinkBatchWait
	}
	if batchWait < 0 {
		return CommandJournalSubscribeOptions{}, 0, 0, fmt.Errorf("%w: batch wait must be non-negative", ErrCommandJournalSinkBatchLimit)
	}
	return subscriptionOptions, batchSize, batchWait, nil
}

// Done returns a channel closed when the runner has stopped.
func (runner *CommandJournalSinkRunner) Done() <-chan struct{} {
	if runner == nil {
		return nil
	}
	return runner.done
}

// Err returns the terminal sink or subscription error, if any.
func (runner *CommandJournalSinkRunner) Err() error {
	if runner == nil {
		return nil
	}
	runner.errMu.RLock()
	defer runner.errMu.RUnlock()
	return runner.err
}

// Wait blocks until the sink runner stops and returns its terminal error.
func (runner *CommandJournalSinkRunner) Wait() error {
	if runner == nil {
		return nil
	}
	<-runner.done
	return runner.Err()
}

// Close stops the runner and waits for its delivery loop to finish. An
// explicit close returns nil unless the runner had already failed.
func (runner *CommandJournalSinkRunner) Close() error {
	if runner == nil {
		return nil
	}
	runner.stopOnce.Do(func() {
		close(runner.stop)
		runner.cancel()
	})
	return runner.Wait()
}

func (runner *CommandJournalSinkRunner) run(ctx context.Context, subscription *CommandJournalSubscription, sink CommandJournalSink, batchSize int, batchWait time.Duration, checkpointStore CommandJournalSinkCheckpointStore) {
	defer close(runner.done)
	defer subscription.Close()
	defer runner.cancel()

	for {
		batch, closed, err := runner.readBatch(ctx, subscription, batchSize, batchWait)
		if err != nil {
			if !runner.stopped() {
				runner.setError(err)
			}
			return
		}
		if len(batch) > 0 {
			if err := sink.Write(ctx, batch); err != nil {
				if !runner.stopped() {
					runner.setError(err)
				}
				return
			}
			if checkpointStore != nil {
				if err := checkpointStore.Save(ctx, batch[len(batch)-1].Sequence); err != nil {
					if !runner.stopped() {
						runner.setError(err)
					}
					return
				}
			}
		}
		if closed {
			if err := subscription.Err(); err != nil && !runner.stopped() {
				runner.setError(err)
			}
			return
		}
	}
}

func (runner *CommandJournalSinkRunner) readBatch(ctx context.Context, subscription *CommandJournalSubscription, batchSize int, batchWait time.Duration) ([]CommandJournalRecord, bool, error) {
	batch := make([]CommandJournalRecord, 0, batchSize)
	for {
		select {
		case record, ok := <-subscription.Records():
			if !ok {
				return batch, true, nil
			}
			batch = append(batch, record)
			goto collect
		case <-runner.stop:
			return nil, false, nil
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}

collect:
	if len(batch) >= batchSize {
		return batch, false, nil
	}
drain:
	for len(batch) < batchSize {
		select {
		case record, ok := <-subscription.Records():
			if !ok {
				return batch, true, nil
			}
			batch = append(batch, record)
		default:
			break drain
		}
	}
	if len(batch) >= batchSize {
		return batch, false, nil
	}
	timer := time.NewTimer(batchWait)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	for len(batch) < batchSize {
		select {
		case record, ok := <-subscription.Records():
			if !ok {
				return batch, true, nil
			}
			batch = append(batch, record)
		case <-timer.C:
			return batch, false, nil
		case <-runner.stop:
			return nil, false, nil
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
	return batch, false, nil
}

func (runner *CommandJournalSinkRunner) stopped() bool {
	select {
	case <-runner.stop:
		return true
	default:
		return false
	}
}

func (runner *CommandJournalSinkRunner) setError(err error) {
	if err == nil {
		return
	}
	runner.errMu.Lock()
	if runner.err == nil {
		runner.err = err
	}
	runner.errMu.Unlock()
}
