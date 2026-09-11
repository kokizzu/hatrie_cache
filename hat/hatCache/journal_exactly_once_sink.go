package hatCache

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrNilCommandJournalExactlyOnceSink        = errors.New("hatriecache: exactly-once command journal sink is nil")
	ErrNilCommandJournalExactlyOnceTransaction = errors.New("hatriecache: exactly-once command journal transaction is nil")
)

// CommandJournalExactlyOnceSink owns the output watermark and creates a
// transaction that atomically publishes records with that watermark. The
// implementation must make LoadSequence and Commit observe the same durable
// output state.
type CommandJournalExactlyOnceSink interface {
	LoadSequence(context.Context) (uint64, error)
	Begin(context.Context, uint64) (CommandJournalExactlyOnceTransaction, error)
}

// CommandJournalExactlyOnceTransaction stages one sink batch. Commit must make
// the staged output and sequence visible atomically. If Commit returns an
// error, its outcome is unknown and the runner stops without retrying it.
type CommandJournalExactlyOnceTransaction interface {
	Write(context.Context, []CommandJournalRecord) error
	Commit(context.Context, uint64) error
	Rollback(context.Context) error
}

// CommandJournalExactlyOnceSinkOptions configures the same bounded replay and
// batch controls as CommandJournalSinkOptions without a separate checkpoint
// store. The sink owns the checkpoint transactionally.
type CommandJournalExactlyOnceSinkOptions struct {
	AfterSequence uint64
	ReplayLimit   int
	Buffer        int
	PollInterval  time.Duration
	BatchSize     int
	BatchWait     time.Duration
}

// CommandJournalExactlyOnceSinkRunner owns one transactional sink
// subscription. Use Wait to observe its terminal result or Close to stop it.
type CommandJournalExactlyOnceSinkRunner struct {
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	cancel   context.CancelFunc
	errMu    sync.RWMutex
	err      error
}

// StartCommandJournalExactlyOnceSink starts a sink whose output and sequence
// watermark are committed by one sink-owned transaction per batch.
func (journal *CommandJournal) StartCommandJournalExactlyOnceSink(ctx context.Context, sink CommandJournalExactlyOnceSink, options CommandJournalExactlyOnceSinkOptions) (*CommandJournalExactlyOnceSinkRunner, error) {
	if journal == nil {
		return nil, ErrNilCommandJournal
	}
	if sink == nil {
		return nil, ErrNilCommandJournalExactlyOnceSink
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	subscriptionOptions, batchSize, batchWait, err := normalizeCommandJournalSinkOptions(CommandJournalSinkOptions{
		AfterSequence: options.AfterSequence,
		ReplayLimit:   options.ReplayLimit,
		Buffer:        options.Buffer,
		PollInterval:  options.PollInterval,
		BatchSize:     options.BatchSize,
		BatchWait:     options.BatchWait,
	})
	if err != nil {
		return nil, err
	}
	sequence, err := sink.LoadSequence(ctx)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	subscriptionOptions.AfterSequence = sequence
	runCtx, cancel := context.WithCancel(ctx)
	subscription, err := journal.Subscribe(runCtx, subscriptionOptions)
	if err != nil {
		cancel()
		return nil, err
	}
	runner := &CommandJournalExactlyOnceSinkRunner{
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		cancel: cancel,
	}
	go runner.run(runCtx, subscription, sink, batchSize, batchWait, sequence)
	return runner, nil
}

// Done returns a channel closed when the runner has stopped.
func (runner *CommandJournalExactlyOnceSinkRunner) Done() <-chan struct{} {
	if runner == nil {
		return nil
	}
	return runner.done
}

// Err returns the terminal sink or subscription error, if any.
func (runner *CommandJournalExactlyOnceSinkRunner) Err() error {
	if runner == nil {
		return nil
	}
	runner.errMu.RLock()
	defer runner.errMu.RUnlock()
	return runner.err
}

// Wait blocks until the runner stops and returns its terminal error.
func (runner *CommandJournalExactlyOnceSinkRunner) Wait() error {
	if runner == nil {
		return nil
	}
	<-runner.done
	return runner.Err()
}

// Close stops the runner and waits for its transaction loop to finish.
func (runner *CommandJournalExactlyOnceSinkRunner) Close() error {
	if runner == nil {
		return nil
	}
	runner.stopOnce.Do(func() {
		close(runner.stop)
		runner.cancel()
	})
	return runner.Wait()
}

func (runner *CommandJournalExactlyOnceSinkRunner) run(ctx context.Context, subscription *CommandJournalSubscription, sink CommandJournalExactlyOnceSink, batchSize int, batchWait time.Duration, nextSequence uint64) {
	defer close(runner.done)
	defer subscription.Close()
	defer runner.cancel()

	for {
		batch, closed, err := readCommandJournalSinkBatch(ctx, runner.stop, subscription, batchSize, batchWait)
		if err != nil {
			if !runner.stopped() {
				runner.setError(err)
			}
			return
		}
		if len(batch) > 0 {
			transaction, err := sink.Begin(ctx, nextSequence)
			if err != nil {
				if !runner.stopped() {
					runner.setError(err)
				}
				return
			}
			if transaction == nil {
				if !runner.stopped() {
					runner.setError(ErrNilCommandJournalExactlyOnceTransaction)
				}
				return
			}
			if err := transaction.Write(ctx, batch); err != nil {
				rollbackErr := transaction.Rollback(ctx)
				if !runner.stopped() {
					runner.setError(errors.Join(err, rollbackErr))
				}
				return
			}
			lastSequence := batch[len(batch)-1].Sequence
			if err := transaction.Commit(ctx, lastSequence); err != nil {
				if !runner.stopped() {
					runner.setError(err)
				}
				return
			}
			nextSequence = lastSequence
		}
		if closed {
			if err := subscription.Err(); err != nil && !runner.stopped() {
				runner.setError(err)
			}
			return
		}
	}
}

func (runner *CommandJournalExactlyOnceSinkRunner) stopped() bool {
	select {
	case <-runner.stop:
		return true
	default:
		return false
	}
}

func (runner *CommandJournalExactlyOnceSinkRunner) setError(err error) {
	if err == nil {
		return
	}
	runner.errMu.Lock()
	if runner.err == nil {
		runner.err = err
	}
	runner.errMu.Unlock()
}
