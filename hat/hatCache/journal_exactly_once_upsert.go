package hatCache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

const MaxCommandJournalExactlyOnceUpsertIdentityBytes = 256

var (
	ErrNilCommandJournalExactlyOnceUpsertSink           = errors.New("hatriecache: exactly-once upsert sink is nil")
	ErrNilCommandJournalExactlyOnceUpsertTransaction    = errors.New("hatriecache: exactly-once upsert transaction is nil")
	ErrCommandJournalExactlyOnceUpsertIdentityInvalid   = errors.New("hatriecache: exactly-once upsert output identity is invalid")
	ErrCommandJournalExactlyOnceUpsertIdentityDuplicate = errors.New("hatriecache: exactly-once upsert output identity is duplicated")
)

// CommandJournalExactlyOnceUpsertRecord binds a journal record to the stable
// destination identity that the sink must upsert. The sink must treat
// OutputIdentity as the durable idempotency key across retries.
type CommandJournalExactlyOnceUpsertRecord struct {
	OutputIdentity  string
	JournalSequence uint64
	Record          CommandJournalRecord
}

// CommandJournalExactlyOnceUpsertIdentityFunc derives a stable destination
// identity from one journal record. It must return the same identity whenever
// the same record is replayed after an uncertain commit.
type CommandJournalExactlyOnceUpsertIdentityFunc func(CommandJournalRecord) (string, error)

// CommandJournalExactlyOnceUpsertSink owns the output watermark and creates a
// transaction that atomically publishes upserts with that watermark. The
// implementation must make LoadSequence and Commit observe the same durable
// output state.
type CommandJournalExactlyOnceUpsertSink interface {
	LoadSequence(context.Context) (uint64, error)
	Begin(context.Context, uint64) (CommandJournalExactlyOnceUpsertTransaction, error)
}

// CommandJournalExactlyOnceUpsertTransaction stages one sink batch. Upsert
// must replace the value for OutputIdentity, not append a duplicate. Commit
// must make staged output and the sequence visible atomically. If Commit
// returns an error, its outcome is unknown and the runner stops without
// retrying it.
type CommandJournalExactlyOnceUpsertTransaction interface {
	Upsert(context.Context, CommandJournalExactlyOnceUpsertRecord) error
	Commit(context.Context, uint64) error
	Rollback(context.Context) error
}

// CommandJournalExactlyOnceUpsertSinkOptions configures a bounded upsert sink
// runner. The subscription and batching fields use the same defaults and
// limits as CommandJournalSinkOptions. A nil Identity uses a deterministic
// journal-sequence identity.
type CommandJournalExactlyOnceUpsertSinkOptions struct {
	AfterSequence uint64
	ReplayLimit   int
	Buffer        int
	PollInterval  time.Duration
	BatchSize     int
	BatchWait     time.Duration
	Identity      CommandJournalExactlyOnceUpsertIdentityFunc
}

// CommandJournalExactlyOnceUpsertSinkRunner owns one transactional upsert
// subscription. Use Wait to observe its terminal result or Close to stop it.
type CommandJournalExactlyOnceUpsertSinkRunner struct {
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	cancel   context.CancelFunc
	errMu    sync.RWMutex
	err      error
}

// StartCommandJournalExactlyOnceUpsertSink starts a transactional upsert sink
// whose output identities make replay after an uncertain commit idempotent.
// The sink owns the sequence checkpoint transactionally.
func (journal *CommandJournal) StartCommandJournalExactlyOnceUpsertSink(ctx context.Context, sink CommandJournalExactlyOnceUpsertSink, options CommandJournalExactlyOnceUpsertSinkOptions) (*CommandJournalExactlyOnceUpsertSinkRunner, error) {
	if journal == nil {
		return nil, ErrNilCommandJournal
	}
	if sink == nil {
		return nil, ErrNilCommandJournalExactlyOnceUpsertSink
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
	runner := &CommandJournalExactlyOnceUpsertSinkRunner{
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		cancel: cancel,
	}
	go runner.run(runCtx, subscription, sink, batchSize, batchWait, sequence, options.Identity)
	return runner, nil
}

// Done returns a channel closed when the runner has stopped.
func (runner *CommandJournalExactlyOnceUpsertSinkRunner) Done() <-chan struct{} {
	if runner == nil {
		return nil
	}
	return runner.done
}

// Err returns the terminal sink or subscription error, if any.
func (runner *CommandJournalExactlyOnceUpsertSinkRunner) Err() error {
	if runner == nil {
		return nil
	}
	runner.errMu.RLock()
	defer runner.errMu.RUnlock()
	return runner.err
}

// Wait blocks until the upsert runner stops and returns its terminal error.
func (runner *CommandJournalExactlyOnceUpsertSinkRunner) Wait() error {
	if runner == nil {
		return nil
	}
	<-runner.done
	return runner.Err()
}

// Close stops the upsert runner and waits for its transaction loop to finish.
func (runner *CommandJournalExactlyOnceUpsertSinkRunner) Close() error {
	if runner == nil {
		return nil
	}
	runner.stopOnce.Do(func() {
		close(runner.stop)
		runner.cancel()
	})
	return runner.Wait()
}

func (runner *CommandJournalExactlyOnceUpsertSinkRunner) run(ctx context.Context, subscription *CommandJournalSubscription, sink CommandJournalExactlyOnceUpsertSink, batchSize int, batchWait time.Duration, nextSequence uint64, identity CommandJournalExactlyOnceUpsertIdentityFunc) {
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
					runner.setError(ErrNilCommandJournalExactlyOnceUpsertTransaction)
				}
				return
			}
			upsertRecords, err := prepareCommandJournalExactlyOnceUpsertRecords(batch, identity)
			if err != nil {
				rollbackErr := transaction.Rollback(ctx)
				if !runner.stopped() {
					runner.setError(errors.Join(err, rollbackErr))
				}
				return
			}
			for _, record := range upsertRecords {
				if err := transaction.Upsert(ctx, record); err != nil {
					rollbackErr := transaction.Rollback(ctx)
					if !runner.stopped() {
						runner.setError(errors.Join(err, rollbackErr))
					}
					return
				}
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

func prepareCommandJournalExactlyOnceUpsertRecords(batch []CommandJournalRecord, identity CommandJournalExactlyOnceUpsertIdentityFunc) ([]CommandJournalExactlyOnceUpsertRecord, error) {
	if identity == nil {
		identity = defaultCommandJournalExactlyOnceUpsertIdentity
	}
	prepared := make([]CommandJournalExactlyOnceUpsertRecord, 0, len(batch))
	identities := make(map[string]struct{}, len(batch))
	for _, record := range batch {
		outputIdentity, err := identity(record)
		if err != nil {
			return nil, err
		}
		outputIdentity, err = normalizeCommandJournalExactlyOnceUpsertIdentity(outputIdentity)
		if err != nil {
			return nil, err
		}
		if _, exists := identities[outputIdentity]; exists {
			return nil, fmt.Errorf("%w: %q", ErrCommandJournalExactlyOnceUpsertIdentityDuplicate, outputIdentity)
		}
		identities[outputIdentity] = struct{}{}
		prepared = append(prepared, CommandJournalExactlyOnceUpsertRecord{
			OutputIdentity:  outputIdentity,
			JournalSequence: record.Sequence,
			Record:          record,
		})
	}
	return prepared, nil
}

func defaultCommandJournalExactlyOnceUpsertIdentity(record CommandJournalRecord) (string, error) {
	return "journal:" + strconv.FormatUint(record.Sequence, 10), nil
}

func normalizeCommandJournalExactlyOnceUpsertIdentity(identity string) (string, error) {
	identity = strings.TrimSpace(identity)
	if identity == "" || strings.IndexByte(identity, 0) >= 0 || len(identity) > MaxCommandJournalExactlyOnceUpsertIdentityBytes {
		return "", fmt.Errorf("%w: must be non-empty, NUL-free, and at most %d bytes", ErrCommandJournalExactlyOnceUpsertIdentityInvalid, MaxCommandJournalExactlyOnceUpsertIdentityBytes)
	}
	return identity, nil
}

func (runner *CommandJournalExactlyOnceUpsertSinkRunner) stopped() bool {
	select {
	case <-runner.stop:
		return true
	default:
		return false
	}
}

func (runner *CommandJournalExactlyOnceUpsertSinkRunner) setError(err error) {
	if err == nil {
		return
	}
	runner.errMu.Lock()
	if runner.err == nil {
		runner.err = err
	}
	runner.errMu.Unlock()
}
