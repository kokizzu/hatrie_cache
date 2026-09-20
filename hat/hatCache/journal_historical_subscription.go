package hatCache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrCommandJournalHistoricalSubscriptionClosed              = errors.New("hatriecache: historical journal subscription is closed")
	ErrCommandJournalHistoricalSubscriptionCheckpointConflict  = errors.New("hatriecache: historical journal subscription checkpoint conflicts with the requested start")
	ErrCommandJournalHistoricalSubscriptionCheckpointRegressed = errors.New("hatriecache: historical journal subscription checkpoint regressed")
)

// CommandJournalHistoricalSubscriptionOptions controls a replayable journal
// subscription with an externally durable acknowledgement point. Store is
// loaded before the subscription starts; AfterSequence is used only when the
// store has no non-zero checkpoint yet.
type CommandJournalHistoricalSubscriptionOptions struct {
	SourceID      string
	Store         CommandJournalSourceCheckpointStore
	AfterSequence uint64
	UpToSequence  uint64
	ReplayLimit   int
	Buffer        int
	PollInterval  time.Duration
	KeyPrefix     string
	Coalesce      bool
}

// CommandJournalHistoricalSubscription delivers journal records with
// at-least-once restart semantics. Next marks a record as delivered; Commit
// must be called after the consumer applies it. A cancellation before Commit
// intentionally replays that record after restart instead of skipping it.
type CommandJournalHistoricalSubscription struct {
	subscription *CommandJournalSubscription
	coordinator  *CommandJournalSourceCheckpointCoordinator
	sourceID     string
	delivered    uint64
	checkpoint   uint64
	commitMu     sync.Mutex
}

// SubscribeHistoricalCommandJournal opens a bounded replay/live subscription
// from the durable source checkpoint. It adds no work to ordinary journal
// writes and does not create an extra delivery goroutine.
func (journal *CommandJournal) SubscribeHistoricalCommandJournal(ctx context.Context, options CommandJournalHistoricalSubscriptionOptions) (*CommandJournalHistoricalSubscription, error) {
	if journal == nil {
		return nil, ErrNilCommandJournal
	}
	coordinator, err := NewCommandJournalSourceCheckpointCoordinator(journal, options.Store)
	if err != nil {
		return nil, err
	}
	checkpoint, err := coordinator.Load(ctx, options.SourceID)
	if err != nil {
		return nil, err
	}
	startSequence := checkpoint.JournalSequence
	if startSequence != 0 && options.AfterSequence != 0 && options.AfterSequence != startSequence {
		return nil, ErrCommandJournalHistoricalSubscriptionCheckpointConflict
	}
	if startSequence == 0 {
		startSequence = options.AfterSequence
	}
	subscription, err := journal.Subscribe(ctx, CommandJournalSubscribeOptions{
		AfterSequence: startSequence,
		UpToSequence:  options.UpToSequence,
		ReplayLimit:   options.ReplayLimit,
		Buffer:        options.Buffer,
		PollInterval:  options.PollInterval,
		KeyPrefix:     options.KeyPrefix,
		Coalesce:      options.Coalesce,
	})
	if err != nil {
		return nil, err
	}
	return &CommandJournalHistoricalSubscription{
		subscription: subscription,
		coordinator:  coordinator,
		sourceID:     options.SourceID,
		delivered:    startSequence,
		checkpoint:   startSequence,
	}, nil
}

// Next returns the next record. A record is not durable until Commit returns
// successfully, so replay after cancellation is safe by construction.
func (subscription *CommandJournalHistoricalSubscription) Next(ctx context.Context) (CommandJournalRecord, bool, error) {
	if subscription == nil || subscription.subscription == nil {
		return CommandJournalRecord{}, false, ErrCommandJournalHistoricalSubscriptionClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return CommandJournalRecord{}, false, err
	}
	records := subscription.subscription.Records()
	var (
		record CommandJournalRecord
		ok     bool
	)
	if ctx.Done() == nil {
		record, ok = <-records
	} else {
		select {
		case <-ctx.Done():
			return CommandJournalRecord{}, false, ctx.Err()
		case record, ok = <-records:
		}
	}
	if !ok {
		if err := subscription.subscription.Err(); err != nil {
			return CommandJournalRecord{}, false, err
		}
		return CommandJournalRecord{}, false, nil
	}
	atomic.StoreUint64(&subscription.delivered, record.Sequence)
	return record, true, nil
}

// Commit persists exactly the greatest record returned by Next. The caller is
// responsible for applying that record before calling Commit.
func (subscription *CommandJournalHistoricalSubscription) Commit(ctx context.Context) (CommandJournalSourceCheckpoint, error) {
	if subscription == nil || subscription.coordinator == nil {
		return CommandJournalSourceCheckpoint{}, ErrCommandJournalHistoricalSubscriptionClosed
	}
	subscription.commitMu.Lock()
	defer subscription.commitMu.Unlock()
	sequence := atomic.LoadUint64(&subscription.delivered)
	current := atomic.LoadUint64(&subscription.checkpoint)
	if sequence < current {
		return CommandJournalSourceCheckpoint{}, ErrCommandJournalHistoricalSubscriptionCheckpointRegressed
	}
	checkpoint, err := subscription.coordinator.CommitJournalSequence(ctx, subscription.sourceID, sequence)
	if err != nil {
		return CommandJournalSourceCheckpoint{}, err
	}
	atomic.StoreUint64(&subscription.checkpoint, checkpoint.JournalSequence)
	return checkpoint, nil
}

// Checkpoint returns the last successfully committed sequence, not merely the
// last record delivered to the caller.
func (subscription *CommandJournalHistoricalSubscription) Checkpoint() CommandJournalSourceCheckpoint {
	if subscription == nil {
		return CommandJournalSourceCheckpoint{}
	}
	return CommandJournalSourceCheckpoint{JournalSequence: atomic.LoadUint64(&subscription.checkpoint)}
}

// DeliveredSequence returns the last record returned by Next. It is useful
// for diagnostics; it is not a restart-safe checkpoint until Commit succeeds.
func (subscription *CommandJournalHistoricalSubscription) DeliveredSequence() uint64 {
	if subscription == nil {
		return 0
	}
	return atomic.LoadUint64(&subscription.delivered)
}

// Err reports the terminal error from the bounded journal subscription.
func (subscription *CommandJournalHistoricalSubscription) Err() error {
	if subscription == nil || subscription.subscription == nil {
		return ErrCommandJournalHistoricalSubscriptionClosed
	}
	return subscription.subscription.Err()
}

// Close cancels the underlying subscription and waits for its records channel
// to close. It is safe to call more than once.
func (subscription *CommandJournalHistoricalSubscription) Close() {
	if subscription == nil || subscription.subscription == nil {
		return
	}
	subscription.subscription.Close()
}
