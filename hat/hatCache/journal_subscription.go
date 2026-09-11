package hatCache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultCommandJournalSubscriptionBuffer       = 256
	MaxCommandJournalSubscriptionBuffer           = 65536
	DefaultCommandJournalSubscriptionPollInterval = 10 * time.Millisecond
)

var (
	ErrCommandJournalSubscriptionReplayLimit   = errors.New("hatriecache: journal subscription replay limit exceeded")
	ErrCommandJournalSubscriptionOverflow      = errors.New("hatriecache: journal subscription buffer overflowed")
	ErrCommandJournalSubscriptionSpaceRequired = errors.New("hatriecache: journal subscription space is required")
)

const commandJournalSubscriptionEventBuffer = 1

// CommandJournalSubscribeOptions controls an opt-in subscription to durable
// command-journal records. AfterSequence is exclusive: sequence n starts
// delivery at n+1. A zero ReplayLimit, Buffer, or PollInterval selects its
// corresponding default.
type CommandJournalSubscribeOptions struct {
	AfterSequence uint64
	ReplayLimit   int
	Buffer        int
	PollInterval  time.Duration
}

// CommandJournalSubscription delivers a contiguous journal tail followed by
// newly appended records. Records is closed when the subscription stops.
// Err reports cancellation, overflow, compaction, or journal errors after the
// records channel has closed (or while shutdown is in progress).
type CommandJournalSubscription struct {
	records  chan CommandJournalRecord
	events   chan CommandJournalRecord
	stop     chan struct{}
	done     chan struct{}
	wake     <-chan struct{}
	spaceKey string

	stopOnce sync.Once
	errMu    sync.RWMutex
	err      error
}

// Subscribe replays the current journal tail after options.AfterSequence and
// then polls for new records. With no subscriptions, the journal only performs
// a cheap subscriber-count check and does no channel or disk work. A replay
// larger than ReplayLimit is rejected so a caller never silently starts after
// an unobserved gap.
func (journal *CommandJournal) Subscribe(ctx context.Context, options CommandJournalSubscribeOptions) (*CommandJournalSubscription, error) {
	return journal.subscribe(ctx, options, "")
}

// SubscribeSpace replays and follows only records whose command key exactly
// matches space. The space value is a logical partition label for callers that
// maintain a projection per cache key; unrelated journal sequences are skipped
// while preserving the global journal cursor.
func (journal *CommandJournal) SubscribeSpace(ctx context.Context, space string, options CommandJournalSubscribeOptions) (*CommandJournalSubscription, error) {
	if journal == nil {
		return nil, ErrNilCommandJournal
	}
	if space == "" {
		return nil, ErrCommandJournalSubscriptionSpaceRequired
	}
	return journal.subscribe(ctx, options, space)
}

func (journal *CommandJournal) subscribe(ctx context.Context, options CommandJournalSubscribeOptions, spaceKey string) (*CommandJournalSubscription, error) {
	if journal == nil {
		return nil, ErrNilCommandJournal
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	replayLimit, buffer, pollInterval, err := normalizeCommandJournalSubscriptionOptions(options)
	if err != nil {
		return nil, err
	}
	subscription := &CommandJournalSubscription{
		records:  make(chan CommandJournalRecord, buffer),
		events:   make(chan CommandJournalRecord, commandJournalSubscriptionEventBuffer),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		spaceKey: spaceKey,
	}
	wake := journal.registerCommandJournalSubscription(subscription)
	var tail CommandJournalTail
	if spaceKey == "" {
		tail, err = journal.commandJournalSubscriptionTail(options.AfterSequence, replayLimit)
	} else {
		tail, err = journal.commandJournalSubscriptionSpaceTail(options.AfterSequence, replayLimit, spaceKey)
	}
	if err != nil {
		journal.unregisterCommandJournalSubscription(subscription)
		return nil, err
	}
	if tail.HasMore {
		journal.unregisterCommandJournalSubscription(subscription)
		return nil, fmt.Errorf("%w: after sequence %d has more than %d records", ErrCommandJournalSubscriptionReplayLimit, options.AfterSequence, replayLimit)
	}

	subscription.wake = wake
	go subscription.run(ctx, journal, tail.LastSequence, tail.Entries, pollInterval)
	return subscription, nil
}

func normalizeCommandJournalSubscriptionOptions(options CommandJournalSubscribeOptions) (int, int, time.Duration, error) {
	replayLimit := options.ReplayLimit
	if replayLimit == 0 {
		replayLimit = DefaultCommandJournalTailLimit
	}
	if replayLimit < 0 || replayLimit > MaxCommandJournalTailLimit {
		return 0, 0, 0, fmt.Errorf("hatriecache: journal subscription replay limit must be between 1 and %d", MaxCommandJournalTailLimit)
	}

	buffer := options.Buffer
	if buffer == 0 {
		buffer = DefaultCommandJournalSubscriptionBuffer
	}
	if buffer < 0 || buffer > MaxCommandJournalSubscriptionBuffer {
		return 0, 0, 0, fmt.Errorf("hatriecache: journal subscription buffer must be between 1 and %d", MaxCommandJournalSubscriptionBuffer)
	}

	pollInterval := options.PollInterval
	if pollInterval == 0 {
		pollInterval = DefaultCommandJournalSubscriptionPollInterval
	}
	if pollInterval < 0 {
		return 0, 0, 0, errors.New("hatriecache: journal subscription poll interval must be non-negative")
	}
	return replayLimit, buffer, pollInterval, nil
}

func (journal *CommandJournal) commandJournalSubscriptionTail(afterSequence uint64, limit int) (CommandJournalTail, error) {
	journal.mu.Lock()
	if journal.closed {
		journal.mu.Unlock()
		return CommandJournalTail{}, ErrCommandJournalClosed
	}
	lastSequence := journal.lastSequenceLocked()
	if afterSequence >= lastSequence {
		journal.mu.Unlock()
		return CommandJournalTail{
			LastSequence: lastSequence,
			Limit:        limit,
			Entries:      []CommandJournalRecord{},
		}, nil
	}
	journal.mu.Unlock()
	return journal.Tail(afterSequence, limit)
}

func (journal *CommandJournal) commandJournalSubscriptionSpaceTail(afterSequence uint64, limit int, spaceKey string) (CommandJournalTail, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return CommandJournalTail{}, ErrCommandJournalClosed
	}
	tail, err := readCommandJournalSpaceTailSet(journal.path, journal.segmented(), afterSequence, limit, spaceKey)
	if err != nil {
		return CommandJournalTail{}, err
	}
	if afterSequence < tail.CompactedThrough {
		tail.Entries = []CommandJournalRecord{}
		tail.HasMore = false
		return tail, fmt.Errorf("%w: requested sequence %d is before compacted sequence %d", ErrCommandJournalCompacted, afterSequence, tail.CompactedThrough)
	}
	return tail, nil
}

// Records returns the subscription's bounded record channel.
func (subscription *CommandJournalSubscription) Records() <-chan CommandJournalRecord {
	if subscription == nil {
		return nil
	}
	return subscription.records
}

// Err returns the terminal subscription error. It is nil after an explicit
// Close unless the subscription had already stopped for another reason.
func (subscription *CommandJournalSubscription) Err() error {
	if subscription == nil {
		return nil
	}
	subscription.errMu.RLock()
	defer subscription.errMu.RUnlock()
	return subscription.err
}

// Close stops the subscription and waits until Records has been closed.
func (subscription *CommandJournalSubscription) Close() {
	if subscription == nil {
		return
	}
	subscription.stopOnce.Do(func() { close(subscription.stop) })
	<-subscription.done
}

func (subscription *CommandJournalSubscription) run(ctx context.Context, journal *CommandJournal, nextSequence uint64, replay []CommandJournalRecord, pollInterval time.Duration) {
	defer journal.unregisterCommandJournalSubscription(subscription)
	defer close(subscription.records)
	defer close(subscription.done)

	for _, record := range replay {
		select {
		case subscription.records <- record:
		case <-ctx.Done():
			subscription.setError(ctx.Err())
			return
		case <-subscription.stop:
			return
		}
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	wake := subscription.wake
	for {
		select {
		case <-ctx.Done():
			subscription.setError(ctx.Err())
			return
		case <-subscription.stop:
			return
		case <-journal.closeDone:
			if subscription.stopped() {
				return
			}
			for {
				select {
				case record := <-subscription.events:
					if record.Sequence <= nextSequence {
						continue
					}
					if !subscription.matches(record) {
						nextSequence = record.Sequence
						continue
					}
					if record.Sequence > nextSequence+1 && subscription.spaceKey == "" {
						if !subscription.stopped() {
							subscription.setError(ErrCommandJournalClosed)
						}
						return
					}
					if err := subscription.deliverLive(ctx, record); err != nil {
						if !subscription.stopped() {
							subscription.setError(err)
						}
						return
					}
					nextSequence = record.Sequence
				default:
					if !subscription.stopped() {
						subscription.setError(ErrCommandJournalClosed)
					}
					return
				}
			}
		case record := <-subscription.events:
			if record.Sequence <= nextSequence {
				continue
			}
			if !subscription.matches(record) {
				nextSequence = record.Sequence
				continue
			}
			if record.Sequence > nextSequence+1 && subscription.spaceKey == "" {
				wake = journal.commandJournalSubscriptionWake()
				var err error
				nextSequence, err = subscription.poll(journal, nextSequence)
				if err != nil {
					if !subscription.stopped() {
						subscription.setError(err)
					}
					return
				}
				continue
			}
			if err := subscription.deliverLive(ctx, record); err != nil {
				if !subscription.stopped() {
					subscription.setError(err)
				}
				return
			}
			nextSequence = record.Sequence
		case <-wake:
			wake = journal.commandJournalSubscriptionWake()
			var err error
			nextSequence, err = subscription.poll(journal, nextSequence)
			if err != nil {
				if !subscription.stopped() {
					subscription.setError(err)
				}
				return
			}
		case <-ticker.C:
			wake = journal.commandJournalSubscriptionWake()
			var err error
			nextSequence, err = subscription.poll(journal, nextSequence)
			if err != nil {
				if !subscription.stopped() {
					subscription.setError(err)
				}
				return
			}
		}
	}
}

func (journal *CommandJournal) registerCommandJournalSubscription(subscription *CommandJournalSubscription) <-chan struct{} {
	journal.subscriptionWakeMu.Lock()
	defer journal.subscriptionWakeMu.Unlock()
	if journal.subscriptionWake == nil {
		journal.subscriptionWake = make(chan struct{})
	}
	if journal.subscriptions == nil {
		journal.subscriptions = make(map[*CommandJournalSubscription]struct{})
	}
	journal.subscriptions[subscription] = struct{}{}
	atomic.AddUint64(&journal.subscriptionCount, 1)
	return journal.subscriptionWake
}

func (journal *CommandJournal) unregisterCommandJournalSubscription(subscription *CommandJournalSubscription) {
	if journal == nil {
		return
	}
	journal.subscriptionWakeMu.Lock()
	if _, exists := journal.subscriptions[subscription]; exists {
		delete(journal.subscriptions, subscription)
		atomic.AddUint64(&journal.subscriptionCount, ^uint64(0))
	}
	journal.subscriptionWakeMu.Unlock()
}

func (journal *CommandJournal) notifyCommandJournalSubscriptions(records ...CommandJournalRecord) {
	if journal == nil || atomic.LoadUint64(&journal.subscriptionCount) == 0 {
		return
	}
	journal.subscriptionWakeMu.Lock()
	defer journal.subscriptionWakeMu.Unlock()
	if atomic.LoadUint64(&journal.subscriptionCount) == 0 {
		return
	}
	if len(records) > 0 {
		for subscription := range journal.subscriptions {
			for _, record := range records {
				if !subscription.matches(record) {
					continue
				}
				select {
				case subscription.events <- record:
				default:
					subscription.setError(ErrCommandJournalSubscriptionOverflow)
					subscription.stopOnce.Do(func() { close(subscription.stop) })
					break
				}
			}
		}
		return
	}
	if journal.subscriptionWake == nil {
		journal.subscriptionWake = make(chan struct{})
	}
	close(journal.subscriptionWake)
	journal.subscriptionWake = make(chan struct{})
}

func (subscription *CommandJournalSubscription) matches(record CommandJournalRecord) bool {
	return subscription.spaceKey == "" || record.Request.Key == subscription.spaceKey
}

func (subscription *CommandJournalSubscription) deliverLive(ctx context.Context, record CommandJournalRecord) error {
	select {
	case subscription.records <- record:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-subscription.stop:
		return nil
	default:
		return ErrCommandJournalSubscriptionOverflow
	}
}

func (journal *CommandJournal) commandJournalSubscriptionWake() <-chan struct{} {
	journal.subscriptionWakeMu.Lock()
	defer journal.subscriptionWakeMu.Unlock()
	if journal.subscriptionWake == nil {
		journal.subscriptionWake = make(chan struct{})
	}
	return journal.subscriptionWake
}

func (subscription *CommandJournalSubscription) poll(journal *CommandJournal, nextSequence uint64) (uint64, error) {
	for {
		tail, err := journal.Tail(nextSequence, DefaultCommandJournalTailLimit)
		if err != nil {
			return nextSequence, err
		}
		for _, record := range tail.Entries {
			if !subscription.matches(record) {
				nextSequence = record.Sequence
				continue
			}
			select {
			case subscription.records <- record:
				nextSequence = record.Sequence
			case <-subscription.stop:
				return nextSequence, nil
			default:
				return nextSequence, ErrCommandJournalSubscriptionOverflow
			}
		}
		if !tail.HasMore {
			return nextSequence, nil
		}
		if len(tail.Entries) == 0 {
			return nextSequence, errors.New("hatriecache: journal subscription tail made no progress")
		}
	}
}

func (subscription *CommandJournalSubscription) stopped() bool {
	select {
	case <-subscription.stop:
		return true
	default:
		return false
	}
}

func (subscription *CommandJournalSubscription) setError(err error) {
	if err == nil {
		return
	}
	subscription.errMu.Lock()
	if subscription.err == nil {
		subscription.err = err
	}
	subscription.errMu.Unlock()
}
