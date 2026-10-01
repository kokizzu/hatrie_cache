package hatCache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// CommandJournalSpaceFeedSchemaVersion identifies the version of the stable
// space-feed event and checkpoint envelope.
const CommandJournalSpaceFeedSchemaVersion uint16 = 1

// CommandJournalSpaceFeedBackpressurePolicy controls what happens when a
// consumer cannot keep up with the bounded feed buffer.
type CommandJournalSpaceFeedBackpressurePolicy uint8

const (
	// CommandJournalSpaceFeedFailFast preserves every matching journal record
	// and terminates the feed with ErrCommandJournalSubscriptionOverflow when
	// the configured buffer is full.
	CommandJournalSpaceFeedFailFast CommandJournalSpaceFeedBackpressurePolicy = iota
	// CommandJournalSpaceFeedCoalesceLatest keeps only the latest pending record
	// for the named space. This is lossy by design and must be acknowledged by
	// the consumer through the checkpoint it chooses to persist.
	CommandJournalSpaceFeedCoalesceLatest
)

var (
	ErrCommandJournalSpaceFeedCheckpoint   = errors.New("hatriecache: invalid journal space feed checkpoint")
	ErrCommandJournalSpaceFeedBackpressure = errors.New("hatriecache: invalid journal space feed backpressure policy")
	ErrCommandJournalSpaceFeedAck          = errors.New("hatriecache: invalid journal space feed acknowledgement")
	ErrCommandJournalSpaceFeedClosed       = errors.New("hatriecache: journal space feed is closed")
)

// CommandJournalSpaceFeedCheckpoint is the durable resume position for a
// named space. Sequence is the last record the consumer has acknowledged.
// Persisting this value after applying a record makes reconnects exclusive of
// the acknowledged sequence and avoids duplicate delivery.
type CommandJournalSpaceFeedCheckpoint struct {
	SchemaVersion uint16 `json:"schema_version"`
	Space         string `json:"space"`
	Sequence      uint64 `json:"sequence"`
}

// CommandJournalSpaceFeedEvent is a versioned journal record for one named
// space. Record remains the original lossless command-journal value.
type CommandJournalSpaceFeedEvent struct {
	SchemaVersion uint16
	Space         string
	Sequence      uint64
	Record        CommandJournalRecord
}

// CommandJournalSpaceFeedOptions configures a bounded named-space feed.
// Checkpoint is optional; its zero value starts at the beginning of the
// retained journal. Buffer and replay settings use the journal subscription
// defaults when zero.
type CommandJournalSpaceFeedOptions struct {
	Checkpoint   CommandJournalSpaceFeedCheckpoint
	ReplayLimit  int
	Buffer       int
	PollInterval time.Duration
	Backpressure CommandJournalSpaceFeedBackpressurePolicy
}

// CommandJournalSpaceFeed delivers ordered records for one named space and
// exposes explicit acknowledgement/checkpoint operations for reconnects.
type CommandJournalSpaceFeed struct {
	subscription *CommandJournalSubscription
	space        string

	mu         sync.RWMutex
	closed     bool
	delivered  uint64
	checkpoint CommandJournalSpaceFeedCheckpoint
}

// SubscribeSpaceFeed creates a versioned, resumable feed for one logical
// space. The feed is opt-in and does not change ordinary journal writes.
func (journal *CommandJournal) SubscribeSpaceFeed(ctx context.Context, space string, options CommandJournalSpaceFeedOptions) (*CommandJournalSpaceFeed, error) {
	if journal == nil {
		return nil, ErrNilCommandJournal
	}
	if strings.TrimSpace(space) == "" {
		return nil, ErrCommandJournalSubscriptionSpaceRequired
	}
	if options.Backpressure != CommandJournalSpaceFeedFailFast && options.Backpressure != CommandJournalSpaceFeedCoalesceLatest {
		return nil, ErrCommandJournalSpaceFeedBackpressure
	}
	checkpoint := options.Checkpoint
	if checkpoint.SchemaVersion == 0 && checkpoint.Space == "" && checkpoint.Sequence == 0 {
		checkpoint = CommandJournalSpaceFeedCheckpoint{}
	} else {
		if checkpoint.SchemaVersion != CommandJournalSpaceFeedSchemaVersion || checkpoint.Space != space {
			return nil, ErrCommandJournalSpaceFeedCheckpoint
		}
		if checkpoint.Sequence > journal.Sequence() {
			return nil, fmt.Errorf("%w: sequence %d is ahead of journal sequence %d", ErrCommandJournalSpaceFeedCheckpoint, checkpoint.Sequence, journal.Sequence())
		}
	}

	subscription, err := journal.SubscribeSpace(ctx, space, CommandJournalSubscribeOptions{
		AfterSequence: checkpoint.Sequence,
		ReplayLimit:   options.ReplayLimit,
		Buffer:        options.Buffer,
		PollInterval:  options.PollInterval,
		Coalesce:      options.Backpressure == CommandJournalSpaceFeedCoalesceLatest,
	})
	if err != nil {
		return nil, err
	}
	return &CommandJournalSpaceFeed{
		subscription: subscription,
		space:        space,
		delivered:    checkpoint.Sequence,
		checkpoint: CommandJournalSpaceFeedCheckpoint{
			SchemaVersion: CommandJournalSpaceFeedSchemaVersion,
			Space:         space,
			Sequence:      checkpoint.Sequence,
		},
	}, nil
}

// Next waits for the next feed event or context cancellation. io.EOF means a
// bounded or cleanly closed underlying journal subscription completed.
func (feed *CommandJournalSpaceFeed) Next(ctx context.Context) (CommandJournalSpaceFeedEvent, error) {
	if feed == nil || feed.subscription == nil {
		return CommandJournalSpaceFeedEvent{}, ErrCommandJournalSpaceFeedClosed
	}
	feed.mu.RLock()
	closed := feed.closed
	feed.mu.RUnlock()
	if closed {
		return CommandJournalSpaceFeedEvent{}, ErrCommandJournalSpaceFeedClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return CommandJournalSpaceFeedEvent{}, ctx.Err()
	case record, ok := <-feed.subscription.Records():
		if !ok {
			if err := feed.subscription.Err(); err != nil {
				return CommandJournalSpaceFeedEvent{}, err
			}
			return CommandJournalSpaceFeedEvent{}, io.EOF
		}
		feed.mu.Lock()
		if feed.closed {
			feed.mu.Unlock()
			return CommandJournalSpaceFeedEvent{}, ErrCommandJournalSpaceFeedClosed
		}
		if record.Sequence > feed.delivered {
			feed.delivered = record.Sequence
		}
		feed.mu.Unlock()
		return CommandJournalSpaceFeedEvent{
			SchemaVersion: CommandJournalSpaceFeedSchemaVersion,
			Space:         feed.space,
			Sequence:      record.Sequence,
			Record:        record,
		}, nil
	}
}

// Ack records the last successfully applied event. Acknowledgements are
// monotonic and cannot move beyond an event returned by Next.
func (feed *CommandJournalSpaceFeed) Ack(sequence uint64) error {
	if feed == nil {
		return ErrCommandJournalSpaceFeedClosed
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.closed {
		return ErrCommandJournalSpaceFeedClosed
	}
	if sequence == 0 || sequence > feed.delivered || sequence < feed.checkpoint.Sequence {
		return ErrCommandJournalSpaceFeedAck
	}
	feed.checkpoint.Sequence = sequence
	return nil
}

// Checkpoint returns a copy of the latest acknowledged resume position.
func (feed *CommandJournalSpaceFeed) Checkpoint() CommandJournalSpaceFeedCheckpoint {
	if feed == nil {
		return CommandJournalSpaceFeedCheckpoint{}
	}
	feed.mu.RLock()
	defer feed.mu.RUnlock()
	return feed.checkpoint
}

// Err returns the underlying subscription error, if any.
func (feed *CommandJournalSpaceFeed) Err() error {
	if feed == nil || feed.subscription == nil {
		return ErrCommandJournalSpaceFeedClosed
	}
	return feed.subscription.Err()
}

// Close stops the feed and releases its journal subscription. It is safe to
// call more than once.
func (feed *CommandJournalSpaceFeed) Close() {
	if feed == nil || feed.subscription == nil {
		return
	}
	feed.mu.Lock()
	if feed.closed {
		feed.mu.Unlock()
		return
	}
	feed.closed = true
	feed.mu.Unlock()
	feed.subscription.Close()
}
