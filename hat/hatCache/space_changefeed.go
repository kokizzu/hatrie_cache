package hatCache

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"
)

const DefaultSpaceChangefeedSchemaVersion uint64 = 1

// SpaceChangefeedOperation describes the state transition represented by a
// journal record. A retraction removes a previously materialized value from a
// downstream projection; it does not imply that the journal record itself was
// removed.
type SpaceChangefeedOperation string

const (
	SpaceChangefeedUpsert     SpaceChangefeedOperation = "upsert"
	SpaceChangefeedRetraction SpaceChangefeedOperation = "retraction"
)

var (
	ErrSpaceChangefeedNameRequired       = errors.New("hatriecache: space changefeed name is required")
	ErrSpaceChangefeedSchemaRequired     = errors.New("hatriecache: space changefeed schema version is required")
	ErrSpaceChangefeedSchemaMismatch     = errors.New("hatriecache: space changefeed schema version mismatch")
	ErrSpaceChangefeedCheckpointMismatch = errors.New("hatriecache: space changefeed checkpoint does not match the feed")
	ErrSpaceChangefeedClosed             = errors.New("hatriecache: space changefeed is closed")
)

// SpaceChangefeedOptions names a logical stream over command-journal records.
// KeyPrefix is the physical key boundary for the stream. The stream keeps the
// journal's global sequence numbers, so a checkpoint can resume without
// replaying a matching record twice even when unrelated keys are interleaved.
// RetractionCommands extends the built-in delete command set for applications
// that use custom mutation names.
type SpaceChangefeedOptions struct {
	Name               string
	SchemaVersion      uint64
	KeyPrefix          string
	AfterSequence      uint64
	UpToSequence       uint64
	ReplayLimit        int
	Buffer             int
	PollInterval       time.Duration
	SkipReplay         bool
	Coalesce           bool
	RetractionCommands []string
}

// SpaceChangefeedCheckpoint identifies the last event returned by Next.
// Checkpoints are intentionally tied to both the logical name and schema
// version so an incompatible consumer cannot silently resume a stream.
type SpaceChangefeedCheckpoint struct {
	Name          string `json:"name"`
	SchemaVersion uint64 `json:"schema_version"`
	Sequence      uint64 `json:"sequence"`
}

// SpaceChangefeedEvent is the versioned, named envelope delivered to a
// consumer. Request is the original journal request and Sequence preserves
// the journal's total ordering.
type SpaceChangefeedEvent struct {
	Space         string                   `json:"space"`
	SchemaVersion uint64                   `json:"schema_version"`
	Sequence      uint64                   `json:"sequence"`
	Operation     SpaceChangefeedOperation `json:"operation"`
	Retraction    bool                     `json:"retraction"`
	Request       CacheCommandRequest      `json:"request"`
}

// SpaceChangefeed is a low-allocation view over a bounded command-journal
// subscription. Next provides pull-based backpressure: the wrapper does not
// add an unbounded queue or a conversion goroutine on top of the journal
// subscription.
type SpaceChangefeed struct {
	subscription  *CommandJournalSubscription
	name          string
	schemaVersion uint64
	retractions   map[string]struct{}

	checkpointSequence atomic.Uint64
}

// SubscribeSpaceChangefeed opens a named, versioned changefeed. The existing
// journal subscription owns replay limits, bounded buffering, overflow
// reporting, and compaction errors; this API only adds the stable space
// envelope and checkpoint semantics.
func (journal *CommandJournal) SubscribeSpaceChangefeed(ctx context.Context, options SpaceChangefeedOptions) (*SpaceChangefeed, error) {
	if journal == nil {
		return nil, ErrNilCommandJournal
	}
	if err := validateSpaceChangefeedOptions(options); err != nil {
		return nil, err
	}

	subscription, err := journal.Subscribe(ctx, CommandJournalSubscribeOptions{
		AfterSequence: options.AfterSequence,
		UpToSequence:  options.UpToSequence,
		ReplayLimit:   options.ReplayLimit,
		Buffer:        options.Buffer,
		PollInterval:  options.PollInterval,
		SkipReplay:    options.SkipReplay,
		KeyPrefix:     options.KeyPrefix,
		Coalesce:      options.Coalesce,
	})
	if err != nil {
		return nil, err
	}

	feed := &SpaceChangefeed{
		subscription:  subscription,
		name:          options.Name,
		schemaVersion: options.SchemaVersion,
		retractions:   spaceChangefeedRetractionSet(options.RetractionCommands),
	}
	feed.checkpointSequence.Store(options.AfterSequence)
	return feed, nil
}

// ResumeSpaceChangefeed reconnects from a previously returned checkpoint.
// The caller must repeat the feed name and schema version explicitly; this
// makes accidental cross-feed or cross-schema restores fail closed.
func (journal *CommandJournal) ResumeSpaceChangefeed(ctx context.Context, checkpoint SpaceChangefeedCheckpoint, options SpaceChangefeedOptions) (*SpaceChangefeed, error) {
	if checkpoint.Name == "" {
		return nil, ErrSpaceChangefeedNameRequired
	}
	if checkpoint.SchemaVersion == 0 || options.SchemaVersion == 0 || checkpoint.SchemaVersion != options.SchemaVersion {
		return nil, ErrSpaceChangefeedSchemaMismatch
	}
	if options.Name == "" {
		return nil, ErrSpaceChangefeedNameRequired
	}
	if options.Name != checkpoint.Name {
		return nil, ErrSpaceChangefeedCheckpointMismatch
	}
	if options.AfterSequence != 0 && options.AfterSequence != checkpoint.Sequence {
		return nil, ErrSpaceChangefeedCheckpointMismatch
	}
	options.AfterSequence = checkpoint.Sequence
	options.SkipReplay = false
	feed, err := journal.SubscribeSpaceChangefeed(ctx, options)
	if err != nil {
		return nil, err
	}
	return feed, nil
}

// Next returns the next event and advances the checkpoint only after the
// event has been received by the caller. A closed feed returns ok=false and
// reports the underlying subscription error, if any.
func (feed *SpaceChangefeed) Next(ctx context.Context) (SpaceChangefeedEvent, bool, error) {
	if feed == nil || feed.subscription == nil {
		return SpaceChangefeedEvent{}, false, ErrSpaceChangefeedClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	records := feed.subscription.Records()
	var (
		record CommandJournalRecord
		ok     bool
	)
	if ctx.Done() == nil {
		record, ok = <-records
	} else {
		select {
		case <-ctx.Done():
			return SpaceChangefeedEvent{}, false, ctx.Err()
		case record, ok = <-records:
		}
	}
	if !ok {
		if err := feed.subscription.Err(); err != nil {
			return SpaceChangefeedEvent{}, false, err
		}
		return SpaceChangefeedEvent{}, false, nil
	}
	event := feed.eventFromRecord(record)
	feed.checkpointSequence.Store(record.Sequence)
	return event, true, nil
}

// Checkpoint returns the last event successfully returned by Next.
func (feed *SpaceChangefeed) Checkpoint() SpaceChangefeedCheckpoint {
	if feed == nil {
		return SpaceChangefeedCheckpoint{}
	}
	return SpaceChangefeedCheckpoint{
		Name:          feed.name,
		SchemaVersion: feed.schemaVersion,
		Sequence:      feed.checkpointSequence.Load(),
	}
}

// Err reports the underlying subscription error. It remains nil for an
// ordinary, explicitly closed feed.
func (feed *SpaceChangefeed) Err() error {
	if feed == nil || feed.subscription == nil {
		return ErrSpaceChangefeedClosed
	}
	return feed.subscription.Err()
}

// Close stops the bounded journal subscription. It is safe to call more than
// once.
func (feed *SpaceChangefeed) Close() error {
	if feed == nil || feed.subscription == nil {
		return nil
	}
	feed.subscription.Close()
	return nil
}

func validateSpaceChangefeedOptions(options SpaceChangefeedOptions) error {
	if strings.TrimSpace(options.Name) == "" {
		return ErrSpaceChangefeedNameRequired
	}
	if options.SchemaVersion == 0 {
		return ErrSpaceChangefeedSchemaRequired
	}
	return nil
}

func spaceChangefeedRetractionSet(commands []string) map[string]struct{} {
	if len(commands) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(commands))
	for _, command := range commands {
		command = strings.ToUpper(strings.TrimSpace(command))
		if command != "" {
			set[command] = struct{}{}
		}
	}
	return set
}

func (feed *SpaceChangefeed) isRetraction(command string) bool {
	if feed.retractions != nil {
		if _, ok := feed.retractions[command]; ok {
			return true
		}
	}
	switch command {
	case "DEL", "DELCF", "DELRT", "INTERNALDEL", "RBDEL", "REMRB", "REMSET":
		return true
	default:
		return false
	}
}

func (feed *SpaceChangefeed) eventFromRecord(record CommandJournalRecord) SpaceChangefeedEvent {
	event := SpaceChangefeedEvent{
		Space:         feed.name,
		SchemaVersion: feed.schemaVersion,
		Sequence:      record.Sequence,
		Operation:     SpaceChangefeedUpsert,
		Request:       record.Request,
	}
	if feed.isRetraction(record.Request.Command) {
		event.Operation = SpaceChangefeedRetraction
		event.Retraction = true
	}
	return event
}
