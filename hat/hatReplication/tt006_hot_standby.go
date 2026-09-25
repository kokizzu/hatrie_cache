package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"hatrie_cache/hat/hatJournal"
)

const (
	// DefaultHotStandbyBatchSize bounds one fetch when the caller does not
	// provide a batch size.
	DefaultHotStandbyBatchSize = 256
	// MaxHotStandbyBatchSize prevents an unbounded allocation in a source
	// adapter.
	MaxHotStandbyBatchSize = 1 << 16
	// DefaultHotStandbyPollInterval keeps an idle standby from busy-spinning.
	DefaultHotStandbyPollInterval = 100 * time.Millisecond
	// MaxHotStandbyPollInterval prevents an accidental multi-hour recovery
	// pause from being hidden in a configuration value.
	MaxHotStandbyPollInterval = 24 * time.Hour
	// MaxHotStandbyIdentifierBytes bounds operator-visible node identifiers.
	MaxHotStandbyIdentifierBytes = 256
)

var (
	// ErrHotStandbyNil indicates a nil standby.
	ErrHotStandbyNil = errors.New("hatReplication: hot standby is nil")
	// ErrHotStandbyInvalid indicates invalid options or a malformed source
	// batch.
	ErrHotStandbyInvalid = errors.New("hatReplication: hot standby data is invalid")
	// ErrHotStandbyPhase indicates that an operation is not valid in the
	// current lifecycle phase.
	ErrHotStandbyPhase = errors.New("hatReplication: hot standby phase does not permit the operation")
	// ErrHotStandbyAlreadyStarted indicates that Run was called more than once.
	ErrHotStandbyAlreadyStarted = errors.New("hatReplication: hot standby already started")
	// ErrHotStandbyRunning indicates that promotion was attempted while the
	// replay loop is still running.
	ErrHotStandbyRunning = errors.New("hatReplication: hot standby replay is still running")
	// ErrHotStandbySequenceGap indicates that a source batch cannot be applied
	// without a snapshot or a fresh bootstrap.
	ErrHotStandbySequenceGap = errors.New("hatReplication: hot standby journal sequence gap")
	// ErrHotStandbySequenceRegression indicates that the source moved backward.
	ErrHotStandbySequenceRegression = errors.New("hatReplication: hot standby source sequence regressed")
	// ErrHotStandbyNotCaughtUp indicates that promotion would lose observed
	// source records.
	ErrHotStandbyNotCaughtUp = errors.New("hatReplication: hot standby is not caught up")
	// ErrHotStandbyFencing indicates a stale promotion fencing token.
	ErrHotStandbyFencing = errors.New("hatReplication: hot standby fencing token mismatch")
	// ErrHotStandbyGeneration indicates a stale status snapshot was used for a
	// lifecycle transition.
	ErrHotStandbyGeneration = errors.New("hatReplication: hot standby generation mismatch")
)

// HotStandbyPhase identifies the lifecycle of a continuous replay runner.
type HotStandbyPhase uint8

const (
	HotStandbyPhaseIdle HotStandbyPhase = iota
	HotStandbyPhaseCatchingUp
	HotStandbyPhaseCaughtUp
	HotStandbyPhaseStopped
	HotStandbyPhaseFailed
	HotStandbyPhaseActive
)

// String returns a stable status name for monitoring and logs.
func (phase HotStandbyPhase) String() string {
	switch phase {
	case HotStandbyPhaseIdle:
		return "idle"
	case HotStandbyPhaseCatchingUp:
		return "catching_up"
	case HotStandbyPhaseCaughtUp:
		return "caught_up"
	case HotStandbyPhaseStopped:
		return "stopped"
	case HotStandbyPhaseFailed:
		return "failed"
	case HotStandbyPhaseActive:
		return "active"
	default:
		return "unknown"
	}
}

// HotStandbyOptions configures one snapshot-installed standby. StartSequence
// is the last journal sequence already present in the standby snapshot. The
// caller supplies the source and apply adapters to keep transport, storage,
// and command semantics outside this package.
type HotStandbyOptions struct {
	StandbyID     string
	SourceID      string
	StartSequence uint64
	BatchSize     int
	PollInterval  time.Duration
	FencingToken  uint64
}

// HotStandbyBatch is one source observation. Records must be contiguous after
// the standby's current AppliedSequence. An empty batch is valid only when
// SourceSequence equals the applied sequence, which prevents a source adapter
// from hiding a missing or compacted WAL range.
type HotStandbyBatch struct {
	SourceSequence uint64
	Records        []hatJournal.Record
}

// HotStandbySource fetches records strictly after afterSequence. A source
// should return at most limit records and include its observed current tail in
// SourceSequence, even when no records are available.
type HotStandbySource interface {
	Fetch(context.Context, uint64, int) (HotStandbyBatch, error)
}

// HotStandbyApplier applies a validated batch atomically from the standby's
// point of view. The callback should return only after every record in the
// batch is durable or has been rolled back.
type HotStandbyApplier interface {
	Apply(context.Context, HotStandbyBatch) error
}

// HotStandbyFetchFunc adapts a function to HotStandbySource.
type HotStandbyFetchFunc func(context.Context, uint64, int) (HotStandbyBatch, error)

// Fetch implements HotStandbySource.
func (fetch HotStandbyFetchFunc) Fetch(ctx context.Context, afterSequence uint64, limit int) (HotStandbyBatch, error) {
	if fetch == nil {
		return HotStandbyBatch{}, ErrHotStandbyInvalid
	}
	return fetch(ctx, afterSequence, limit)
}

// HotStandbyApplyFunc adapts a function to HotStandbyApplier.
type HotStandbyApplyFunc func(context.Context, HotStandbyBatch) error

// Apply implements HotStandbyApplier.
func (apply HotStandbyApplyFunc) Apply(ctx context.Context, batch HotStandbyBatch) error {
	if apply == nil {
		return ErrHotStandbyInvalid
	}
	return apply(ctx, batch)
}

// HotStandbyState is an immutable status snapshot. Generation changes after
// every observable transition so promotion can reject stale operator state.
type HotStandbyState struct {
	StandbyID       string          `json:"standby_id,omitempty"`
	SourceID        string          `json:"source_id,omitempty"`
	Phase           HotStandbyPhase `json:"phase"`
	Generation      uint64          `json:"generation"`
	SourceSequence  uint64          `json:"source_sequence"`
	AppliedSequence uint64          `json:"applied_sequence"`
	Lag             uint64          `json:"lag"`
	Fetches         uint64          `json:"fetches"`
	Batches         uint64          `json:"batches"`
	Records         uint64          `json:"records"`
	LastFetchedAt   time.Time       `json:"last_fetched_at,omitempty"`
	LastAppliedAt   time.Time       `json:"last_applied_at,omitempty"`
	LastError       string          `json:"last_error,omitempty"`
}

// HotStandby continuously fetches and applies journal batches until its
// context is canceled or an adapter returns an error. Run is deliberately
// blocking: the embedding service owns the goroutine, lifecycle, and
// transport shutdown. It never starts hidden background work.
type HotStandby struct {
	mu      sync.RWMutex
	options HotStandbyOptions
	state   HotStandbyState
	started bool
	running bool
}

// NewHotStandby creates a replay runner starting at an already-installed
// snapshot boundary.
func NewHotStandby(options HotStandbyOptions) (*HotStandby, error) {
	standbyID, err := normalizeHotStandbyIdentifier(options.StandbyID, "standby ID")
	if err != nil {
		return nil, err
	}
	sourceID, err := normalizeHotStandbyIdentifier(options.SourceID, "source ID")
	if err != nil {
		return nil, err
	}
	if options.BatchSize < 0 || options.BatchSize > MaxHotStandbyBatchSize {
		return nil, fmt.Errorf("%w: batch size must be between 0 and %d", ErrHotStandbyInvalid, MaxHotStandbyBatchSize)
	}
	if options.PollInterval < 0 || options.PollInterval > MaxHotStandbyPollInterval {
		return nil, fmt.Errorf("%w: poll interval is outside the supported range", ErrHotStandbyInvalid)
	}
	if options.FencingToken == 0 {
		return nil, fmt.Errorf("%w: non-zero fencing token is required", ErrHotStandbyInvalid)
	}
	if options.BatchSize == 0 {
		options.BatchSize = DefaultHotStandbyBatchSize
	}
	if options.PollInterval == 0 {
		options.PollInterval = DefaultHotStandbyPollInterval
	}
	options.StandbyID = standbyID
	options.SourceID = sourceID
	return &HotStandby{
		options: options,
		state: HotStandbyState{
			StandbyID:       standbyID,
			SourceID:        sourceID,
			Phase:           HotStandbyPhaseIdle,
			Generation:      1,
			SourceSequence:  options.StartSequence,
			AppliedSequence: options.StartSequence,
		},
	}, nil
}

// Run starts one continuous replay lifecycle. A successful context shutdown
// returns the context error and leaves the standby Stopped; adapter errors
// leave it Failed and preserve the source/applied lag for diagnosis.
func (standby *HotStandby) Run(ctx context.Context, source HotStandbySource, applier HotStandbyApplier) (runErr error) {
	if standby == nil {
		return ErrHotStandbyNil
	}
	if ctx == nil || source == nil || applier == nil {
		return ErrHotStandbyInvalid
	}
	standby.mu.Lock()
	if standby.started {
		standby.mu.Unlock()
		return ErrHotStandbyAlreadyStarted
	}
	standby.started = true
	standby.running = true
	standby.state.Phase = HotStandbyPhaseCatchingUp
	standby.state.Generation++
	standby.mu.Unlock()
	defer func() {
		standby.mu.Lock()
		standby.running = false
		if runErr == nil || errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
			standby.state.Phase = HotStandbyPhaseStopped
			if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
				standby.state.LastError = ""
			}
		} else {
			standby.state.Phase = HotStandbyPhaseFailed
			standby.state.LastError = runErr.Error()
		}
		standby.state.Generation++
		standby.mu.Unlock()
	}()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		standby.mu.RLock()
		afterSequence := standby.state.AppliedSequence
		batchSize := standby.options.BatchSize
		standby.mu.RUnlock()
		batch, err := source.Fetch(ctx, afterSequence, batchSize)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := standby.observeBatch(afterSequence, batch); err != nil {
			return err
		}
		if len(batch.Records) > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := applier.Apply(ctx, batch); err != nil {
				return err
			}
			if err := standby.markApplied(batch); err != nil {
				return err
			}
			continue
		}
		if err := standby.waitForNextPoll(ctx); err != nil {
			return err
		}
	}
}

// Promote atomically changes a stopped, fully caught-up standby into the
// active phase. The caller must fence the old source using the same token
// before publishing writes; this method does not perform network or topology
// changes.
func (standby *HotStandby) Promote(expectedGeneration, fencingToken uint64) (HotStandbyState, error) {
	if standby == nil {
		return HotStandbyState{}, ErrHotStandbyNil
	}
	standby.mu.Lock()
	defer standby.mu.Unlock()
	if standby.running {
		return HotStandbyState{}, ErrHotStandbyRunning
	}
	if standby.state.Phase != HotStandbyPhaseStopped {
		return HotStandbyState{}, ErrHotStandbyPhase
	}
	if fencingToken != standby.options.FencingToken {
		return HotStandbyState{}, ErrHotStandbyFencing
	}
	if expectedGeneration != standby.state.Generation {
		return HotStandbyState{}, ErrHotStandbyGeneration
	}
	if standby.state.SourceSequence != standby.state.AppliedSequence {
		return HotStandbyState{}, ErrHotStandbyNotCaughtUp
	}
	standby.state.Phase = HotStandbyPhaseActive
	standby.state.Generation++
	return standby.state, nil
}

// Snapshot returns a consistent status copy.
func (standby *HotStandby) Snapshot() HotStandbyState {
	if standby == nil {
		return HotStandbyState{}
	}
	standby.mu.RLock()
	defer standby.mu.RUnlock()
	return standby.state
}

func (standby *HotStandby) observeBatch(afterSequence uint64, batch HotStandbyBatch) error {
	standby.mu.Lock()
	defer standby.mu.Unlock()
	if batch.SourceSequence < standby.state.SourceSequence {
		return fmt.Errorf("%w: observed %d after %d", ErrHotStandbySequenceRegression, batch.SourceSequence, standby.state.SourceSequence)
	}
	if batch.SourceSequence < afterSequence {
		return fmt.Errorf("%w: source sequence %d precedes requested sequence %d", ErrHotStandbyInvalid, batch.SourceSequence, afterSequence)
	}
	// Preserve a non-regressing source watermark even when the batch itself is
	// rejected. This makes failure status actionable without ever advancing the
	// applied sequence past data that the applier did not accept.
	standby.state.SourceSequence = batch.SourceSequence
	standby.state.Lag = batch.SourceSequence - standby.state.AppliedSequence
	standby.state.Fetches++
	standby.state.LastFetchedAt = time.Now().UTC()
	standby.state.Generation++
	if len(batch.Records) > standby.options.BatchSize {
		return fmt.Errorf("%w: source returned %d records for limit %d", ErrHotStandbyInvalid, len(batch.Records), standby.options.BatchSize)
	}
	if len(batch.Records) == 0 {
		if batch.SourceSequence != afterSequence {
			return fmt.Errorf("%w: empty batch hides records between %d and %d", ErrHotStandbySequenceGap, afterSequence, batch.SourceSequence)
		}
		standby.state.Phase = HotStandbyPhaseCaughtUp
	} else {
		if afterSequence == ^uint64(0) {
			return fmt.Errorf("%w: journal sequence is exhausted", ErrHotStandbyInvalid)
		}
		want := afterSequence + 1
		for _, record := range batch.Records {
			if record.Sequence != want {
				return fmt.Errorf("%w: expected %d, got %d", ErrHotStandbySequenceGap, want, record.Sequence)
			}
			want++
		}
		last := batch.Records[len(batch.Records)-1].Sequence
		if last > batch.SourceSequence {
			return fmt.Errorf("%w: last record %d exceeds source sequence %d", ErrHotStandbyInvalid, last, batch.SourceSequence)
		}
	}
	return nil
}

func (standby *HotStandby) markApplied(batch HotStandbyBatch) error {
	standby.mu.Lock()
	defer standby.mu.Unlock()
	if len(batch.Records) == 0 {
		return nil
	}
	last := batch.Records[len(batch.Records)-1].Sequence
	if last < standby.state.AppliedSequence {
		return fmt.Errorf("%w: applied sequence regressed from %d to %d", ErrHotStandbySequenceRegression, standby.state.AppliedSequence, last)
	}
	if last > standby.state.SourceSequence {
		return fmt.Errorf("%w: applied sequence %d exceeds source sequence %d", ErrHotStandbyInvalid, last, standby.state.SourceSequence)
	}
	standby.state.AppliedSequence = last
	standby.state.Lag = standby.state.SourceSequence - last
	standby.state.Batches++
	standby.state.Records += uint64(len(batch.Records))
	standby.state.LastAppliedAt = time.Now().UTC()
	if standby.state.Lag == 0 {
		standby.state.Phase = HotStandbyPhaseCaughtUp
	} else {
		standby.state.Phase = HotStandbyPhaseCatchingUp
	}
	standby.state.Generation++
	return nil
}

func (standby *HotStandby) waitForNextPoll(ctx context.Context) error {
	standby.mu.RLock()
	interval := standby.options.PollInterval
	standby.mu.RUnlock()
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func normalizeHotStandbyIdentifier(value, label string) (string, error) {
	value = strings.TrimSpace(value)
	if len(value) > MaxHotStandbyIdentifierBytes || strings.IndexByte(value, 0) >= 0 {
		return "", fmt.Errorf("%w: invalid %s", ErrHotStandbyInvalid, label)
	}
	return value, nil
}
