package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// TypedTableArrangementHydrationState is the admission state of one named
// arrangement. The registry is opt-in; existing arrangements do not allocate
// or consult it unless a caller creates one.
type TypedTableArrangementHydrationState string

const (
	TypedTableArrangementHydrationHydrating TypedTableArrangementHydrationState = "hydrating"
	TypedTableArrangementHydrationReady     TypedTableArrangementHydrationState = "ready"
	TypedTableArrangementHydrationFailed    TypedTableArrangementHydrationState = "failed"
)

const (
	DefaultTypedTableArrangementHydrationAdmissionEntries = 1024
	MaxTypedTableArrangementHydrationAdmissionEntries     = 1_000_000
	MaxTypedTableArrangementHydrationAdmissionKeyBytes    = 1024
)

var (
	ErrTypedTableArrangementHydrationAdmissionInvalid = errors.New("typed table arrangement hydration admission is invalid")
	ErrTypedTableArrangementHydrationAdmissionLimit   = errors.New("typed table arrangement hydration admission limit exceeded")
	ErrTypedTableArrangementHydrationUnknown          = errors.New("typed table arrangement hydration key is unknown")
	ErrTypedTableArrangementHydrationFailed           = errors.New("typed table arrangement hydration failed")
	ErrTypedTableArrangementHydrationProgress         = errors.New("typed table arrangement hydration progress is invalid")
)

// TypedTableArrangementHydrationProgress is a detached, deterministic view of
// one arrangement's hydration state. Join arrangements populate the left and
// right fields; aggregate arrangements use Checkpoint and SourceSequence.
type TypedTableArrangementHydrationProgress struct {
	Key                 string                              `json:"key"`
	State               TypedTableArrangementHydrationState `json:"state"`
	Checkpoint          uint64                              `json:"checkpoint"`
	SourceSequence      uint64                              `json:"source_sequence"`
	Pending             uint64                              `json:"pending"`
	LeftCheckpoint      uint64                              `json:"left_checkpoint,omitempty"`
	LeftSourceSequence  uint64                              `json:"left_source_sequence,omitempty"`
	RightCheckpoint     uint64                              `json:"right_checkpoint,omitempty"`
	RightSourceSequence uint64                              `json:"right_source_sequence,omitempty"`
	Failure             string                              `json:"failure,omitempty"`
}

type typedTableArrangementHydrationEntry struct {
	progress TypedTableArrangementHydrationProgress
	err      error
	changed  chan struct{}
}

// TypedTableArrangementHydrationAdmission tracks bounded arrangement
// hydration and blocks a dependent query until every named arrangement is
// ready. It is deliberately separate from the arrangement registries so
// callers can choose their own query admission boundary.
type TypedTableArrangementHydrationAdmission struct {
	mu         sync.Mutex
	maxEntries int
	entries    map[string]*typedTableArrangementHydrationEntry
}

// NewTypedTableArrangementHydrationAdmission creates an opt-in bounded
// admission registry. A zero limit selects the conservative default.
func NewTypedTableArrangementHydrationAdmission(maxEntries int) (*TypedTableArrangementHydrationAdmission, error) {
	if maxEntries == 0 {
		maxEntries = DefaultTypedTableArrangementHydrationAdmissionEntries
	}
	if maxEntries < 0 || maxEntries > MaxTypedTableArrangementHydrationAdmissionEntries {
		return nil, fmt.Errorf("%w: max entries %d", ErrTypedTableArrangementHydrationAdmissionInvalid, maxEntries)
	}
	return &TypedTableArrangementHydrationAdmission{
		maxEntries: maxEntries,
		entries:    make(map[string]*typedTableArrangementHydrationEntry),
	}, nil
}

// Register adds or re-registers one arrangement. Re-registration is the
// explicit recovery path after a failed replay and may reset its checkpoint.
func (admission *TypedTableArrangementHydrationAdmission) Register(key string, checkpoint, sourceSequence uint64) error {
	key, err := normalizeTypedTableArrangementHydrationKey(key)
	if err != nil {
		return err
	}
	progress, err := aggregateHydrationProgress(key, checkpoint, sourceSequence)
	if err != nil {
		return err
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	entry := admission.entries[key]
	if entry == nil {
		if len(admission.entries) >= admission.maxEntries {
			return ErrTypedTableArrangementHydrationAdmissionLimit
		}
		entry = &typedTableArrangementHydrationEntry{changed: make(chan struct{})}
		admission.entries[key] = entry
	}
	entry.progress = progress
	entry.err = nil
	notifyTypedTableArrangementHydrationEntry(entry)
	return nil
}

// RegisterJoin adds or re-registers one two-input join arrangement.
func (admission *TypedTableArrangementHydrationAdmission) RegisterJoin(key string, freshness TypedTableJoinArrangementFreshness) error {
	key, err := normalizeTypedTableArrangementHydrationKey(key)
	if err != nil {
		return err
	}
	progress, err := joinHydrationProgress(key, freshness)
	if err != nil {
		return err
	}
	return admission.registerProgress(progress)
}

// Update advances an aggregate arrangement without allowing progress to move
// backwards. Register is required to intentionally reset a failed/restarted
// arrangement.
func (admission *TypedTableArrangementHydrationAdmission) Update(key string, checkpoint, sourceSequence uint64) error {
	key, err := normalizeTypedTableArrangementHydrationKey(key)
	if err != nil {
		return err
	}
	progress, err := aggregateHydrationProgress(key, checkpoint, sourceSequence)
	if err != nil {
		return err
	}
	return admission.updateProgress(progress)
}

// UpdateJoin advances a two-input join arrangement without allowing either
// input checkpoint to move backwards.
func (admission *TypedTableArrangementHydrationAdmission) UpdateJoin(key string, freshness TypedTableJoinArrangementFreshness) error {
	key, err := normalizeTypedTableArrangementHydrationKey(key)
	if err != nil {
		return err
	}
	progress, err := joinHydrationProgress(key, freshness)
	if err != nil {
		return err
	}
	return admission.updateProgress(progress)
}

// Complete marks an aggregate arrangement ready at sourceSequence.
func (admission *TypedTableArrangementHydrationAdmission) Complete(key string, sourceSequence uint64) error {
	return admission.Update(key, sourceSequence, sourceSequence)
}

// Fail publishes a terminal failure for one arrangement. A later Register
// call clears the failure and is the explicit retry/recovery boundary.
func (admission *TypedTableArrangementHydrationAdmission) Fail(key string, cause error) error {
	key, err := normalizeTypedTableArrangementHydrationKey(key)
	if err != nil {
		return err
	}
	if cause == nil {
		cause = ErrTypedTableArrangementHydrationFailed
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	entry := admission.entries[key]
	if entry == nil {
		return fmt.Errorf("%w: %s", ErrTypedTableArrangementHydrationUnknown, key)
	}
	entry.progress.State = TypedTableArrangementHydrationFailed
	entry.progress.Failure = cause.Error()
	entry.err = cause
	notifyTypedTableArrangementHydrationEntry(entry)
	return nil
}

// Admit waits until every named arrangement is ready, or returns a context
// cancellation, unknown-key, or terminal hydration error. The wait is
// notification-based and does not poll or create a goroutine per caller.
func (admission *TypedTableArrangementHydrationAdmission) Admit(ctx context.Context, keys ...string) error {
	if admission == nil {
		return fmt.Errorf("%w: nil admission", ErrTypedTableArrangementHydrationAdmissionInvalid)
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrTypedTableArrangementHydrationAdmissionInvalid)
	}
	if len(keys) == 1 {
		key, err := normalizeTypedTableArrangementHydrationKey(keys[0])
		if err != nil {
			return err
		}
		return admission.admitSingle(ctx, key)
	}
	normalized, err := normalizeTypedTableArrangementHydrationKeys(keys)
	if err != nil {
		return err
	}
	for {
		admission.mu.Lock()
		var wait <-chan struct{}
		for _, key := range normalized {
			entry := admission.entries[key]
			if entry == nil {
				admission.mu.Unlock()
				return fmt.Errorf("%w: %s", ErrTypedTableArrangementHydrationUnknown, key)
			}
			switch entry.progress.State {
			case TypedTableArrangementHydrationReady:
				continue
			case TypedTableArrangementHydrationFailed:
				cause := entry.err
				admission.mu.Unlock()
				if cause == nil {
					return ErrTypedTableArrangementHydrationFailed
				}
				return fmt.Errorf("%w: %w", ErrTypedTableArrangementHydrationFailed, cause)
			default:
				wait = entry.changed
			}
			break
		}
		admission.mu.Unlock()
		if wait == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wait:
		}
	}
}

func (admission *TypedTableArrangementHydrationAdmission) admitSingle(ctx context.Context, key string) error {
	for {
		admission.mu.Lock()
		entry := admission.entries[key]
		if entry == nil {
			admission.mu.Unlock()
			return fmt.Errorf("%w: %s", ErrTypedTableArrangementHydrationUnknown, key)
		}
		switch entry.progress.State {
		case TypedTableArrangementHydrationReady:
			admission.mu.Unlock()
			return nil
		case TypedTableArrangementHydrationFailed:
			cause := entry.err
			admission.mu.Unlock()
			if cause == nil {
				return ErrTypedTableArrangementHydrationFailed
			}
			return fmt.Errorf("%w: %w", ErrTypedTableArrangementHydrationFailed, cause)
		default:
			wait := entry.changed
			admission.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-wait:
			}
		}
	}
}

// Snapshot returns all progress in lexical key order and never exposes live
// registry state.
func (admission *TypedTableArrangementHydrationAdmission) Snapshot() []TypedTableArrangementHydrationProgress {
	if admission == nil {
		return nil
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	progress := make([]TypedTableArrangementHydrationProgress, 0, len(admission.entries))
	for _, entry := range admission.entries {
		if entry != nil {
			progress = append(progress, entry.progress)
		}
	}
	sort.Slice(progress, func(left, right int) bool { return progress[left].Key < progress[right].Key })
	return progress
}

// Remove deletes one arrangement and wakes any waiters so they observe the
// resulting unknown-key error instead of waiting forever.
func (admission *TypedTableArrangementHydrationAdmission) Remove(key string) bool {
	key, err := normalizeTypedTableArrangementHydrationKey(key)
	if err != nil || admission == nil {
		return false
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	entry := admission.entries[key]
	if entry == nil {
		return false
	}
	notifyTypedTableArrangementHydrationEntry(entry)
	delete(admission.entries, key)
	return true
}

// HydrateAggregate performs one bounded aggregate replay and publishes its
// progress. It checks the context before replay; callers can use smaller
// limits when they need cancellation during a long rebuild.
func (admission *TypedTableArrangementHydrationAdmission) HydrateAggregate(ctx context.Context, key string, arrangement *TypedTableAggregateArrangement, limit int) (TypedTableAggregateArrangementHydration, error) {
	if ctx == nil {
		return TypedTableAggregateArrangementHydration{}, fmt.Errorf("%w: nil context", ErrTypedTableArrangementHydrationAdmissionInvalid)
	}
	if arrangement == nil {
		return TypedTableAggregateArrangementHydration{}, fmt.Errorf("%w: nil aggregate arrangement", ErrTypedTableArrangementHydrationAdmissionInvalid)
	}
	if err := ctx.Err(); err != nil {
		return TypedTableAggregateArrangementHydration{}, err
	}
	freshness, err := arrangement.Freshness()
	if err != nil {
		return TypedTableAggregateArrangementHydration{}, err
	}
	if err := admission.Register(key, freshness.Checkpoint, freshness.SourceSequence); err != nil {
		return TypedTableAggregateArrangementHydration{}, err
	}
	report, err := arrangement.Hydrate(limit)
	if err != nil {
		_ = admission.Fail(key, err)
		return TypedTableAggregateArrangementHydration{}, err
	}
	updated, err := arrangement.Freshness()
	if err != nil {
		_ = admission.Fail(key, err)
		return TypedTableAggregateArrangementHydration{}, err
	}
	if err := admission.Update(key, updated.Checkpoint, updated.SourceSequence); err != nil {
		_ = admission.Fail(key, err)
		return TypedTableAggregateArrangementHydration{}, err
	}
	return report, nil
}

// HydrateJoin performs one bounded two-input replay and publishes its
// progress.
func (admission *TypedTableArrangementHydrationAdmission) HydrateJoin(ctx context.Context, key string, arrangement *TypedTableJoinArrangement, limit int) (TypedTableJoinArrangementHydration, error) {
	if ctx == nil {
		return TypedTableJoinArrangementHydration{}, fmt.Errorf("%w: nil context", ErrTypedTableArrangementHydrationAdmissionInvalid)
	}
	if arrangement == nil {
		return TypedTableJoinArrangementHydration{}, fmt.Errorf("%w: nil join arrangement", ErrTypedTableArrangementHydrationAdmissionInvalid)
	}
	if err := ctx.Err(); err != nil {
		return TypedTableJoinArrangementHydration{}, err
	}
	freshness, err := arrangement.Freshness()
	if err != nil {
		return TypedTableJoinArrangementHydration{}, err
	}
	if err := admission.RegisterJoin(key, freshness); err != nil {
		return TypedTableJoinArrangementHydration{}, err
	}
	report, err := arrangement.Hydrate(limit)
	if err != nil {
		_ = admission.Fail(key, err)
		return TypedTableJoinArrangementHydration{}, err
	}
	updated, err := arrangement.Freshness()
	if err != nil {
		_ = admission.Fail(key, err)
		return TypedTableJoinArrangementHydration{}, err
	}
	if err := admission.UpdateJoin(key, updated); err != nil {
		_ = admission.Fail(key, err)
		return TypedTableJoinArrangementHydration{}, err
	}
	return report, nil
}

func (admission *TypedTableArrangementHydrationAdmission) registerProgress(progress TypedTableArrangementHydrationProgress) error {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	entry := admission.entries[progress.Key]
	if entry == nil {
		if len(admission.entries) >= admission.maxEntries {
			return ErrTypedTableArrangementHydrationAdmissionLimit
		}
		entry = &typedTableArrangementHydrationEntry{changed: make(chan struct{})}
		admission.entries[progress.Key] = entry
	}
	entry.progress = progress
	entry.err = nil
	notifyTypedTableArrangementHydrationEntry(entry)
	return nil
}

func (admission *TypedTableArrangementHydrationAdmission) updateProgress(progress TypedTableArrangementHydrationProgress) error {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	entry := admission.entries[progress.Key]
	if entry == nil {
		return fmt.Errorf("%w: %s", ErrTypedTableArrangementHydrationUnknown, progress.Key)
	}
	if entry.progress.State == TypedTableArrangementHydrationFailed {
		return ErrTypedTableArrangementHydrationFailed
	}
	if typedTableArrangementHydrationRegressed(entry.progress, progress) {
		return ErrTypedTableArrangementHydrationProgress
	}
	entry.progress = progress
	entry.err = nil
	notifyTypedTableArrangementHydrationEntry(entry)
	return nil
}

func notifyTypedTableArrangementHydrationEntry(entry *typedTableArrangementHydrationEntry) {
	if entry.changed != nil {
		close(entry.changed)
	}
	entry.changed = make(chan struct{})
}

func aggregateHydrationProgress(key string, checkpoint, sourceSequence uint64) (TypedTableArrangementHydrationProgress, error) {
	if checkpoint > sourceSequence {
		return TypedTableArrangementHydrationProgress{}, ErrTypedTableArrangementHydrationProgress
	}
	return TypedTableArrangementHydrationProgress{
		Key:            key,
		State:          typedTableArrangementHydrationState(checkpoint, sourceSequence),
		Checkpoint:     checkpoint,
		SourceSequence: sourceSequence,
		Pending:        sourceSequence - checkpoint,
	}, nil
}

func joinHydrationProgress(key string, freshness TypedTableJoinArrangementFreshness) (TypedTableArrangementHydrationProgress, error) {
	if freshness.LeftCheckpoint > freshness.LeftSourceSequence || freshness.RightCheckpoint > freshness.RightSourceSequence {
		return TypedTableArrangementHydrationProgress{}, ErrTypedTableArrangementHydrationProgress
	}
	pending := saturatingTypedTableArrangementHydrationAdd(
		freshness.LeftSourceSequence-freshness.LeftCheckpoint,
		freshness.RightSourceSequence-freshness.RightCheckpoint,
	)
	state := TypedTableArrangementHydrationReady
	if pending > 0 {
		state = TypedTableArrangementHydrationHydrating
	}
	return TypedTableArrangementHydrationProgress{
		Key:                 key,
		State:               state,
		Pending:             pending,
		LeftCheckpoint:      freshness.LeftCheckpoint,
		LeftSourceSequence:  freshness.LeftSourceSequence,
		RightCheckpoint:     freshness.RightCheckpoint,
		RightSourceSequence: freshness.RightSourceSequence,
	}, nil
}

func saturatingTypedTableArrangementHydrationAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}

func typedTableArrangementHydrationState(checkpoint, sourceSequence uint64) TypedTableArrangementHydrationState {
	if checkpoint == sourceSequence {
		return TypedTableArrangementHydrationReady
	}
	return TypedTableArrangementHydrationHydrating
}

func typedTableArrangementHydrationRegressed(previous, next TypedTableArrangementHydrationProgress) bool {
	if next.Checkpoint < previous.Checkpoint || next.SourceSequence < previous.SourceSequence || next.LeftCheckpoint < previous.LeftCheckpoint || next.LeftSourceSequence < previous.LeftSourceSequence || next.RightCheckpoint < previous.RightCheckpoint || next.RightSourceSequence < previous.RightSourceSequence {
		return true
	}
	return false
}

func normalizeTypedTableArrangementHydrationKey(key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" || len(key) > MaxTypedTableArrangementHydrationAdmissionKeyBytes {
		return "", fmt.Errorf("%w: invalid key", ErrTypedTableArrangementHydrationAdmissionInvalid)
	}
	return key, nil
}

func normalizeTypedTableArrangementHydrationKeys(keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	result := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		normalized, err := normalizeTypedTableArrangementHydrationKey(key)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	sort.Strings(result)
	return result, nil
}
