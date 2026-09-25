package hatSql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultReplicaHotHandoffBatchSize bounds one incremental query-state
	// transfer when the caller does not provide a size.
	DefaultReplicaHotHandoffBatchSize = 256
	// MaxReplicaHotHandoffBatchSize prevents a source adapter from forcing an
	// unbounded delta batch into the target.
	MaxReplicaHotHandoffBatchSize = 1 << 16
	// DefaultReplicaHotHandoffPollInterval avoids busy-spinning while a source
	// is producing deltas faster than a replacement replica can apply them.
	DefaultReplicaHotHandoffPollInterval = 100 * time.Millisecond
	// MaxReplicaHotHandoffPollInterval bounds an accidental recovery pause.
	MaxReplicaHotHandoffPollInterval = 24 * time.Hour
	// DefaultReplicaHotHandoffSnapshotBytes bounds one transferred query-state
	// image when the caller does not provide a smaller limit.
	DefaultReplicaHotHandoffSnapshotBytes = 64 << 20
	// MaxReplicaHotHandoffSnapshotBytes prevents an accidental enormous
	// in-memory image from being accepted by the coordinator.
	MaxReplicaHotHandoffSnapshotBytes = 1 << 30
	// MaxReplicaHotHandoffIdentifierBytes bounds operator-visible identifiers.
	MaxReplicaHotHandoffIdentifierBytes = 256
)

var (
	// ErrReplicaHotHandoffNil indicates a method call on a nil handoff.
	ErrReplicaHotHandoffNil = errors.New("hatSql: replica hot handoff is nil")
	// ErrReplicaHotHandoffInvalid indicates invalid options or adapter input.
	ErrReplicaHotHandoffInvalid = errors.New("hatSql: replica hot handoff data is invalid")
	// ErrReplicaHotHandoffPhase indicates that a lifecycle operation is not
	// valid in the current handoff phase.
	ErrReplicaHotHandoffPhase = errors.New("hatSql: replica hot handoff phase does not permit the operation")
	// ErrReplicaHotHandoffRunning indicates that a second sync was attempted
	// while the first sync is still using the target adapter.
	ErrReplicaHotHandoffRunning = errors.New("hatSql: replica hot handoff sync is already running")
	// ErrReplicaHotHandoffSequenceGap indicates that a delta cannot be applied
	// without a missing query-state update.
	ErrReplicaHotHandoffSequenceGap = errors.New("hatSql: replica hot handoff sequence gap")
	// ErrReplicaHotHandoffSequenceRegression indicates that a source frontier
	// moved backward.
	ErrReplicaHotHandoffSequenceRegression = errors.New("hatSql: replica hot handoff source frontier regressed")
	// ErrReplicaHotHandoffEpochMismatch indicates that a batch belongs to a
	// different source generation than the installed snapshot.
	ErrReplicaHotHandoffEpochMismatch = errors.New("hatSql: replica hot handoff epoch mismatch")
	// ErrReplicaHotHandoffSchemaMismatch indicates that the snapshot schema is
	// not the schema expected by the replacement replica.
	ErrReplicaHotHandoffSchemaMismatch = errors.New("hatSql: replica hot handoff schema mismatch")
	// ErrReplicaHotHandoffSnapshotTooLarge indicates that the image exceeded
	// the configured handoff limit.
	ErrReplicaHotHandoffSnapshotTooLarge = errors.New("hatSql: replica hot handoff snapshot is too large")
	// ErrReplicaHotHandoffNotReady indicates that promotion would cut over a
	// replica with an unobserved source frontier.
	ErrReplicaHotHandoffNotReady = errors.New("hatSql: replica hot handoff is not ready")
	// ErrReplicaHotHandoffGeneration indicates stale lifecycle state was used
	// for promotion.
	ErrReplicaHotHandoffGeneration = errors.New("hatSql: replica hot handoff generation mismatch")
	// ErrReplicaHotHandoffFencing indicates a stale source-fencing token.
	ErrReplicaHotHandoffFencing = errors.New("hatSql: replica hot handoff fencing token mismatch")
)

// ReplicaHotHandoffPhase identifies the lifecycle of one replacement replica.
type ReplicaHotHandoffPhase uint8

const (
	ReplicaHotHandoffPhaseIdle ReplicaHotHandoffPhase = iota
	ReplicaHotHandoffPhaseInstalling
	ReplicaHotHandoffPhaseCatchingUp
	ReplicaHotHandoffPhaseReady
	ReplicaHotHandoffPhasePromoted
	ReplicaHotHandoffPhaseFailed
)

// String returns a stable monitoring name for the handoff phase.
func (phase ReplicaHotHandoffPhase) String() string {
	switch phase {
	case ReplicaHotHandoffPhaseIdle:
		return "idle"
	case ReplicaHotHandoffPhaseInstalling:
		return "installing"
	case ReplicaHotHandoffPhaseCatchingUp:
		return "catching_up"
	case ReplicaHotHandoffPhaseReady:
		return "ready"
	case ReplicaHotHandoffPhasePromoted:
		return "promoted"
	case ReplicaHotHandoffPhaseFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// ReplicaHotHandoffOptions configures one opt-in replacement-replica
// coordinator. The fencing token is checked when the caller promotes the
// prepared target; publishing routes and fencing the old source remain the
// caller's responsibility.
type ReplicaHotHandoffOptions struct {
	SourceID              string
	TargetID              string
	ExpectedSchemaVersion uint64
	BatchSize             int
	PollInterval          time.Duration
	FencingToken          uint64
	MaxSnapshotBytes      int
}

// ReplicaHotHandoffSnapshot is an immutable-by-convention query-state image
// captured at Frontier. Payload is passed to InstallSnapshot synchronously and
// is not retained by the coordinator after that callback returns.
type ReplicaHotHandoffSnapshot struct {
	SourceID      string
	SnapshotID    string
	Epoch         uint64
	SchemaVersion uint64
	Frontier      uint64
	Payload       []byte
}

// ReplicaHotHandoffDelta is one ordered update after an installed snapshot.
// The target applies deltas in sequence order through its atomic Apply method.
type ReplicaHotHandoffDelta struct {
	Sequence uint64
	Payload  []byte
}

// ReplicaHotHandoffBatch is one source observation. SourceFrontier is the
// source's observed current frontier, including when the returned delta list
// is empty. A batch may contain fewer deltas than the frontier distance when a
// bounded source page is still catching up.
type ReplicaHotHandoffBatch struct {
	Epoch          uint64
	SourceFrontier uint64
	Deltas         []ReplicaHotHandoffDelta
}

// ReplicaHotHandoffSource supplies a consistent query-state image and ordered
// updates. Snapshot and Fetch are deliberately transport-neutral so callers
// can use local memory, gRPC, a journal, or an object-store transfer. The
// snapshot payload must remain valid for the synchronous InstallSnapshot call;
// a target that retains it after that call must copy it itself.
type ReplicaHotHandoffSource interface {
	Snapshot(context.Context) (ReplicaHotHandoffSnapshot, error)
	Fetch(context.Context, uint64, int) (ReplicaHotHandoffBatch, error)
}

// ReplicaHotHandoffTarget atomically installs an image, applies ordered
// updates, and confirms that the warmed query state is safe to serve. The
// callbacks must return only after their respective state transition is
// durable or rolled back by the caller.
type ReplicaHotHandoffTarget interface {
	InstallSnapshot(context.Context, ReplicaHotHandoffSnapshot) error
	Apply(context.Context, ReplicaHotHandoffBatch) error
	Ready(context.Context) error
}

// ReplicaHotHandoffSnapshotFunc adapts a snapshot callback to a source.
type ReplicaHotHandoffSnapshotFunc func(context.Context) (ReplicaHotHandoffSnapshot, error)

// Snapshot implements the snapshot portion of ReplicaHotHandoffSource. It is
// useful with a separate ReplicaHotHandoffFetchFunc adapter.
func (fetch ReplicaHotHandoffSnapshotFunc) Snapshot(ctx context.Context) (ReplicaHotHandoffSnapshot, error) {
	if fetch == nil {
		return ReplicaHotHandoffSnapshot{}, ErrReplicaHotHandoffInvalid
	}
	return fetch(ctx)
}

// ReplicaHotHandoffFetchFunc adapts a delta callback to a source.
type ReplicaHotHandoffFetchFunc func(context.Context, uint64, int) (ReplicaHotHandoffBatch, error)

// Fetch implements ReplicaHotHandoffSource. It is useful with a separate
// ReplicaHotHandoffSnapshotFunc adapter.
func (fetch ReplicaHotHandoffFetchFunc) Fetch(ctx context.Context, after uint64, limit int) (ReplicaHotHandoffBatch, error) {
	if fetch == nil {
		return ReplicaHotHandoffBatch{}, ErrReplicaHotHandoffInvalid
	}
	return fetch(ctx, after, limit)
}

// ReplicaHotHandoffInstallFunc adapts the snapshot installation callback.
type ReplicaHotHandoffInstallFunc func(context.Context, ReplicaHotHandoffSnapshot) error

// InstallSnapshot implements the snapshot installation portion of a target.
func (install ReplicaHotHandoffInstallFunc) InstallSnapshot(ctx context.Context, snapshot ReplicaHotHandoffSnapshot) error {
	if install == nil {
		return ErrReplicaHotHandoffInvalid
	}
	return install(ctx, snapshot)
}

// ReplicaHotHandoffApplyFunc adapts the delta application callback.
type ReplicaHotHandoffApplyFunc func(context.Context, ReplicaHotHandoffBatch) error

// Apply implements the delta application portion of a target.
func (apply ReplicaHotHandoffApplyFunc) Apply(ctx context.Context, batch ReplicaHotHandoffBatch) error {
	if apply == nil {
		return ErrReplicaHotHandoffInvalid
	}
	return apply(ctx, batch)
}

// ReplicaHotHandoffReadyFunc adapts the readiness callback.
type ReplicaHotHandoffReadyFunc func(context.Context) error

// Ready implements the readiness portion of a target.
func (ready ReplicaHotHandoffReadyFunc) Ready(ctx context.Context) error {
	if ready == nil {
		return ErrReplicaHotHandoffInvalid
	}
	return ready(ctx)
}

// ReplicaHotHandoffState is an immutable status snapshot. Generation changes
// on every lifecycle transition, allowing operators to reject stale promotion
// observations.
type ReplicaHotHandoffState struct {
	SourceID         string                 `json:"source_id,omitempty"`
	TargetID         string                 `json:"target_id,omitempty"`
	SnapshotID       string                 `json:"snapshot_id,omitempty"`
	Phase            ReplicaHotHandoffPhase `json:"phase"`
	Generation       uint64                 `json:"generation"`
	Epoch            uint64                 `json:"epoch"`
	SchemaVersion    uint64                 `json:"schema_version"`
	SnapshotFrontier uint64                 `json:"snapshot_frontier"`
	SourceFrontier   uint64                 `json:"source_frontier"`
	AppliedFrontier  uint64                 `json:"applied_frontier"`
	Lag              uint64                 `json:"lag"`
	Fetches          uint64                 `json:"fetches"`
	Batches          uint64                 `json:"batches"`
	Deltas           uint64                 `json:"deltas"`
	LastFetchedAt    time.Time              `json:"last_fetched_at,omitempty"`
	LastAppliedAt    time.Time              `json:"last_applied_at,omitempty"`
	LastError        string                 `json:"last_error,omitempty"`
}

// ReplicaHotHandoffToken is the caller-owned cutover proof returned by
// Promote. The caller should publish this target at Frontier and fence the old
// source before serving traffic from the new replica.
type ReplicaHotHandoffToken struct {
	SourceID      string `json:"source_id"`
	TargetID      string `json:"target_id"`
	SnapshotID    string `json:"snapshot_id"`
	Epoch         uint64 `json:"epoch"`
	SchemaVersion uint64 `json:"schema_version"`
	Frontier      uint64 `json:"frontier"`
	Generation    uint64 `json:"generation"`
	FencingToken  uint64 `json:"fencing_token"`
}

// ReplicaHotHandoff coordinates a warmed replacement query replica. It has no
// goroutine of its own: Prepare, CatchUp, and Promote are explicit lifecycle
// calls owned by the embedding service.
type ReplicaHotHandoff struct {
	mu      sync.RWMutex
	options ReplicaHotHandoffOptions
	state   ReplicaHotHandoffState
	ready   bool
	running bool
}

// NewReplicaHotHandoff validates options and creates an idle coordinator.
func NewReplicaHotHandoff(options ReplicaHotHandoffOptions) (*ReplicaHotHandoff, error) {
	sourceID, err := normalizeReplicaHotHandoffIdentifier(options.SourceID, "source ID")
	if err != nil {
		return nil, err
	}
	targetID, err := normalizeReplicaHotHandoffIdentifier(options.TargetID, "target ID")
	if err != nil {
		return nil, err
	}
	if options.BatchSize < 0 || options.BatchSize > MaxReplicaHotHandoffBatchSize {
		return nil, fmt.Errorf("%w: batch size must be between 0 and %d", ErrReplicaHotHandoffInvalid, MaxReplicaHotHandoffBatchSize)
	}
	if options.PollInterval < 0 || options.PollInterval > MaxReplicaHotHandoffPollInterval {
		return nil, fmt.Errorf("%w: poll interval is outside the supported range", ErrReplicaHotHandoffInvalid)
	}
	if options.MaxSnapshotBytes < 0 || options.MaxSnapshotBytes > MaxReplicaHotHandoffSnapshotBytes {
		return nil, fmt.Errorf("%w: max snapshot bytes must be between 0 and %d", ErrReplicaHotHandoffInvalid, MaxReplicaHotHandoffSnapshotBytes)
	}
	if options.FencingToken == 0 {
		return nil, fmt.Errorf("%w: non-zero fencing token is required", ErrReplicaHotHandoffInvalid)
	}
	if options.BatchSize == 0 {
		options.BatchSize = DefaultReplicaHotHandoffBatchSize
	}
	if options.PollInterval == 0 {
		options.PollInterval = DefaultReplicaHotHandoffPollInterval
	}
	if options.MaxSnapshotBytes == 0 {
		options.MaxSnapshotBytes = DefaultReplicaHotHandoffSnapshotBytes
	}
	options.SourceID = sourceID
	options.TargetID = targetID
	return &ReplicaHotHandoff{
		options: options,
		state: ReplicaHotHandoffState{
			SourceID:   sourceID,
			TargetID:   targetID,
			Phase:      ReplicaHotHandoffPhaseIdle,
			Generation: 1,
		},
	}, nil
}

// Prepare captures and atomically installs the source image. It may only be
// called once; failed handoffs remain failed so callers cannot accidentally
// reuse a target whose state is only partially known.
func (handoff *ReplicaHotHandoff) Prepare(ctx context.Context, source ReplicaHotHandoffSource, target ReplicaHotHandoffTarget) error {
	if handoff == nil {
		return ErrReplicaHotHandoffNil
	}
	if ctx == nil || source == nil || target == nil {
		return handoff.fail(ErrReplicaHotHandoffInvalid)
	}
	handoff.mu.Lock()
	if handoff.state.Phase != ReplicaHotHandoffPhaseIdle {
		handoff.mu.Unlock()
		return ErrReplicaHotHandoffPhase
	}
	handoff.state.Phase = ReplicaHotHandoffPhaseInstalling
	handoff.state.Generation++
	handoff.mu.Unlock()

	snapshot, err := source.Snapshot(ctx)
	if err != nil {
		return handoff.fail(err)
	}
	if err := ctx.Err(); err != nil {
		return handoff.fail(err)
	}
	if err := handoff.validateSnapshot(snapshot); err != nil {
		return handoff.fail(err)
	}
	if err := target.InstallSnapshot(ctx, snapshot); err != nil {
		return handoff.fail(err)
	}
	handoff.mu.Lock()
	handoff.state.SnapshotID = snapshot.SnapshotID
	handoff.state.Epoch = snapshot.Epoch
	handoff.state.SchemaVersion = snapshot.SchemaVersion
	handoff.state.SnapshotFrontier = snapshot.Frontier
	handoff.state.SourceFrontier = snapshot.Frontier
	handoff.state.AppliedFrontier = snapshot.Frontier
	handoff.state.Lag = 0
	handoff.state.Phase = ReplicaHotHandoffPhaseCatchingUp
	handoff.state.LastError = ""
	handoff.state.Generation++
	handoff.ready = false
	handoff.mu.Unlock()
	return nil
}

// SyncOnce fetches and applies one bounded update page. When the observed
// source frontier equals the applied frontier it calls target.Ready and moves
// the handoff to Ready. Callers may use this method for their own scheduler or
// use CatchUp for a blocking loop.
func (handoff *ReplicaHotHandoff) SyncOnce(ctx context.Context, source ReplicaHotHandoffSource, target ReplicaHotHandoffTarget) error {
	if handoff == nil {
		return ErrReplicaHotHandoffNil
	}
	if ctx == nil || source == nil || target == nil {
		return handoff.fail(ErrReplicaHotHandoffInvalid)
	}
	handoff.mu.Lock()
	if handoff.running {
		handoff.mu.Unlock()
		return ErrReplicaHotHandoffRunning
	}
	if handoff.state.Phase != ReplicaHotHandoffPhaseCatchingUp && handoff.state.Phase != ReplicaHotHandoffPhaseReady {
		handoff.mu.Unlock()
		return ErrReplicaHotHandoffPhase
	}
	handoff.running = true
	handoff.state.Phase = ReplicaHotHandoffPhaseCatchingUp
	after := handoff.state.AppliedFrontier
	previousSourceFrontier := handoff.state.SourceFrontier
	epoch := handoff.state.Epoch
	batchSize := handoff.options.BatchSize
	handoff.mu.Unlock()
	defer func() {
		handoff.mu.Lock()
		handoff.running = false
		handoff.mu.Unlock()
	}()

	if err := ctx.Err(); err != nil {
		return handoff.fail(err)
	}
	batch, err := source.Fetch(ctx, after, batchSize)
	if err != nil {
		return handoff.fail(err)
	}
	if err := ctx.Err(); err != nil {
		return handoff.fail(err)
	}
	if err := validateReplicaHotHandoffBatch(batch, after, previousSourceFrontier, epoch, batchSize); err != nil {
		return handoff.fail(err)
	}
	if len(batch.Deltas) > 0 {
		if err := target.Apply(ctx, batch); err != nil {
			return handoff.fail(err)
		}
	}
	now := time.Now()
	newApplied := after
	if len(batch.Deltas) > 0 {
		newApplied = batch.Deltas[len(batch.Deltas)-1].Sequence
	}
	handoff.mu.Lock()
	handoff.state.SourceFrontier = batch.SourceFrontier
	handoff.state.AppliedFrontier = newApplied
	handoff.state.Lag = batch.SourceFrontier - newApplied
	handoff.state.Fetches++
	handoff.state.Batches++
	handoff.state.Deltas += uint64(len(batch.Deltas))
	handoff.state.LastFetchedAt = now
	if len(batch.Deltas) > 0 {
		handoff.state.LastAppliedAt = now
	}
	handoff.state.Generation++
	handoff.mu.Unlock()

	if newApplied != batch.SourceFrontier {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return handoff.fail(err)
	}
	if err := target.Ready(ctx); err != nil {
		return handoff.fail(err)
	}
	handoff.mu.Lock()
	handoff.state.Phase = ReplicaHotHandoffPhaseReady
	handoff.state.Lag = 0
	handoff.state.LastError = ""
	handoff.state.Generation++
	handoff.ready = true
	handoff.mu.Unlock()
	return nil
}

// CatchUp repeatedly applies bounded pages until the target confirms it is
// ready, or the context/source/target returns an error. The method blocks in
// the caller's goroutine and starts no hidden worker.
func (handoff *ReplicaHotHandoff) CatchUp(ctx context.Context, source ReplicaHotHandoffSource, target ReplicaHotHandoffTarget) error {
	if handoff == nil {
		return ErrReplicaHotHandoffNil
	}
	if ctx == nil {
		return handoff.fail(ErrReplicaHotHandoffInvalid)
	}
	for {
		if err := ctx.Err(); err != nil {
			return handoff.fail(err)
		}
		if err := handoff.SyncOnce(ctx, source, target); err != nil {
			return err
		}
		if handoff.State().Phase == ReplicaHotHandoffPhaseReady {
			return nil
		}
		timer := time.NewTimer(handoff.pollInterval())
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return handoff.fail(ctx.Err())
		case <-timer.C:
		}
	}
}

// Promote fences the handoff lifecycle and returns the caller-owned cutover
// proof. The caller must still atomically publish the route and fence the old
// source using the returned frontier/token.
func (handoff *ReplicaHotHandoff) Promote(expectedGeneration, fencingToken uint64) (ReplicaHotHandoffToken, error) {
	if handoff == nil {
		return ReplicaHotHandoffToken{}, ErrReplicaHotHandoffNil
	}
	handoff.mu.Lock()
	defer handoff.mu.Unlock()
	if handoff.state.Generation != expectedGeneration {
		return ReplicaHotHandoffToken{}, ErrReplicaHotHandoffGeneration
	}
	if fencingToken != handoff.options.FencingToken {
		return ReplicaHotHandoffToken{}, ErrReplicaHotHandoffFencing
	}
	if !handoff.ready || handoff.state.Phase != ReplicaHotHandoffPhaseReady || handoff.state.Lag != 0 {
		return ReplicaHotHandoffToken{}, ErrReplicaHotHandoffNotReady
	}
	handoff.state.Phase = ReplicaHotHandoffPhasePromoted
	handoff.state.Generation++
	return ReplicaHotHandoffToken{
		SourceID:      handoff.state.SourceID,
		TargetID:      handoff.state.TargetID,
		SnapshotID:    handoff.state.SnapshotID,
		Epoch:         handoff.state.Epoch,
		SchemaVersion: handoff.state.SchemaVersion,
		Frontier:      handoff.state.AppliedFrontier,
		Generation:    handoff.state.Generation,
		FencingToken:  fencingToken,
	}, nil
}

// State returns an immutable point-in-time handoff status.
func (handoff *ReplicaHotHandoff) State() ReplicaHotHandoffState {
	if handoff == nil {
		return ReplicaHotHandoffState{Phase: ReplicaHotHandoffPhaseFailed, LastError: ErrReplicaHotHandoffNil.Error()}
	}
	handoff.mu.RLock()
	defer handoff.mu.RUnlock()
	return handoff.state
}

func (handoff *ReplicaHotHandoff) fail(err error) error {
	if err == nil {
		err = ErrReplicaHotHandoffInvalid
	}
	handoff.mu.Lock()
	if handoff.state.Phase != ReplicaHotHandoffPhasePromoted {
		handoff.state.Phase = ReplicaHotHandoffPhaseFailed
		handoff.state.LastError = err.Error()
		handoff.state.Generation++
		handoff.ready = false
	}
	handoff.mu.Unlock()
	return err
}

func (handoff *ReplicaHotHandoff) pollInterval() time.Duration {
	handoff.mu.RLock()
	interval := handoff.options.PollInterval
	handoff.mu.RUnlock()
	return interval
}

func (handoff *ReplicaHotHandoff) validateSnapshot(snapshot ReplicaHotHandoffSnapshot) error {
	if strings.TrimSpace(snapshot.SourceID) != handoff.options.SourceID || snapshot.SnapshotID == "" || snapshot.Epoch == 0 {
		return fmt.Errorf("%w: snapshot identity or epoch is invalid", ErrReplicaHotHandoffInvalid)
	}
	if handoff.options.ExpectedSchemaVersion != 0 && snapshot.SchemaVersion != handoff.options.ExpectedSchemaVersion {
		return fmt.Errorf("%w: got %d, want %d", ErrReplicaHotHandoffSchemaMismatch, snapshot.SchemaVersion, handoff.options.ExpectedSchemaVersion)
	}
	if len(snapshot.Payload) > handoff.options.MaxSnapshotBytes {
		return fmt.Errorf("%w: got %d bytes, max %d", ErrReplicaHotHandoffSnapshotTooLarge, len(snapshot.Payload), handoff.options.MaxSnapshotBytes)
	}
	return nil
}

func validateReplicaHotHandoffBatch(batch ReplicaHotHandoffBatch, after, previousSourceFrontier, epoch uint64, batchSize int) error {
	if batch.Epoch != epoch {
		return fmt.Errorf("%w: got %d, want %d", ErrReplicaHotHandoffEpochMismatch, batch.Epoch, epoch)
	}
	if batch.SourceFrontier < previousSourceFrontier || batch.SourceFrontier < after {
		return fmt.Errorf("%w: got %d after %d", ErrReplicaHotHandoffSequenceRegression, batch.SourceFrontier, previousSourceFrontier)
	}
	if len(batch.Deltas) > batchSize {
		return fmt.Errorf("%w: got %d deltas, max %d", ErrReplicaHotHandoffInvalid, len(batch.Deltas), batchSize)
	}
	if len(batch.Deltas) == 0 {
		if batch.SourceFrontier != after {
			return fmt.Errorf("%w: empty batch hides frontier %d after %d", ErrReplicaHotHandoffSequenceGap, batch.SourceFrontier, after)
		}
		return nil
	}
	expected := after + 1
	for _, delta := range batch.Deltas {
		if delta.Sequence != expected {
			if delta.Sequence <= after {
				return fmt.Errorf("%w: got %d after %d", ErrReplicaHotHandoffSequenceRegression, delta.Sequence, after)
			}
			return fmt.Errorf("%w: got %d, want %d", ErrReplicaHotHandoffSequenceGap, delta.Sequence, expected)
		}
		if expected == ^uint64(0) {
			break
		}
		expected++
	}
	if batch.Deltas[len(batch.Deltas)-1].Sequence > batch.SourceFrontier {
		return fmt.Errorf("%w: last delta %d exceeds frontier %d", ErrReplicaHotHandoffSequenceRegression, batch.Deltas[len(batch.Deltas)-1].Sequence, batch.SourceFrontier)
	}
	return nil
}

func normalizeReplicaHotHandoffIdentifier(value, label string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > MaxReplicaHotHandoffIdentifierBytes {
		return "", fmt.Errorf("%w: %s is empty or too long", ErrReplicaHotHandoffInvalid, label)
	}
	return value, nil
}
