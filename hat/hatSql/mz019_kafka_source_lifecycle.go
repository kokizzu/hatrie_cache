package hatSql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const MaxKafkaTableSourcePauseReasonBytes = 256

const (
	KafkaTableSourceLifecycleRunning = "running"
	KafkaTableSourceLifecyclePaused  = "paused"
)

var (
	ErrKafkaTableSourcePaused           = errors.New("Kafka table source is paused")
	ErrKafkaTableSourceLifecycleInvalid = errors.New("Kafka table source lifecycle is invalid")
)

// KafkaTableSourceLifecycle is the operator-controlled connector state that
// travels with KafkaTableSourceSnapshot and its durable checkpoint.
type KafkaTableSourceLifecycle struct {
	State      string `json:"state"`
	Generation uint64 `json:"generation"`
	Reason     string `json:"reason,omitempty"`
}

// Lifecycle returns a detached lifecycle state. A zero state is interpreted
// as running for snapshots written before lifecycle persistence existed.
func (source *KafkaTableSource) Lifecycle() KafkaTableSourceLifecycle {
	if source == nil {
		return KafkaTableSourceLifecycle{}
	}
	source.mu.RLock()
	defer source.mu.RUnlock()
	lifecycle := source.lifecycle
	if lifecycle.State == "" {
		lifecycle.State = KafkaTableSourceLifecycleRunning
	}
	return lifecycle
}

// Pause stops direct and polled ingestion. Repeated pauses are idempotent and
// preserve the first reason and generation.
func (source *KafkaTableSource) Pause(reason string) (KafkaTableSourceLifecycle, error) {
	if source == nil {
		return KafkaTableSourceLifecycle{}, ErrKafkaTableSourceNil
	}
	reason, err := normalizeKafkaTableSourcePauseReason(reason)
	if err != nil {
		return KafkaTableSourceLifecycle{}, err
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.lifecycle.State == KafkaTableSourceLifecyclePaused {
		return source.lifecycle, nil
	}
	if source.lifecycle.Generation == ^uint64(0) {
		return KafkaTableSourceLifecycle{}, ErrKafkaTableSourceLifecycleInvalid
	}
	source.lifecycle.State = KafkaTableSourceLifecyclePaused
	source.lifecycle.Generation++
	source.lifecycle.Reason = reason
	return source.lifecycle, nil
}

// Resume allows ingestion again and advances the lifecycle generation.
func (source *KafkaTableSource) Resume() (KafkaTableSourceLifecycle, error) {
	if source == nil {
		return KafkaTableSourceLifecycle{}, ErrKafkaTableSourceNil
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.lifecycle.State != KafkaTableSourceLifecyclePaused {
		if source.lifecycle.State == "" {
			source.lifecycle.State = KafkaTableSourceLifecycleRunning
		}
		return source.lifecycle, nil
	}
	if source.lifecycle.Generation == ^uint64(0) {
		return KafkaTableSourceLifecycle{}, ErrKafkaTableSourceLifecycleInvalid
	}
	source.lifecycle.State = KafkaTableSourceLifecycleRunning
	source.lifecycle.Generation++
	source.lifecycle.Reason = ""
	return source.lifecycle, nil
}

// PauseWithCheckpoint changes lifecycle state and persists it atomically with
// the current rows, offsets, and replay markers.
func (source *KafkaTableSource) PauseWithCheckpoint(ctx context.Context, store KafkaTableSourceCheckpointStore, reason string) (KafkaTableSourceLifecycle, error) {
	reason, err := normalizeKafkaTableSourcePauseReason(reason)
	if err != nil {
		return KafkaTableSourceLifecycle{}, err
	}
	return source.updateKafkaTableSourceLifecycleWithCheckpoint(ctx, store, true, reason)
}

// ResumeWithCheckpoint changes lifecycle state and persists it atomically with
// the current rows, offsets, and replay markers.
func (source *KafkaTableSource) ResumeWithCheckpoint(ctx context.Context, store KafkaTableSourceCheckpointStore) (KafkaTableSourceLifecycle, error) {
	return source.updateKafkaTableSourceLifecycleWithCheckpoint(ctx, store, false, "")
}

func (source *KafkaTableSource) updateKafkaTableSourceLifecycleWithCheckpoint(ctx context.Context, store KafkaTableSourceCheckpointStore, pause bool, reason string) (KafkaTableSourceLifecycle, error) {
	if source == nil {
		return KafkaTableSourceLifecycle{}, ErrKafkaTableSourceNil
	}
	if store == nil {
		return KafkaTableSourceLifecycle{}, ErrKafkaTableSourceCheckpointStoreRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return KafkaTableSourceLifecycle{}, err
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	previous := source.snapshotLocked()
	var lifecycle KafkaTableSourceLifecycle
	var err error
	if pause {
		if source.lifecycle.State == KafkaTableSourceLifecyclePaused {
			lifecycle = source.lifecycle
		} else {
			if source.lifecycle.Generation == ^uint64(0) {
				return KafkaTableSourceLifecycle{}, ErrKafkaTableSourceLifecycleInvalid
			}
			source.lifecycle.State = KafkaTableSourceLifecyclePaused
			source.lifecycle.Generation++
			source.lifecycle.Reason = reason
			lifecycle = source.lifecycle
		}
	} else {
		if source.lifecycle.State == KafkaTableSourceLifecyclePaused {
			if source.lifecycle.Generation == ^uint64(0) {
				return KafkaTableSourceLifecycle{}, ErrKafkaTableSourceLifecycleInvalid
			}
			source.lifecycle.State = KafkaTableSourceLifecycleRunning
			source.lifecycle.Generation++
			source.lifecycle.Reason = ""
		}
		if source.lifecycle.State == "" {
			source.lifecycle.State = KafkaTableSourceLifecycleRunning
		}
		lifecycle = source.lifecycle
	}
	if err = store.Commit(ctx, source.snapshotLocked()); err != nil {
		rollbackErr := source.restoreSnapshotLocked(previous)
		if rollbackErr != nil {
			return KafkaTableSourceLifecycle{}, fmt.Errorf("%w: %v; rollback: %v", ErrKafkaTableSourceCheckpointCommit, err, rollbackErr)
		}
		return KafkaTableSourceLifecycle{}, fmt.Errorf("%w: %v", ErrKafkaTableSourceCheckpointCommit, err)
	}
	return lifecycle, nil
}

func (source *KafkaTableSource) isPaused() bool {
	return source.Lifecycle().State == KafkaTableSourceLifecyclePaused
}

func normalizeKafkaTableSourceLifecycle(lifecycle KafkaTableSourceLifecycle) (KafkaTableSourceLifecycle, error) {
	if lifecycle.State == "" {
		lifecycle.State = KafkaTableSourceLifecycleRunning
	}
	if lifecycle.State != KafkaTableSourceLifecycleRunning && lifecycle.State != KafkaTableSourceLifecyclePaused {
		return KafkaTableSourceLifecycle{}, fmt.Errorf("%w: unsupported state", ErrKafkaTableSourceLifecycleInvalid)
	}
	if !utf8.ValidString(lifecycle.Reason) || len(lifecycle.Reason) > MaxKafkaTableSourcePauseReasonBytes {
		return KafkaTableSourceLifecycle{}, fmt.Errorf("%w: reason is too large or invalid", ErrKafkaTableSourceLifecycleInvalid)
	}
	for _, character := range lifecycle.Reason {
		if unicode.IsControl(character) || unicode.Is(unicode.Cf, character) {
			return KafkaTableSourceLifecycle{}, fmt.Errorf("%w: reason contains control characters", ErrKafkaTableSourceLifecycleInvalid)
		}
	}
	lifecycle.Reason = strings.TrimSpace(lifecycle.Reason)
	if lifecycle.State == KafkaTableSourceLifecycleRunning && lifecycle.Reason != "" {
		return KafkaTableSourceLifecycle{}, fmt.Errorf("%w: running state cannot have a reason", ErrKafkaTableSourceLifecycleInvalid)
	}
	if lifecycle.State == KafkaTableSourceLifecyclePaused && lifecycle.Generation == 0 {
		return KafkaTableSourceLifecycle{}, fmt.Errorf("%w: paused state requires a generation", ErrKafkaTableSourceLifecycleInvalid)
	}
	return lifecycle, nil
}

func normalizeKafkaTableSourcePauseReason(reason string) (string, error) {
	if !utf8.ValidString(reason) || len(reason) > MaxKafkaTableSourcePauseReasonBytes {
		return "", fmt.Errorf("%w: reason is too large or invalid", ErrKafkaTableSourceLifecycleInvalid)
	}
	for _, character := range reason {
		if unicode.IsControl(character) || unicode.Is(unicode.Cf, character) {
			return "", fmt.Errorf("%w: reason contains control characters", ErrKafkaTableSourceLifecycleInvalid)
		}
	}
	return strings.TrimSpace(reason), nil
}
