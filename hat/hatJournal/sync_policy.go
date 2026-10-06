package hatJournal

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	// SyncModeImmediate fsyncs each completed journal collection. It is the
	// safe default and preserves the existing durability boundary.
	SyncModeImmediate SyncMode = "immediate"
	// SyncModePeriodic fsyncs the first collection and then at most once per
	// configured interval. Writes between syncs remain recoverable only if the
	// operating system has flushed them before a crash.
	SyncModePeriodic SyncMode = "periodic"
	// SyncModeDisabled never calls fsync for journal collections. Use only when
	// the caller explicitly accepts process/host crash loss.
	SyncModeDisabled SyncMode = "disabled"

	// DefaultSyncInterval is used when periodic mode is selected without an
	// explicit interval.
	DefaultSyncInterval = time.Second
)

var (
	// ErrInvalidSyncMode indicates an unsupported journal sync mode.
	ErrInvalidSyncMode = errors.New("hatJournal: invalid sync mode")
	// ErrInvalidSyncInterval indicates a negative journal sync interval.
	ErrInvalidSyncInterval = errors.New("hatJournal: invalid sync interval")
)

// SyncMode controls when a journal collection requests an operating-system
// sync. The zero value is normalized to SyncModeImmediate.
type SyncMode string

// ParseSyncMode accepts stable configuration spellings and returns the
// canonical sync mode.
func ParseSyncMode(value string) (SyncMode, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "sync", "immediate", "always":
		return SyncModeImmediate, nil
	case "periodic", "interval":
		return SyncModePeriodic, nil
	case "disabled", "disable", "none", "off":
		return SyncModeDisabled, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrInvalidSyncMode, value)
	}
}

func normalizeSyncPolicy(mode SyncMode, interval time.Duration) (SyncMode, time.Duration, error) {
	parsed, err := ParseSyncMode(string(mode))
	if err != nil {
		return "", 0, err
	}
	if interval < 0 {
		return "", 0, fmt.Errorf("%w: %s", ErrInvalidSyncInterval, interval)
	}
	if parsed != SyncModePeriodic {
		return parsed, 0, nil
	}
	if interval == 0 {
		interval = DefaultSyncInterval
	}
	return parsed, interval, nil
}

// SyncPolicy is the small state machine used by journal appenders. It is
// intentionally not time-based in a background goroutine: the owning journal
// decides when to evaluate it at a collection boundary.
type SyncPolicy struct {
	mode     SyncMode
	interval time.Duration
	lastSync time.Time
}

// NewSyncPolicy creates a normalized policy for an append stream.
func NewSyncPolicy(mode SyncMode, interval time.Duration) (*SyncPolicy, error) {
	mode, interval, err := normalizeSyncPolicy(mode, interval)
	if err != nil {
		return nil, err
	}
	return &SyncPolicy{mode: mode, interval: interval}, nil
}

// ShouldSync reports whether the current collection should call fsync.
func (policy *SyncPolicy) ShouldSync(now time.Time) bool {
	if policy == nil {
		return true
	}
	switch policy.mode {
	case SyncModeDisabled:
		return false
	case SyncModePeriodic:
		return policy.lastSync.IsZero() || !now.Before(policy.lastSync.Add(policy.interval))
	default:
		return true
	}
}

// MarkSynced records a successful operating-system sync.
func (policy *SyncPolicy) MarkSynced(now time.Time) {
	if policy == nil || policy.mode == SyncModeDisabled {
		return
	}
	policy.lastSync = now
}

// Mode returns the canonical policy mode. A nil policy reports the safe
// immediate mode.
func (policy *SyncPolicy) Mode() SyncMode {
	if policy == nil {
		return SyncModeImmediate
	}
	return policy.mode
}

// Interval returns the normalized periodic interval, or zero for other modes.
func (policy *SyncPolicy) Interval() time.Duration {
	if policy == nil {
		return 0
	}
	return policy.interval
}
