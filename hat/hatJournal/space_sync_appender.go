package hatJournal

import (
	"errors"
	"io"
	"sync"
	"time"
)

const (
	// DefaultSpaceSyncAppenderPeriodicInterval bounds the default delay before
	// a periodic append is flushed when the byte threshold is not reached.
	DefaultSpaceSyncAppenderPeriodicInterval = 2 * time.Millisecond
	// DefaultSpaceSyncAppenderPeriodicMaxBytes keeps small periodic batches
	// bounded without forcing a sync for every append.
	DefaultSpaceSyncAppenderPeriodicMaxBytes = 64 << 10
	MaxSpaceSyncAppenderPeriodicMaxBytes     = 1 << 30
)

var (
	ErrSpaceSyncAppenderNil           = errors.New("hatJournal: space sync appender is nil")
	ErrSpaceSyncAppenderWriterNil     = errors.New("hatJournal: space sync appender writer is nil")
	ErrSpaceSyncAppenderSyncNil       = errors.New("hatJournal: space sync appender sync function is nil")
	ErrSpaceSyncAppenderConfiguration = errors.New("hatJournal: space sync appender configuration is invalid")
)

// SpaceSyncAppenderOptions configures a serialized append boundary. The
// registry and sync function are caller-owned; the appender only coordinates
// their use around one writer.
type SpaceSyncAppenderOptions struct {
	Registry         *SpaceSyncPolicyRegistry
	PeriodicInterval time.Duration
	PeriodicMaxBytes int
	Now              func() time.Time
}

// SpaceSyncAppendResult describes the policy selected for one append and
// whether the bytes were followed by a successful sync.
type SpaceSyncAppendResult struct {
	Policy       SpaceSyncPolicy
	BytesWritten int
	Synced       bool
}

// SpaceSyncAppender applies a named-space sync policy at a concrete writer
// boundary. It is opt-in and serialized so a sync cannot overtake a write.
// Existing journal writers keep their behavior until they choose this helper.
type SpaceSyncAppender struct {
	mu               sync.Mutex
	writer           io.Writer
	syncFunc         func() error
	registry         *SpaceSyncPolicyRegistry
	periodicInterval time.Duration
	periodicMaxBytes int64
	now              func() time.Time
	lastSync         time.Time
	pendingSyncBytes int64
}

// NewSpaceSyncAppender creates a bounded policy-aware writer boundary. A nil
// registry uses the conservative periodic default registry. A nil clock uses
// time.Now. The sync function is required even when the current registry has
// only disabled policies because policies may be changed after construction.
func NewSpaceSyncAppender(writer io.Writer, syncFunc func() error, options SpaceSyncAppenderOptions) (*SpaceSyncAppender, error) {
	if writer == nil {
		return nil, ErrSpaceSyncAppenderWriterNil
	}
	if syncFunc == nil {
		return nil, ErrSpaceSyncAppenderSyncNil
	}
	interval := options.PeriodicInterval
	if interval == 0 {
		interval = DefaultSpaceSyncAppenderPeriodicInterval
	}
	if interval < 0 {
		return nil, ErrSpaceSyncAppenderConfiguration
	}
	maxBytes := options.PeriodicMaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultSpaceSyncAppenderPeriodicMaxBytes
	}
	if maxBytes < 1 || maxBytes > MaxSpaceSyncAppenderPeriodicMaxBytes {
		return nil, ErrSpaceSyncAppenderConfiguration
	}
	registry := options.Registry
	if registry == nil {
		var err error
		registry, err = NewSpaceSyncPolicyRegistry(SpaceSyncPolicyOptions{})
		if err != nil {
			return nil, err
		}
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &SpaceSyncAppender{
		writer:           writer,
		syncFunc:         syncFunc,
		registry:         registry,
		periodicInterval: interval,
		periodicMaxBytes: int64(maxBytes),
		now:              now,
		lastSync:         now(),
	}, nil
}

// Append writes one logical space payload and applies its current policy.
// Immediate policy syncs every successful append. Periodic policy syncs when
// its bounded byte or time threshold is reached. Disabled policy performs no
// required sync. A failed sync leaves pending bytes available to Flush.
func (appender *SpaceSyncAppender) Append(space string, payload []byte) (SpaceSyncAppendResult, error) {
	if appender == nil {
		return SpaceSyncAppendResult{}, ErrSpaceSyncAppenderNil
	}
	policy := appender.registry.Resolve(space)
	result := SpaceSyncAppendResult{Policy: policy}
	appender.mu.Lock()
	defer appender.mu.Unlock()

	written, err := appender.writer.Write(payload)
	result.BytesWritten = written
	if err != nil {
		return result, err
	}
	if written != len(payload) {
		return result, io.ErrShortWrite
	}
	switch policy {
	case SpaceSyncPolicyImmediate:
		appender.pendingSyncBytes += int64(written)
		if err := appender.syncLocked(); err != nil {
			return result, err
		}
		result.Synced = true
	case SpaceSyncPolicyPeriodic:
		appender.pendingSyncBytes += int64(written)
		if appender.periodicDueLocked() {
			if err := appender.syncLocked(); err != nil {
				return result, err
			}
			result.Synced = true
		}
	case SpaceSyncPolicyDisabled:
		// Disabled writes are deliberately not added to the pending durable
		// byte count; Flush only retries writes that requested durability.
	default:
		return result, ErrSpaceSyncPolicyInvalid
	}
	return result, nil
}

// Flush syncs all pending periodic or failed-immediate appends. It returns
// false when no policy-required bytes are pending.
func (appender *SpaceSyncAppender) Flush() (bool, error) {
	if appender == nil {
		return false, ErrSpaceSyncAppenderNil
	}
	appender.mu.Lock()
	defer appender.mu.Unlock()
	if appender.pendingSyncBytes == 0 {
		return false, nil
	}
	if err := appender.syncLocked(); err != nil {
		return false, err
	}
	return true, nil
}

func (appender *SpaceSyncAppender) periodicDueLocked() bool {
	if appender.pendingSyncBytes >= appender.periodicMaxBytes {
		return true
	}
	return !appender.now().Before(appender.lastSync.Add(appender.periodicInterval))
}

func (appender *SpaceSyncAppender) syncLocked() error {
	if err := appender.syncFunc(); err != nil {
		return err
	}
	appender.pendingSyncBytes = 0
	appender.lastSync = appender.now()
	return nil
}
