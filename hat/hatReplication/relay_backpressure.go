package hatReplication

import "sync"

const (
	// DefaultRelayBackpressureHighWatermark is the default maximum lag, in
	// journal entries, before a relay pauses new asynchronous work.
	DefaultRelayBackpressureHighWatermark uint64 = 10_000
	// DefaultRelayBackpressureResumeWatermark is the default lag at which a
	// paused relay resumes. Keeping it below the high watermark prevents
	// pause/resume thrashing around one boundary.
	DefaultRelayBackpressureResumeWatermark uint64 = 5_000
)

// RelayBackpressureOptions configures lag-based admission for a replication
// relay. It is disabled unless Enabled is true. Zero watermarks select sane
// defaults; a resume watermark at or above the high watermark is clamped below
// it.
type RelayBackpressureOptions struct {
	Enabled         bool
	HighWatermark   uint64
	ResumeWatermark uint64
}

// RelayBackpressureDecision is the result of one lag observation.
type RelayBackpressureDecision struct {
	Allowed      bool
	Paused       bool
	Transitioned bool
	Lag          uint64
}

// RelayBackpressureSnapshot is a read-only controller state snapshot.
type RelayBackpressureSnapshot struct {
	Enabled         bool
	Paused          bool
	Lag             uint64
	HighWatermark   uint64
	ResumeWatermark uint64
	Transitions     uint64
}

// RelayBackpressure is a small hysteresis state machine for a relay whose
// downstream applier reports journal progress. It owns no queue and never
// drops data; callers decide how to retain or reject work after Admit returns
// false.
type RelayBackpressure struct {
	mu              sync.Mutex
	enabled         bool
	highWatermark   uint64
	resumeWatermark uint64
	paused          bool
	lag             uint64
	transitions     uint64
}

// NewRelayBackpressure creates a lag admission controller. Disabled
// controllers are intentionally valid no-ops so embedding code can keep
// configuration construction simple.
func NewRelayBackpressure(options RelayBackpressureOptions) *RelayBackpressure {
	highWatermark := options.HighWatermark
	if highWatermark == 0 {
		highWatermark = DefaultRelayBackpressureHighWatermark
	}
	resumeWatermark := options.ResumeWatermark
	if resumeWatermark == 0 {
		resumeWatermark = DefaultRelayBackpressureResumeWatermark
		if resumeWatermark >= highWatermark {
			resumeWatermark = highWatermark / 2
		}
	}
	if resumeWatermark >= highWatermark {
		resumeWatermark = highWatermark - 1
	}
	return &RelayBackpressure{
		enabled:         options.Enabled,
		highWatermark:   highWatermark,
		resumeWatermark: resumeWatermark,
	}
}

// Admit observes current downstream lag and returns whether the relay may
// accept more work. Crossing the high watermark pauses admission; a paused
// relay resumes only at or below the lower watermark.
func (backpressure *RelayBackpressure) Admit(lag uint64) RelayBackpressureDecision {
	if backpressure == nil {
		return RelayBackpressureDecision{Allowed: true, Lag: lag}
	}
	backpressure.mu.Lock()
	defer backpressure.mu.Unlock()
	backpressure.lag = lag
	if !backpressure.enabled {
		return RelayBackpressureDecision{Allowed: true, Lag: lag}
	}
	transitioned := false
	if backpressure.paused {
		if lag <= backpressure.resumeWatermark {
			backpressure.paused = false
			backpressure.transitions++
			transitioned = true
		}
	} else if lag >= backpressure.highWatermark {
		backpressure.paused = true
		backpressure.transitions++
		transitioned = true
	}
	return RelayBackpressureDecision{
		Allowed:      !backpressure.paused,
		Paused:       backpressure.paused,
		Transitioned: transitioned,
		Lag:          lag,
	}
}

// Snapshot returns the current controller state.
func (backpressure *RelayBackpressure) Snapshot() RelayBackpressureSnapshot {
	if backpressure == nil {
		return RelayBackpressureSnapshot{}
	}
	backpressure.mu.Lock()
	defer backpressure.mu.Unlock()
	return RelayBackpressureSnapshot{
		Enabled:         backpressure.enabled,
		Paused:          backpressure.paused,
		Lag:             backpressure.lag,
		HighWatermark:   backpressure.highWatermark,
		ResumeWatermark: backpressure.resumeWatermark,
		Transitions:     backpressure.transitions,
	}
}
