package hatReplication

import (
	"errors"
	"sync"
)

const DefaultReplicaBackpressureMaxLag uint64 = 1024

var (
	ErrReplicaBackpressureNil                = errors.New("hatriecache: replica backpressure controller is nil")
	ErrReplicaBackpressureInvalidOptions     = errors.New("hatriecache: replica backpressure options are invalid")
	ErrReplicaBackpressureSequenceRegression = errors.New("hatriecache: replica backpressure observation regressed")
)

// ReplicaBackpressureOptions configures an opt-in lag gate. MaxLag pauses
// admission when source progress reaches the threshold. ResumeLag clears the
// pause after the replica catches up; zero selects half of MaxLag.
type ReplicaBackpressureOptions struct {
	MaxLag    uint64
	ResumeLag uint64
}

// ReplicaBackpressureState is a point-in-time admission decision. Changed is
// true only on the observation that entered or left the paused state.
type ReplicaBackpressureState struct {
	SourceLSN  uint64
	AppliedLSN uint64
	LagLSN     uint64
	MaxLag     uint64
	ResumeLag  uint64
	Paused     bool
	Changed    bool
}

// ReplicaBackpressureController prevents a relay or applier from admitting
// more work while a replica is too far behind. Observations must be monotone;
// rejecting regressions prevents stale health samples from releasing a pause.
type ReplicaBackpressureController struct {
	mu         sync.Mutex
	maxLag     uint64
	resumeLag  uint64
	sourceLSN  uint64
	appliedLSN uint64
	observed   bool
	paused     bool
}

// NewReplicaBackpressureController creates a lag gate. It does not install
// itself into a replication transport; callers explicitly use Observe or
// Admit at their relay/applier boundary.
func NewReplicaBackpressureController(options ReplicaBackpressureOptions) (*ReplicaBackpressureController, error) {
	maxLag := options.MaxLag
	if maxLag == 0 {
		maxLag = DefaultReplicaBackpressureMaxLag
	}
	resumeLag := options.ResumeLag
	if resumeLag == 0 {
		resumeLag = maxLag / 2
	}
	if resumeLag >= maxLag {
		return nil, ErrReplicaBackpressureInvalidOptions
	}
	return &ReplicaBackpressureController{maxLag: maxLag, resumeLag: resumeLag}, nil
}

// Observe records monotone source and applied positions and returns the new
// admission state. Lag saturates at zero when applied progress is ahead.
func (controller *ReplicaBackpressureController) Observe(sourceLSN, appliedLSN uint64) (ReplicaBackpressureState, error) {
	if controller == nil {
		return ReplicaBackpressureState{}, ErrReplicaBackpressureNil
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.observed && (sourceLSN < controller.sourceLSN || appliedLSN < controller.appliedLSN) {
		return controller.snapshotLocked(false), ErrReplicaBackpressureSequenceRegression
	}
	lag := uint64(0)
	if sourceLSN > appliedLSN {
		lag = sourceLSN - appliedLSN
	}
	previousPaused := controller.paused
	if controller.paused {
		if lag <= controller.resumeLag {
			controller.paused = false
		}
	} else if lag >= controller.maxLag {
		controller.paused = true
	}
	controller.sourceLSN = sourceLSN
	controller.appliedLSN = appliedLSN
	controller.observed = true
	return controller.snapshotWithLagLocked(lag, controller.paused != previousPaused), nil
}

// Admit observes progress and reports whether the relay/applier may continue.
func (controller *ReplicaBackpressureController) Admit(sourceLSN, appliedLSN uint64) (bool, error) {
	state, err := controller.Observe(sourceLSN, appliedLSN)
	if err != nil {
		return false, err
	}
	return !state.Paused, nil
}

// Snapshot returns the current decision without changing hysteresis state.
func (controller *ReplicaBackpressureController) Snapshot() ReplicaBackpressureState {
	if controller == nil {
		return ReplicaBackpressureState{}
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.snapshotLocked(false)
}

func (controller *ReplicaBackpressureController) snapshotLocked(changed bool) ReplicaBackpressureState {
	lag := uint64(0)
	if controller.sourceLSN > controller.appliedLSN {
		lag = controller.sourceLSN - controller.appliedLSN
	}
	return controller.snapshotWithLagLocked(lag, changed)
}

func (controller *ReplicaBackpressureController) snapshotWithLagLocked(lag uint64, changed bool) ReplicaBackpressureState {
	return ReplicaBackpressureState{
		SourceLSN:  controller.sourceLSN,
		AppliedLSN: controller.appliedLSN,
		LagLSN:     lag,
		MaxLag:     controller.maxLag,
		ResumeLag:  controller.resumeLag,
		Paused:     controller.paused,
		Changed:    changed,
	}
}
