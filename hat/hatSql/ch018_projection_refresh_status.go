package hatSql

import "time"

// ProjectionRefreshState describes the last known state of an incremental
// projection runner.
type ProjectionRefreshState string

const (
	// ProjectionRefreshStateDisabled means the runner is configured off.
	ProjectionRefreshStateDisabled ProjectionRefreshState = "disabled"
	// ProjectionRefreshStateIdle means an enabled runner has not processed a
	// change batch yet.
	ProjectionRefreshStateIdle ProjectionRefreshState = "idle"
	// ProjectionRefreshStateHealthy means the applied and observed frontiers
	// are equal and the last refresh succeeded.
	ProjectionRefreshStateHealthy ProjectionRefreshState = "healthy"
	// ProjectionRefreshStateLagging means the last refresh succeeded but input
	// has been observed beyond the applied frontier.
	ProjectionRefreshStateLagging ProjectionRefreshState = "lagging"
	// ProjectionRefreshStateFailed means the last attempted refresh failed.
	ProjectionRefreshStateFailed ProjectionRefreshState = "failed"
)

const maxProjectionRefreshErrorBytes = 1024

// ProjectionRefreshStatus is a bounded, immutable-on-read status snapshot for
// one incremental projection runner. ObservedSequence is the highest source
// sequence supplied to Apply or Rebuild; AppliedSequence is the last sequence
// whose refresh and optional checkpoint save succeeded.
type ProjectionRefreshStatus struct {
	Name                string                 `json:"name"`
	Enabled             bool                   `json:"enabled"`
	State               ProjectionRefreshState `json:"state"`
	AppliedSequence     uint64                 `json:"applied_sequence"`
	ObservedSequence    uint64                 `json:"observed_sequence"`
	Lag                 uint64                 `json:"lag"`
	LastAttemptAt       time.Time              `json:"last_attempt_at,omitempty"`
	LastSuccessAt       time.Time              `json:"last_success_at,omitempty"`
	LastError           string                 `json:"last_error,omitempty"`
	ConsecutiveFailures uint64                 `json:"consecutive_failures"`
}

// Status returns an independent snapshot of the runner's current refresh
// state. The status is process-local and is intentionally not part of the
// projection checkpoint contract.
func (runner *IncrementalProjectionRunner) Status() ProjectionRefreshStatus {
	if runner == nil {
		return ProjectionRefreshStatus{}
	}
	runner.mu.Lock()
	status := runner.status
	runner.mu.Unlock()
	return status
}

func newProjectionRefreshStatus(name string, enabled bool, checkpoint uint64) ProjectionRefreshStatus {
	state := ProjectionRefreshStateDisabled
	if enabled {
		state = ProjectionRefreshStateIdle
	}
	return ProjectionRefreshStatus{
		Name:             name,
		Enabled:          enabled,
		State:            state,
		AppliedSequence:  checkpoint,
		ObservedSequence: checkpoint,
	}
}

func (runner *IncrementalProjectionRunner) ensureProjectionRefreshStatusLocked() {
	if runner.status.Name == "" {
		runner.status = newProjectionRefreshStatus(runner.config.Name, runner.config.Enabled, runner.checkpoint)
	}
}

func (runner *IncrementalProjectionRunner) observeProjectionSequenceLocked(sequence uint64) {
	runner.ensureProjectionRefreshStatusLocked()
	if sequence > runner.status.ObservedSequence {
		runner.status.ObservedSequence = sequence
	}
	runner.status.Lag = projectionRefreshLag(runner.status.ObservedSequence, runner.status.AppliedSequence)
	if runner.status.State != ProjectionRefreshStateFailed && runner.status.State != ProjectionRefreshStateDisabled && runner.status.Lag > 0 {
		runner.status.State = ProjectionRefreshStateLagging
	}
}

func (runner *IncrementalProjectionRunner) markProjectionRefreshFailureLocked(observed uint64, err error) {
	runner.observeProjectionSequenceLocked(observed)
	now := time.Now().UTC()
	runner.status.LastAttemptAt = now
	runner.status.LastError = boundedProjectionRefreshError(err)
	if runner.status.ConsecutiveFailures < ^uint64(0) {
		runner.status.ConsecutiveFailures++
	}
	runner.status.State = ProjectionRefreshStateFailed
}

func (runner *IncrementalProjectionRunner) markProjectionRefreshSuccessLocked(applied uint64) {
	runner.ensureProjectionRefreshStatusLocked()
	if applied > runner.status.ObservedSequence {
		runner.status.ObservedSequence = applied
	}
	runner.status.AppliedSequence = applied
	runner.status.Lag = projectionRefreshLag(runner.status.ObservedSequence, applied)
	now := time.Now().UTC()
	runner.status.LastAttemptAt = now
	runner.status.LastSuccessAt = now
	runner.status.LastError = ""
	runner.status.ConsecutiveFailures = 0
	runner.status.State = ProjectionRefreshStateHealthy
	if runner.status.Lag > 0 {
		runner.status.State = ProjectionRefreshStateLagging
	}
}

func projectionRefreshLag(observed, applied uint64) uint64 {
	if observed <= applied {
		return 0
	}
	return observed - applied
}

func boundedProjectionRefreshError(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if len(message) <= maxProjectionRefreshErrorBytes {
		return message
	}
	return message[:maxProjectionRefreshErrorBytes]
}
