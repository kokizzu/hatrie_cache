package hatPipeline

import (
	"context"
	"errors"
	"sort"
	"time"
)

const (
	defaultConnectorHealthMaxAttempts    = 3
	defaultConnectorHealthInitialBackoff = 100 * time.Millisecond
	defaultConnectorHealthMaxBackoff     = 5 * time.Second
	maxConnectorHealthAttempts           = 128
	maxConnectorHealthBackoff            = time.Hour
	maxConnectorHealthErrorBytes         = 4096
)

var (
	// ErrConnectorHealthPolicyInvalid means that a remediation policy is not
	// bounded or contains an invalid backoff range.
	ErrConnectorHealthPolicyInvalid = errors.New("connector health policy is invalid")
	// ErrConnectorQuarantined means that remediation is blocked until an
	// operator explicitly releases the connector quarantine.
	ErrConnectorQuarantined = errors.New("connector is quarantined")
	// ErrConnectorNotQuarantined means that no active quarantine exists.
	ErrConnectorNotQuarantined = errors.New("connector is not quarantined")
)

// ConnectorHealthState is the operator-visible state maintained by the
// opt-in remediation API. It is separate from ConnectorState so ordinary
// lifecycle transitions remain backward compatible.
type ConnectorHealthState uint8

const (
	ConnectorUnknown ConnectorHealthState = iota
	ConnectorHealthy
	ConnectorRetrying
	ConnectorHealthFailed
	ConnectorQuarantined
)

func (s ConnectorHealthState) String() string {
	switch s {
	case ConnectorUnknown:
		return "unknown"
	case ConnectorHealthy:
		return "healthy"
	case ConnectorRetrying:
		return "retrying"
	case ConnectorHealthFailed:
		return "failed"
	case ConnectorQuarantined:
		return "quarantined"
	default:
		return "unknown"
	}
}

// ConnectorHealthStatus is a bounded snapshot of remediation state. The
// lifecycle state remains available through Status; this snapshot only
// describes the explicit health policy control plane.
type ConnectorHealthStatus struct {
	ID          string
	State       ConnectorHealthState
	Attempts    int
	LastError   string
	NextRetryAt time.Time
	UpdatedAt   time.Time
}

// ConnectorHealthPolicy controls one synchronous remediation run. Zero
// numeric values select the documented numeric defaults; bool fields are
// opt-in so a zero policy never quarantines a connector unexpectedly.
type ConnectorHealthPolicy struct {
	MaxAttempts         int
	InitialBackoff      time.Duration
	MaxBackoff          time.Duration
	QuarantineOnFailure bool
	Retryable           func(error) bool
}

// DefaultConnectorHealthPolicy returns bounded retry defaults. Callers must
// still explicitly invoke StartWithHealthPolicy to enable remediation.
func DefaultConnectorHealthPolicy() ConnectorHealthPolicy {
	return ConnectorHealthPolicy{
		MaxAttempts:         defaultConnectorHealthMaxAttempts,
		InitialBackoff:      defaultConnectorHealthInitialBackoff,
		MaxBackoff:          defaultConnectorHealthMaxBackoff,
		QuarantineOnFailure: true,
	}
}

func (policy ConnectorHealthPolicy) normalized() (ConnectorHealthPolicy, error) {
	if policy.MaxAttempts == 0 {
		policy.MaxAttempts = defaultConnectorHealthMaxAttempts
	}
	if policy.InitialBackoff == 0 {
		policy.InitialBackoff = defaultConnectorHealthInitialBackoff
	}
	if policy.MaxBackoff == 0 {
		policy.MaxBackoff = defaultConnectorHealthMaxBackoff
	}
	if policy.MaxAttempts < 1 || policy.MaxAttempts > maxConnectorHealthAttempts || policy.InitialBackoff < 0 || policy.MaxBackoff < policy.InitialBackoff || policy.MaxBackoff > maxConnectorHealthBackoff {
		return ConnectorHealthPolicy{}, ErrConnectorHealthPolicyInvalid
	}
	return policy, nil
}

// StartWithHealthPolicy retries a Created, Failed, or Paused connector with
// context-aware exponential backoff. It is synchronous and opt-in. A paused
// connector is resumed; a Created or Failed connector is started. A running
// connector is returned as healthy without being interrupted.
func (r *ConnectorRegistry) StartWithHealthPolicy(ctx context.Context, id string, policy ConnectorHealthPolicy) (ConnectorHealthStatus, error) {
	if id == "" {
		return ConnectorHealthStatus{}, ErrConnectorIDEmpty
	}
	if ctx == nil {
		ctx = context.Background()
	}
	policy, err := policy.normalized()
	if err != nil {
		return ConnectorHealthStatus{}, err
	}
	entry, err := r.connectorHealthEntry(id)
	if err != nil {
		return ConnectorHealthStatus{}, err
	}
	entry.remediationMu.Lock()
	defer entry.remediationMu.Unlock()

	if status, ok := r.healthStatus(id); ok && status.State == ConnectorQuarantined {
		return status, ErrConnectorQuarantined
	}
	if lifecycle, ok := r.Status(id); !ok {
		return ConnectorHealthStatus{}, ErrConnectorNotFound
	} else if lifecycle.State == ConnectorRunning {
		return r.updateHealth(id, ConnectorHealthStatus{State: ConnectorHealthy}), nil
	}

	var lastErr error
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return r.finishHealthFailure(id, attempt-1, lastErr, err), err
		}

		lifecycle, ok := r.Status(id)
		if !ok {
			return ConnectorHealthStatus{}, ErrConnectorNotFound
		}
		var attemptErr error
		switch lifecycle.State {
		case ConnectorCreated, ConnectorFailed:
			attemptErr = r.Start(ctx, id)
		case ConnectorPaused:
			attemptErr = r.Resume(ctx, id)
		case ConnectorRunning:
			return r.updateHealth(id, ConnectorHealthStatus{State: ConnectorHealthy, Attempts: attempt - 1}), nil
		default:
			attemptErr = ErrConnectorInvalidTransition
		}
		if attemptErr == nil {
			return r.updateHealth(id, ConnectorHealthStatus{State: ConnectorHealthy, Attempts: attempt}), nil
		}
		lastErr = attemptErr
		retryable := !errors.Is(attemptErr, ErrConnectorRegistryClosed) && !errors.Is(attemptErr, ErrConnectorInvalidTransition)
		if policy.Retryable != nil {
			retryable = retryable && policy.Retryable(attemptErr)
		}
		if !retryable || attempt == policy.MaxAttempts {
			status := ConnectorHealthStatus{
				State:     ConnectorHealthFailed,
				Attempts:  attempt,
				LastError: boundedConnectorHealthError(attemptErr.Error()),
			}
			if policy.QuarantineOnFailure {
				status.State = ConnectorQuarantined
			}
			return r.updateHealth(id, status), attemptErr
		}

		backoff := connectorHealthBackoff(policy, attempt)
		r.updateHealth(id, ConnectorHealthStatus{
			State:       ConnectorRetrying,
			Attempts:    attempt,
			LastError:   boundedConnectorHealthError(attemptErr.Error()),
			NextRetryAt: time.Now().UTC().Add(backoff),
		})
		if err := waitConnectorHealthBackoff(ctx, backoff); err != nil {
			return r.finishHealthFailure(id, attempt, lastErr, err), err
		}
	}

	return r.finishHealthFailure(id, policy.MaxAttempts, lastErr, lastErr), lastErr
}

// QuarantineConnector pauses a running connector and records an explicit
// quarantine reason. Quarantined connectors cannot be retried until released.
func (r *ConnectorRegistry) QuarantineConnector(ctx context.Context, id, reason string) (ConnectorHealthStatus, error) {
	if id == "" {
		return ConnectorHealthStatus{}, ErrConnectorIDEmpty
	}
	if ctx == nil {
		ctx = context.Background()
	}
	entry, err := r.connectorHealthEntry(id)
	if err != nil {
		return ConnectorHealthStatus{}, err
	}
	entry.remediationMu.Lock()
	defer entry.remediationMu.Unlock()
	if status, ok := r.healthStatus(id); ok && status.State == ConnectorQuarantined {
		return status, nil
	}
	lifecycle, ok := r.Status(id)
	if !ok {
		return ConnectorHealthStatus{}, ErrConnectorNotFound
	}
	if lifecycle.State == ConnectorRunning {
		if err := r.Pause(ctx, id); err != nil {
			return ConnectorHealthStatus{}, err
		}
	}
	return r.updateHealth(id, ConnectorHealthStatus{
		State:     ConnectorQuarantined,
		LastError: boundedConnectorHealthError(reason),
	}), nil
}

// ReleaseConnectorQuarantine clears an explicit quarantine without starting
// the connector. The caller can then invoke StartWithHealthPolicy again.
func (r *ConnectorRegistry) ReleaseConnectorQuarantine(id string) error {
	if id == "" {
		return ErrConnectorIDEmpty
	}
	if _, err := r.connectorHealthEntry(id); err != nil {
		return err
	}
	status, ok := r.healthStatus(id)
	if !ok || status.State != ConnectorQuarantined {
		return ErrConnectorNotQuarantined
	}
	r.updateHealth(id, ConnectorHealthStatus{})
	return nil
}

// HealthStatus returns the current policy status for one registered connector.
// A connector that has not used remediation yet is reported as Unknown.
func (r *ConnectorRegistry) HealthStatus(id string) (ConnectorHealthStatus, bool) {
	if id == "" {
		return ConnectorHealthStatus{}, false
	}
	if _, err := r.connectorHealthEntry(id); err != nil {
		return ConnectorHealthStatus{}, false
	}
	if status, ok := r.healthStatus(id); ok {
		return status, true
	}
	return ConnectorHealthStatus{ID: id, State: ConnectorUnknown}, true
}

// HealthSnapshot returns bounded health statuses sorted by connector ID.
func (r *ConnectorRegistry) HealthSnapshot() []ConnectorHealthStatus {
	r.mu.RLock()
	ids := make([]string, 0, len(r.connectors))
	for id := range r.connectors {
		ids = append(ids, id)
	}
	r.mu.RUnlock()
	sort.Strings(ids)
	statuses := make([]ConnectorHealthStatus, 0, len(ids))
	for _, id := range ids {
		status, _ := r.HealthStatus(id)
		statuses = append(statuses, status)
	}
	return statuses
}

func (r *ConnectorRegistry) connectorHealthEntry(id string) (*managedConnector, error) {
	r.mu.RLock()
	closed := r.closed
	entry := r.connectors[id]
	r.mu.RUnlock()
	if closed {
		return nil, ErrConnectorRegistryClosed
	}
	if entry == nil {
		return nil, ErrConnectorNotFound
	}
	return entry, nil
}

func (r *ConnectorRegistry) healthStatus(id string) (ConnectorHealthStatus, bool) {
	r.healthMu.RLock()
	status, ok := r.health[id]
	r.healthMu.RUnlock()
	return status, ok
}

func (r *ConnectorRegistry) updateHealth(id string, status ConnectorHealthStatus) ConnectorHealthStatus {
	status.ID = id
	status.LastError = boundedConnectorHealthError(status.LastError)
	status.UpdatedAt = time.Now().UTC()
	r.healthMu.Lock()
	if r.health == nil {
		r.health = make(map[string]ConnectorHealthStatus)
	}
	r.health[id] = status
	r.healthMu.Unlock()
	return status
}

func (r *ConnectorRegistry) finishHealthFailure(id string, attempts int, lastErr, returnedErr error) ConnectorHealthStatus {
	status := ConnectorHealthStatus{State: ConnectorHealthFailed, Attempts: attempts}
	if lastErr != nil {
		status.LastError = lastErr.Error()
	} else if returnedErr != nil {
		status.LastError = returnedErr.Error()
	}
	return r.updateHealth(id, status)
}

func connectorHealthBackoff(policy ConnectorHealthPolicy, attempt int) time.Duration {
	backoff := policy.InitialBackoff
	for index := 1; index < attempt; index++ {
		if backoff >= policy.MaxBackoff-backoff {
			return policy.MaxBackoff
		}
		backoff *= 2
		if backoff >= policy.MaxBackoff {
			return policy.MaxBackoff
		}
	}
	return backoff
}

func waitConnectorHealthBackoff(ctx context.Context, backoff time.Duration) error {
	if backoff <= 0 {
		return nil
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func boundedConnectorHealthError(message string) string {
	if len(message) <= maxConnectorHealthErrorBytes {
		return message
	}
	return message[:maxConnectorHealthErrorBytes]
}
