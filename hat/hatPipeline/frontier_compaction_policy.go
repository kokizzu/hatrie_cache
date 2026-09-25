package hatPipeline

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrFrontierCompactionPolicyInvalid indicates an empty frontier ID or an
	// invalid outstanding-task bound.
	ErrFrontierCompactionPolicyInvalid = errors.New("hatPipeline: frontier compaction policy is invalid")
)

const maxFrontierCompactionOutstanding = 1 << 20

// FrontierCompactionPolicy bounds compaction work for one frontier. A policy
// is opt-in; schedulers without a policy retain their existing behavior.
type FrontierCompactionPolicy struct {
	// MaxOutstanding is the maximum number of submitted tasks that may be
	// waiting for frontier safety or executing at once for this frontier.
	MaxOutstanding int
}

type frontierCompactionPolicyRegistry struct {
	mu          sync.Mutex
	policies    map[string]FrontierCompactionPolicy
	outstanding map[string]int
	notify      chan struct{}
}

func newFrontierCompactionPolicyRegistry() *frontierCompactionPolicyRegistry {
	return &frontierCompactionPolicyRegistry{
		policies:    make(map[string]FrontierCompactionPolicy),
		outstanding: make(map[string]int),
	}
}

func (registry *frontierCompactionPolicyRegistry) set(frontierID string, policy FrontierCompactionPolicy) error {
	if registry == nil || frontierID == "" || policy.MaxOutstanding < 1 || policy.MaxOutstanding > maxFrontierCompactionOutstanding {
		return ErrFrontierCompactionPolicyInvalid
	}
	registry.mu.Lock()
	registry.policies[frontierID] = policy
	registry.signalLocked()
	registry.mu.Unlock()
	return nil
}

func (registry *frontierCompactionPolicyRegistry) clear(frontierID string) {
	if registry == nil || frontierID == "" {
		return
	}
	registry.mu.Lock()
	delete(registry.policies, frontierID)
	registry.signalLocked()
	registry.mu.Unlock()
}

func (registry *frontierCompactionPolicyRegistry) acquire(ctx context.Context, stop <-chan struct{}, frontierID string) (func(), bool, error) {
	if registry == nil {
		return noopFrontierCompactionPolicyRelease, false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return noopFrontierCompactionPolicyRelease, false, err
	}
	for {
		registry.mu.Lock()
		policy, configured := registry.policies[frontierID]
		if !configured {
			registry.mu.Unlock()
			return noopFrontierCompactionPolicyRelease, false, nil
		}
		if registry.outstanding[frontierID] < policy.MaxOutstanding {
			registry.outstanding[frontierID]++
			registry.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() { registry.release(frontierID) })
			}, true, nil
		}
		if registry.notify == nil {
			registry.notify = make(chan struct{})
		}
		notify := registry.notify
		registry.mu.Unlock()
		select {
		case <-notify:
		case <-ctx.Done():
			return noopFrontierCompactionPolicyRelease, false, ctx.Err()
		case <-stop:
			return noopFrontierCompactionPolicyRelease, false, context.Canceled
		}
	}
}

func (registry *frontierCompactionPolicyRegistry) release(frontierID string) {
	registry.mu.Lock()
	if count := registry.outstanding[frontierID]; count > 1 {
		registry.outstanding[frontierID] = count - 1
	} else {
		delete(registry.outstanding, frontierID)
	}
	registry.signalLocked()
	registry.mu.Unlock()
}

func (registry *frontierCompactionPolicyRegistry) signalLocked() {
	if registry.notify != nil {
		close(registry.notify)
		registry.notify = nil
	}
}

func noopFrontierCompactionPolicyRelease() {}
