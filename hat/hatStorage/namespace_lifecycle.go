package hatStorage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"hatrie_cache/hat/hatSql"
)

var (
	ErrNamespaceFrozen   = errors.New("storage namespace is frozen")
	ErrNamespaceArchived = errors.New("storage namespace is archived")
	ErrNamespaceDeleted  = errors.New("storage namespace is deleted")
)

// NamespaceExpiryAction selects the terminal action when a namespace expires.
type NamespaceExpiryAction string

const (
	NamespaceExpiryArchive NamespaceExpiryAction = "archive"
	NamespaceExpiryDelete  NamespaceExpiryAction = "delete"
)

// NamespaceLifecycleHook performs backend-specific archival or deletion. The
// hook makes destructive storage handling explicit instead of guessing paths.
type NamespaceLifecycleHook func(context.Context, string, SQLAdapter) error

// NamespaceLifecyclePolicy controls one namespace. A zero concurrent limit is
// unlimited; ExpiryAction defaults to archive when ExpiresAt is set.
type NamespaceLifecyclePolicy struct {
	MaxConcurrentQueries int
	ExpiresAt            time.Time
	ExpiryAction         NamespaceExpiryAction
	Archive              NamespaceLifecycleHook
	Delete               NamespaceLifecycleHook
}

// NamespaceLifecycleController applies lifecycle policy before invoking the
// registry's normal SQL path. Use it as the namespace-facing execution API.
type NamespaceLifecycleController struct {
	registry *SQLAdapterRegistry
	mu       sync.Mutex
	entries  map[string]*namespaceLifecycleEntry
}

type namespaceLifecycleEntry struct {
	mu     sync.RWMutex
	policy NamespaceLifecyclePolicy
	state  namespaceLifecycleState
	gate   chan struct{}
}

type namespaceLifecycleState uint8

const (
	namespaceActive namespaceLifecycleState = iota
	namespaceFrozen
	namespaceArchived
	namespaceDeleted
)

// NewNamespaceLifecycleController validates policies against registered
// namespaces and copies their configuration.
func NewNamespaceLifecycleController(registry *SQLAdapterRegistry, policies map[string]NamespaceLifecyclePolicy) (*NamespaceLifecycleController, error) {
	if registry == nil {
		return nil, fmt.Errorf("SQL adapter registry is required")
	}
	controller := &NamespaceLifecycleController{registry: registry, entries: make(map[string]*namespaceLifecycleEntry, len(policies))}
	for namespace, policy := range policies {
		if _, err := registry.adapter(namespace); err != nil {
			return nil, err
		}
		if err := validateNamespaceLifecyclePolicy(policy); err != nil {
			return nil, fmt.Errorf("namespace %q: %w", namespace, err)
		}
		controller.entries[namespace] = newNamespaceLifecycleEntry(policy)
	}
	return controller, nil
}

func validateNamespaceLifecyclePolicy(policy NamespaceLifecyclePolicy) error {
	if policy.MaxConcurrentQueries < 0 {
		return fmt.Errorf("max concurrent queries must not be negative")
	}
	if policy.ExpiryAction != "" && policy.ExpiryAction != NamespaceExpiryArchive && policy.ExpiryAction != NamespaceExpiryDelete {
		return fmt.Errorf("invalid expiry action %q", policy.ExpiryAction)
	}
	return nil
}

func newNamespaceLifecycleEntry(policy NamespaceLifecyclePolicy) *namespaceLifecycleEntry {
	entry := &namespaceLifecycleEntry{policy: policy}
	if policy.MaxConcurrentQueries > 0 {
		entry.gate = make(chan struct{}, policy.MaxConcurrentQueries)
	}
	return entry
}

func (controller *NamespaceLifecycleController) entry(namespace string) (*namespaceLifecycleEntry, error) {
	if controller == nil || controller.registry == nil {
		return nil, fmt.Errorf("namespace lifecycle controller is required")
	}
	namespace = strings.TrimSpace(namespace)
	if _, err := controller.registry.adapter(namespace); err != nil {
		return nil, err
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if entry := controller.entries[namespace]; entry != nil {
		return entry, nil
	}
	entry := newNamespaceLifecycleEntry(NamespaceLifecyclePolicy{})
	controller.entries[namespace] = entry
	return entry, nil
}

// Execute runs SQL only while the namespace is active. The configured quota
// queues excess callers and observes context cancellation before execution.
func (controller *NamespaceLifecycleController) Execute(ctx context.Context, namespace, source string, parameters []interface{}, options hatSql.SQLQueryOptions) (hatSql.SQLQueryResult, error) {
	entry, err := controller.entry(namespace)
	if err != nil {
		return hatSql.SQLQueryResult{}, err
	}
	if err := controller.expireOne(ctx, namespace, entry, time.Now()); err != nil {
		return hatSql.SQLQueryResult{}, err
	}
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	if err := namespaceStateError(entry.state); err != nil {
		return hatSql.SQLQueryResult{}, err
	}
	if entry.gate != nil {
		select {
		case entry.gate <- struct{}{}:
			defer func() { <-entry.gate }()
		case <-ctx.Done():
			return hatSql.SQLQueryResult{}, ctx.Err()
		}
	}
	return controller.registry.Execute(ctx, namespace, source, parameters, options)
}

// Freeze prevents new lifecycle-controlled queries until Unfreeze.
func (controller *NamespaceLifecycleController) Freeze(namespace string) error {
	return controller.setFrozen(namespace, true)
}
func (controller *NamespaceLifecycleController) Unfreeze(namespace string) error {
	return controller.setFrozen(namespace, false)
}
func (controller *NamespaceLifecycleController) setFrozen(namespace string, frozen bool) error {
	entry, err := controller.entry(namespace)
	if err != nil {
		return err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.state == namespaceArchived {
		return ErrNamespaceArchived
	}
	if entry.state == namespaceDeleted {
		return ErrNamespaceDeleted
	}
	if !frozen && entry.state != namespaceFrozen {
		return nil
	}
	if frozen && entry.state == namespaceFrozen {
		return nil
	}
	if entry.state != namespaceActive && entry.state != namespaceFrozen {
		return fmt.Errorf("invalid namespace lifecycle state")
	}
	if frozen {
		entry.state = namespaceFrozen
	} else {
		entry.state = namespaceActive
	}
	return nil
}

func (controller *NamespaceLifecycleController) Archive(ctx context.Context, namespace string) error {
	return controller.transition(ctx, namespace, namespaceArchived)
}
func (controller *NamespaceLifecycleController) Delete(ctx context.Context, namespace string) error {
	return controller.transition(ctx, namespace, namespaceDeleted)
}
func (controller *NamespaceLifecycleController) transition(ctx context.Context, namespace string, next namespaceLifecycleState) error {
	entry, err := controller.entry(namespace)
	if err != nil {
		return err
	}
	adapter, err := controller.registry.adapter(namespace)
	if err != nil {
		return err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.state == namespaceDeleted {
		return ErrNamespaceDeleted
	}
	if entry.state == namespaceArchived && next == namespaceArchived {
		return ErrNamespaceArchived
	}
	hook := entry.policy.Archive
	if next == namespaceDeleted {
		hook = entry.policy.Delete
	}
	if hook == nil {
		return fmt.Errorf("namespace %q %s hook is required", namespace, lifecycleStateName(next))
	}
	if err := hook(ctx, namespace, adapter); err != nil {
		return err
	}
	entry.state = next
	return nil
}

// Expire executes the configured lifecycle action for every namespace whose
// expiration has passed and returns the affected namespace names.
func (controller *NamespaceLifecycleController) Expire(ctx context.Context, now time.Time) ([]string, error) {
	controller.mu.Lock()
	names := make([]string, 0, len(controller.entries))
	for name := range controller.entries {
		names = append(names, name)
	}
	controller.mu.Unlock()
	var expired []string
	for _, name := range names {
		entry, err := controller.entry(name)
		if err != nil {
			return expired, err
		}
		if err := controller.expireOne(ctx, name, entry, now); err != nil {
			return expired, err
		}
		entry.mu.RLock()
		state := entry.state
		entry.mu.RUnlock()
		if state == namespaceArchived || state == namespaceDeleted {
			expired = append(expired, name)
		}
	}
	return expired, nil
}

func (controller *NamespaceLifecycleController) expireOne(ctx context.Context, namespace string, entry *namespaceLifecycleEntry, now time.Time) error {
	entry.mu.RLock()
	expiresAt, action, state := entry.policy.ExpiresAt, entry.policy.ExpiryAction, entry.state
	entry.mu.RUnlock()
	if expiresAt.IsZero() || now.Before(expiresAt) || state == namespaceArchived || state == namespaceDeleted {
		return nil
	}
	if action == NamespaceExpiryDelete {
		return controller.Delete(ctx, namespace)
	}
	return controller.Archive(ctx, namespace)
}

func namespaceStateError(state namespaceLifecycleState) error {
	switch state {
	case namespaceFrozen:
		return ErrNamespaceFrozen
	case namespaceArchived:
		return ErrNamespaceArchived
	case namespaceDeleted:
		return ErrNamespaceDeleted
	default:
		return nil
	}
}
func lifecycleStateName(state namespaceLifecycleState) string {
	if state == namespaceDeleted {
		return "delete"
	}
	return "archive"
}
