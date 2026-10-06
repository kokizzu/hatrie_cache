package hatStorage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

var (
	// ErrStorageTierNamespaceInvalid identifies an invalid namespace policy or
	// lifecycle timestamp supplied to the namespace registry.
	ErrStorageTierNamespaceInvalid = errors.New("hatriecache: storage tier namespace policy is invalid")
	// ErrStorageTierNamespaceNotFound identifies a namespace without a policy.
	ErrStorageTierNamespaceNotFound = errors.New("hatriecache: storage tier namespace is not registered")
)

const maxStorageTierNamespaceBytes = 256

// StorageTierNamespacePolicy binds one namespace to an immutable tier policy.
// Namespace names are trimmed and validated when the registry is built or a
// policy is registered.
type StorageTierNamespacePolicy struct {
	Namespace string
	Policy    StorageTierPolicy
}

// StorageTierLifecyclePart describes a part's current tier and the timestamp
// from which its age should be derived. The timestamp is explicit so callers
// can use the same persisted lifecycle metadata for planning and recovery.
type StorageTierLifecyclePart struct {
	Key           string
	CurrentTier   string
	LifecycleTime time.Time
}

// StorageTierNamespaceRegistry maps namespaces to tier policies. Registry
// lookups and registration are concurrency-safe; each stored policy is
// immutable after construction by NewStorageTierPolicy.
type StorageTierNamespaceRegistry struct {
	mu       sync.RWMutex
	policies map[string]StorageTierPolicy
}

// NewStorageTierNamespaceRegistry validates and registers the supplied
// namespace policies. Construction is atomic from the caller's perspective:
// an invalid or duplicate entry returns an error and no registry is returned.
func NewStorageTierNamespaceRegistry(policies ...StorageTierNamespacePolicy) (*StorageTierNamespaceRegistry, error) {
	registry := &StorageTierNamespaceRegistry{
		policies: make(map[string]StorageTierPolicy, len(policies)),
	}
	for _, entry := range policies {
		if err := registry.Register(entry.Namespace, entry.Policy); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

// Register adds one namespace policy. Existing entries are rejected so a
// running process cannot silently change placement rules for a namespace.
func (registry *StorageTierNamespaceRegistry) Register(namespace string, policy StorageTierPolicy) error {
	if registry == nil {
		return ErrStorageTierNamespaceInvalid
	}
	normalized, err := normalizeStorageTierNamespace(namespace)
	if err != nil {
		return err
	}
	if len(policy.rules) == 0 {
		return fmt.Errorf("%w: tier policy is empty", ErrStorageTierNamespaceInvalid)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.policies == nil {
		registry.policies = make(map[string]StorageTierPolicy)
	}
	if _, exists := registry.policies[normalized]; exists {
		return fmt.Errorf("%w: namespace %q is already registered", ErrStorageTierNamespaceInvalid, normalized)
	}
	registry.policies[normalized] = policy
	return nil
}

// Policy returns the immutable policy registered for namespace.
func (registry *StorageTierNamespaceRegistry) Policy(namespace string) (StorageTierPolicy, error) {
	if registry == nil {
		return StorageTierPolicy{}, ErrStorageTierNamespaceInvalid
	}
	normalized, err := normalizeStorageTierNamespace(namespace)
	if err != nil {
		return StorageTierPolicy{}, err
	}
	registry.mu.RLock()
	policy, exists := registry.policies[normalized]
	registry.mu.RUnlock()
	if !exists {
		return StorageTierPolicy{}, fmt.Errorf("%w: namespace %q", ErrStorageTierNamespaceNotFound, normalized)
	}
	return policy, nil
}

// Plan derives each part's age from now and its lifecycle timestamp, then
// delegates to the existing deterministic tier move planner. It performs no
// I/O or metadata mutation.
func (registry *StorageTierNamespaceRegistry) Plan(namespace string, now time.Time, parts []StorageTierLifecyclePart) ([]StorageTierMove, error) {
	if registry == nil || now.IsZero() {
		return nil, ErrStorageTierNamespaceInvalid
	}
	policy, err := registry.Policy(namespace)
	if err != nil {
		return nil, err
	}
	storageParts := make([]StorageTierPart, len(parts))
	for index, part := range parts {
		if part.LifecycleTime.IsZero() {
			return nil, fmt.Errorf("%w: part %d lifecycle time is required", ErrStorageTierNamespaceInvalid, index)
		}
		if part.LifecycleTime.After(now) {
			return nil, fmt.Errorf("%w: part %d lifecycle time is in the future", ErrStorageTierNamespaceInvalid, index)
		}
		storageParts[index] = StorageTierPart{
			Key:         part.Key,
			CurrentTier: part.CurrentTier,
			Age:         now.Sub(part.LifecycleTime),
		}
	}
	return policy.PlanStorageTierMoves(storageParts)
}

// Execute plans through the namespace policy and invokes the caller-owned
// move executor in deterministic input order. It has the same partial-progress
// semantics as ExecuteStorageTierMoves.
func (registry *StorageTierNamespaceRegistry) Execute(ctx context.Context, namespace string, now time.Time, parts []StorageTierLifecyclePart, executor StorageTierMoveExecutor) (StorageTierMoveReport, error) {
	if ctx == nil || executor == nil {
		return StorageTierMoveReport{}, ErrStorageTierMoveInvalid
	}
	moves, err := registry.Plan(namespace, now, parts)
	if err != nil {
		return StorageTierMoveReport{}, err
	}
	report := StorageTierMoveReport{Planned: len(moves)}
	for _, move := range moves {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if err := executor(ctx, move); err != nil {
			return report, err
		}
		report.Moved++
	}
	return report, nil
}

func normalizeStorageTierNamespace(namespace string) (string, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" || len(namespace) > maxStorageTierNamespaceBytes || !utf8.ValidString(namespace) {
		return "", fmt.Errorf("%w: namespace must be non-empty, valid UTF-8, and at most %d bytes", ErrStorageTierNamespaceInvalid, maxStorageTierNamespaceBytes)
	}
	for _, character := range namespace {
		if unicode.IsControl(character) {
			return "", fmt.Errorf("%w: namespace contains a control character", ErrStorageTierNamespaceInvalid)
		}
	}
	return namespace, nil
}
