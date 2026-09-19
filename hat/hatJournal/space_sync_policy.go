package hatJournal

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultSpaceSyncPolicyEntries bounds the number of named overrides in a
	// new registry. The registry is opt-in and does not change journal writes.
	DefaultSpaceSyncPolicyEntries = 128
	// MaxSpaceSyncPolicyEntries prevents untrusted configuration from growing
	// the policy map without bound.
	MaxSpaceSyncPolicyEntries = 4096
	// MaxSpaceSyncPolicySpaceBytes bounds one logical space name.
	MaxSpaceSyncPolicySpaceBytes = 256
)

var (
	ErrSpaceSyncPolicyNil              = errors.New("hatJournal: space sync policy registry is nil")
	ErrSpaceSyncPolicyCapacityInvalid  = errors.New("hatJournal: space sync policy capacity is invalid")
	ErrSpaceSyncPolicyCapacityExceeded = errors.New("hatJournal: space sync policy capacity exceeded")
	ErrSpaceSyncPolicySpaceInvalid     = errors.New("hatJournal: space sync policy space is invalid")
	ErrSpaceSyncPolicyInvalid          = errors.New("hatJournal: space sync policy is invalid")
)

// SpaceSyncPolicy describes the durability mode a caller should apply to a
// named space before or after appending its journal record. Periodic uses the
// journal's configured group-commit window/batch, Immediate requires a sync
// for each committed append, and Disabled permits no explicit sync.
type SpaceSyncPolicy uint8

const (
	SpaceSyncPolicyPeriodic SpaceSyncPolicy = iota + 1
	SpaceSyncPolicyImmediate
	SpaceSyncPolicyDisabled
)

func (policy SpaceSyncPolicy) String() string {
	switch policy {
	case SpaceSyncPolicyPeriodic:
		return "periodic"
	case SpaceSyncPolicyImmediate:
		return "immediate"
	case SpaceSyncPolicyDisabled:
		return "disabled"
	default:
		return "unknown"
	}
}

// Durable reports whether the policy requires the caller to retain data
// across an ordinary process restart. Disabled is intended only for callers
// that accept loss of unsynced records.
func (policy SpaceSyncPolicy) Durable() bool {
	return policy == SpaceSyncPolicyPeriodic || policy == SpaceSyncPolicyImmediate
}

// Immediate reports whether the caller must sync after every committed
// record rather than using a group-commit boundary.
func (policy SpaceSyncPolicy) Immediate() bool {
	return policy == SpaceSyncPolicyImmediate
}

func validSpaceSyncPolicy(policy SpaceSyncPolicy) bool {
	return policy >= SpaceSyncPolicyPeriodic && policy <= SpaceSyncPolicyDisabled
}

// SpaceSyncPolicyOptions configures an opt-in bounded registry. A zero
// Capacity or DefaultPolicy selects the conservative defaults.
type SpaceSyncPolicyOptions struct {
	Capacity      int
	DefaultPolicy SpaceSyncPolicy
}

// SpaceSyncPolicyEntry is one normalized named-space override.
type SpaceSyncPolicyEntry struct {
	Space  string          `json:"space"`
	Policy SpaceSyncPolicy `json:"policy"`
}

// SpaceSyncPolicyRegistry stores bounded per-space durability overrides. It
// only resolves policy; the caller owns journal append, fsync, recovery, and
// authorization. Resolve is allocation-free for already-normalized names.
type SpaceSyncPolicyRegistry struct {
	mu            sync.RWMutex
	capacity      int
	defaultPolicy SpaceSyncPolicy
	policies      map[string]SpaceSyncPolicy
}

// NewSpaceSyncPolicyRegistry creates an opt-in bounded policy registry.
func NewSpaceSyncPolicyRegistry(options SpaceSyncPolicyOptions) (*SpaceSyncPolicyRegistry, error) {
	capacity := options.Capacity
	if capacity == 0 {
		capacity = DefaultSpaceSyncPolicyEntries
	}
	if capacity < 1 || capacity > MaxSpaceSyncPolicyEntries {
		return nil, ErrSpaceSyncPolicyCapacityInvalid
	}
	defaultPolicy := options.DefaultPolicy
	if defaultPolicy == 0 {
		defaultPolicy = SpaceSyncPolicyPeriodic
	}
	if !validSpaceSyncPolicy(defaultPolicy) {
		return nil, ErrSpaceSyncPolicyInvalid
	}
	return &SpaceSyncPolicyRegistry{
		capacity:      capacity,
		defaultPolicy: defaultPolicy,
		policies:      make(map[string]SpaceSyncPolicy, capacity),
	}, nil
}

func normalizeSpaceSyncPolicySpace(space string) (string, error) {
	space = strings.TrimSpace(space)
	if space == "" || len(space) > MaxSpaceSyncPolicySpaceBytes {
		return "", ErrSpaceSyncPolicySpaceInvalid
	}
	return space, nil
}

// Register creates or replaces one named-space override.
func (registry *SpaceSyncPolicyRegistry) Register(space string, policy SpaceSyncPolicy) error {
	if registry == nil {
		return ErrSpaceSyncPolicyNil
	}
	normalized, err := normalizeSpaceSyncPolicySpace(space)
	if err != nil {
		return err
	}
	if !validSpaceSyncPolicy(policy) {
		return ErrSpaceSyncPolicyInvalid
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.policies[normalized]; !exists && len(registry.policies) >= registry.capacity {
		return ErrSpaceSyncPolicyCapacityExceeded
	}
	registry.policies[normalized] = policy
	return nil
}

// Remove deletes one named override and reports whether it existed.
func (registry *SpaceSyncPolicyRegistry) Remove(space string) bool {
	if registry == nil {
		return false
	}
	normalized, err := normalizeSpaceSyncPolicySpace(space)
	if err != nil {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.policies[normalized]; !exists {
		return false
	}
	delete(registry.policies, normalized)
	return true
}

// Resolve returns a named override or the configured default. Invalid or
// empty lookup names deliberately resolve to the default so a caller's hot
// write path cannot accidentally make an unsafe policy decision from malformed
// metadata.
func (registry *SpaceSyncPolicyRegistry) Resolve(space string) SpaceSyncPolicy {
	if registry == nil {
		return 0
	}
	space = strings.TrimSpace(space)
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	if policy, ok := registry.policies[space]; ok {
		return policy
	}
	return registry.defaultPolicy
}

// Snapshot returns deterministic copies of all named overrides.
func (registry *SpaceSyncPolicyRegistry) Snapshot() []SpaceSyncPolicyEntry {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	entries := make([]SpaceSyncPolicyEntry, 0, len(registry.policies))
	for space, policy := range registry.policies {
		entries = append(entries, SpaceSyncPolicyEntry{Space: space, Policy: policy})
	}
	registry.mu.RUnlock()
	sort.Slice(entries, func(left, right int) bool { return entries[left].Space < entries[right].Space })
	return entries
}

// DefaultPolicy returns the fallback policy used for unknown spaces.
func (registry *SpaceSyncPolicyRegistry) DefaultPolicy() SpaceSyncPolicy {
	if registry == nil {
		return 0
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return registry.defaultPolicy
}
