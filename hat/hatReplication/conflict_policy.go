package hatReplication

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrConflictPolicyInvalid       = errors.New("hatriecache: conflict policy is invalid")
	ErrConflictPolicySpaceRequired = errors.New("hatriecache: conflict policy space is required")
	ErrConflictPolicyRegistryNil   = errors.New("hatriecache: conflict policy registry is nil")
	ErrConflictRejected            = errors.New("hatriecache: conflict rejected")
)

const (
	MaxConflictPolicySpaces  = 4096
	MaxConflictPolicySources = 256
)

// ConflictPolicyMode selects how two versions in one named space are merged.
type ConflictPolicyMode uint8

const (
	// ConflictPolicyLastWriteWins preserves the existing timestamp/node/sequence
	// ordering and is the zero-value default.
	ConflictPolicyLastWriteWins ConflictPolicyMode = iota
	// ConflictPolicySourcePriority prefers the first matching source in
	// SourcePriority, then falls back to last-write-wins for equal priority.
	ConflictPolicySourcePriority
	// ConflictPolicyReject fails when two distinct versions conflict.
	ConflictPolicyReject
)

// ConflictPolicy configures conflict behavior for one named space. Source
// priority is ordered highest-first and is copied when installed in a registry.
type ConflictPolicy struct {
	Mode           ConflictPolicyMode
	SourcePriority []string
}

// ConflictPolicyRegistry stores an optional default and per-space overrides.
// It is independent of transport and can be used by replication or application
// code before applying a conflicting write.
type ConflictPolicyRegistry struct {
	mu             sync.RWMutex
	defaultPolicy  ConflictPolicy
	spaceOverrides map[string]ConflictPolicy
}

// NewConflictPolicyRegistry validates an explicit default policy. An empty
// policy selects ConflictPolicyLastWriteWins.
func NewConflictPolicyRegistry(defaultPolicy ConflictPolicy) (*ConflictPolicyRegistry, error) {
	normalized, err := normalizeConflictPolicy(defaultPolicy)
	if err != nil {
		return nil, err
	}
	return &ConflictPolicyRegistry{defaultPolicy: normalized}, nil
}

// Set installs or replaces the policy for one named space.
func (registry *ConflictPolicyRegistry) Set(space string, policy ConflictPolicy) error {
	if registry == nil {
		return ErrConflictPolicyRegistryNil
	}
	space = strings.TrimSpace(space)
	if space == "" {
		return ErrConflictPolicySpaceRequired
	}
	normalized, err := normalizeConflictPolicy(policy)
	if err != nil {
		return err
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.spaceOverrides == nil {
		registry.spaceOverrides = make(map[string]ConflictPolicy)
	}
	if _, exists := registry.spaceOverrides[space]; !exists && len(registry.spaceOverrides) >= MaxConflictPolicySpaces {
		return fmt.Errorf("%w: maximum spaces %d exceeded", ErrConflictPolicyInvalid, MaxConflictPolicySpaces)
	}
	registry.spaceOverrides[space] = normalized
	return nil
}

// Delete removes a per-space override. It reports whether an override existed.
func (registry *ConflictPolicyRegistry) Delete(space string) bool {
	if registry == nil {
		return false
	}
	space = strings.TrimSpace(space)
	if space == "" {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.spaceOverrides[space]; !exists {
		return false
	}
	delete(registry.spaceOverrides, space)
	return true
}

// Resolve applies the configured policy for space to two conflict versions.
func (registry *ConflictPolicyRegistry) Resolve(space string, left, right ConflictVersion) (ConflictVersion, error) {
	if registry == nil {
		return ConflictVersion{}, ErrConflictPolicyRegistryNil
	}
	space = strings.TrimSpace(space)
	if space == "" {
		return ConflictVersion{}, ErrConflictPolicySpaceRequired
	}
	registry.mu.RLock()
	policy, exists := registry.spaceOverrides[space]
	if !exists {
		policy = registry.defaultPolicy
	}
	registry.mu.RUnlock()
	return resolveConflictWithPolicy(policy, left, right)
}

func normalizeConflictPolicy(policy ConflictPolicy) (ConflictPolicy, error) {
	switch policy.Mode {
	case ConflictPolicyLastWriteWins, ConflictPolicyReject:
		if len(policy.SourcePriority) != 0 {
			return ConflictPolicy{}, fmt.Errorf("%w: source priority is only valid with source-priority mode", ErrConflictPolicyInvalid)
		}
		return ConflictPolicy{Mode: policy.Mode}, nil
	case ConflictPolicySourcePriority:
		if len(policy.SourcePriority) == 0 || len(policy.SourcePriority) > MaxConflictPolicySources {
			return ConflictPolicy{}, fmt.Errorf("%w: source priority must contain 1..%d sources", ErrConflictPolicyInvalid, MaxConflictPolicySources)
		}
		sources := make([]string, len(policy.SourcePriority))
		for index, source := range policy.SourcePriority {
			source = strings.TrimSpace(source)
			if source == "" {
				return ConflictPolicy{}, fmt.Errorf("%w: source priority contains an empty source", ErrConflictPolicyInvalid)
			}
			for previous := 0; previous < index; previous++ {
				if sources[previous] == source {
					return ConflictPolicy{}, fmt.Errorf("%w: source priority contains duplicate source %q", ErrConflictPolicyInvalid, source)
				}
			}
			sources[index] = source
		}
		return ConflictPolicy{Mode: policy.Mode, SourcePriority: sources}, nil
	default:
		return ConflictPolicy{}, fmt.Errorf("%w: unsupported mode %d", ErrConflictPolicyInvalid, policy.Mode)
	}
}

func resolveConflictWithPolicy(policy ConflictPolicy, left, right ConflictVersion) (ConflictVersion, error) {
	switch policy.Mode {
	case ConflictPolicyLastWriteWins:
		return ResolveConflictVersion(left, right)
	case ConflictPolicyReject:
		comparison, err := CompareConflictVersions(left, right)
		if err != nil {
			return ConflictVersion{}, err
		}
		if comparison == 0 {
			return left, nil
		}
		return ConflictVersion{}, ErrConflictRejected
	case ConflictPolicySourcePriority:
		leftPriority := conflictSourcePriority(policy.SourcePriority, left.NodeID)
		rightPriority := conflictSourcePriority(policy.SourcePriority, right.NodeID)
		if leftPriority < rightPriority {
			if _, err := CompareConflictVersions(left, right); err != nil {
				return ConflictVersion{}, err
			}
			return left, nil
		}
		if rightPriority < leftPriority {
			if _, err := CompareConflictVersions(left, right); err != nil {
				return ConflictVersion{}, err
			}
			return right, nil
		}
		return ResolveConflictVersion(left, right)
	default:
		return ConflictVersion{}, fmt.Errorf("%w: unsupported mode %d", ErrConflictPolicyInvalid, policy.Mode)
	}
}

func conflictSourcePriority(priority []string, source string) int {
	for index, candidate := range priority {
		if candidate == source {
			return index
		}
	}
	return len(priority)
}
