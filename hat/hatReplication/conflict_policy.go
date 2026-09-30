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

// ConflictResolutionDecision describes the outcome reported to a conflict
// hook. Rejected conflicts have no winner.
type ConflictResolutionDecision uint8

const (
	ConflictResolutionLeftWins ConflictResolutionDecision = iota + 1
	ConflictResolutionRightWins
	ConflictResolutionRejected
)

// ConflictResolutionEvent contains bounded conflict metadata for an
// application-owned hook. ConflictVersion carries the source node and source
// sequence for both competing writes; raw keys and values are not retained.
type ConflictResolutionEvent struct {
	Space    string
	Left     ConflictVersion
	Right    ConflictVersion
	Winner   ConflictVersion
	Decision ConflictResolutionDecision
}

// ConflictHook observes a non-equal conflict after the configured policy has
// selected a winner or rejected the conflict. Hooks must be safe for concurrent
// calls and cannot change the already selected result.
type ConflictHook func(ConflictResolutionEvent)

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
	return resolveConflictWithPolicy(policy.Mode, policy.SourcePriority, left, right)
}

// ResolveWithHook resolves one conflict and invokes hook for a non-equal,
// valid conflict after the policy selects a winner or rejects it. The hook is
// explicit so the existing Resolve hot path remains allocation- and callback-
// free. Hooks must be safe for concurrent calls and cannot change the result.
func (registry *ConflictPolicyRegistry) ResolveWithHook(space string, left, right ConflictVersion, hook ConflictHook) (ConflictVersion, error) {
	winner, resolveErr := registry.Resolve(space, left, right)
	if hook == nil || resolveErr != nil && !errors.Is(resolveErr, ErrConflictRejected) {
		return winner, resolveErr
	}
	comparison, compareErr := CompareConflictVersions(left, right)
	if compareErr != nil || comparison == 0 {
		return winner, resolveErr
	}
	event := ConflictResolutionEvent{Space: strings.TrimSpace(space), Left: left, Right: right, Winner: winner}
	if resolveErr != nil {
		event.Decision = ConflictResolutionRejected
		event.Winner = ConflictVersion{}
	} else if winner == left {
		event.Decision = ConflictResolutionLeftWins
	} else {
		event.Decision = ConflictResolutionRightWins
	}
	hook(event)
	return winner, resolveErr
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

func resolveConflictWithPolicy(mode ConflictPolicyMode, sourcePriority []string, left, right ConflictVersion) (ConflictVersion, error) {
	switch mode {
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
		leftPriority := conflictSourcePriority(sourcePriority, left.NodeID)
		rightPriority := conflictSourcePriority(sourcePriority, right.NodeID)
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
		return ConflictVersion{}, fmt.Errorf("%w: unsupported mode %d", ErrConflictPolicyInvalid, mode)
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
