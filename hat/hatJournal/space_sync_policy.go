package hatJournal

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// SpaceSyncMode controls when a journal write for one named space requires a
// filesystem sync. The journal bytes are always written; disabled mode only
// changes the durability barrier.
type SpaceSyncMode string

const (
	SpaceSyncSynchronous SpaceSyncMode = "synchronous"
	SpaceSyncPeriodic    SpaceSyncMode = "periodic"
	SpaceSyncDisabled    SpaceSyncMode = "disabled"
)

// SpaceSyncPolicy is the opt-in durability policy for one named journal
// space. Periodic mode requires a positive interval; the other modes reject
// an interval so configuration mistakes cannot silently weaken durability.
type SpaceSyncPolicy struct {
	Mode     SpaceSyncMode
	Interval time.Duration
}

const (
	MaxSpaceSyncPolicies  = 256
	MaxSpaceSyncNameBytes = 256
	MaxSpaceSyncInterval  = 365 * 24 * time.Hour
)

var ErrSpaceSyncPolicyInvalid = errors.New("hatJournal: space sync policy is invalid")

func normalizeSpaceSyncPolicies(policies map[string]SpaceSyncPolicy) (map[string]SpaceSyncPolicy, error) {
	if len(policies) == 0 {
		return nil, nil
	}
	if len(policies) > MaxSpaceSyncPolicies {
		return nil, fmt.Errorf("%w: %d policies exceed %d", ErrSpaceSyncPolicyInvalid, len(policies), MaxSpaceSyncPolicies)
	}
	normalized := make(map[string]SpaceSyncPolicy, len(policies))
	for rawName, policy := range policies {
		name := strings.TrimSpace(rawName)
		if name == "" || len([]byte(name)) > MaxSpaceSyncNameBytes {
			return nil, fmt.Errorf("%w: space name is empty or exceeds %d bytes", ErrSpaceSyncPolicyInvalid, MaxSpaceSyncNameBytes)
		}
		if _, exists := normalized[name]; exists {
			return nil, fmt.Errorf("%w: duplicate normalized space %q", ErrSpaceSyncPolicyInvalid, name)
		}
		switch policy.Mode {
		case SpaceSyncSynchronous, SpaceSyncDisabled:
			if policy.Interval != 0 {
				return nil, fmt.Errorf("%w: %s mode cannot set an interval", ErrSpaceSyncPolicyInvalid, policy.Mode)
			}
		case SpaceSyncPeriodic:
			if policy.Interval <= 0 || policy.Interval > MaxSpaceSyncInterval {
				return nil, fmt.Errorf("%w: periodic interval must be between 1ns and %s", ErrSpaceSyncPolicyInvalid, MaxSpaceSyncInterval)
			}
		default:
			return nil, fmt.Errorf("%w: unsupported mode %q", ErrSpaceSyncPolicyInvalid, policy.Mode)
		}
		normalized[name] = policy
	}
	return normalized, nil
}
