package hatJournal

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// SyncMode controls when journal bytes are made durable for a logical space.
// The zero value is synchronous after option validation.
type SyncMode string

const (
	SyncModeSynchronous SyncMode = "synchronous"
	SyncModePeriodic    SyncMode = "periodic"
	SyncModeDisabled    SyncMode = "disabled"
)

const maxSyncPolicyRules = 1024

// SyncPolicyRule assigns a sync mode to keys sharing SpacePrefix. The longest
// matching prefix wins, so broad defaults can be safely refined for one space.
type SyncPolicyRule struct {
	SpacePrefix string
	Mode        SyncMode
}

// SyncPolicy selects the default journal durability mode and optional
// key-prefix overrides. Periodic mode is write-triggered: a sync is requested
// when the configured interval has elapsed, and Sync can force the boundary.
type SyncPolicy struct {
	Default          SyncMode
	PeriodicInterval time.Duration
	Rules            []SyncPolicyRule
}

func normalizeSyncMode(mode SyncMode) (SyncMode, error) {
	switch strings.ToLower(strings.TrimSpace(string(mode))) {
	case "", string(SyncModeSynchronous), "sync", "immediate":
		return SyncModeSynchronous, nil
	case string(SyncModePeriodic), "period":
		return SyncModePeriodic, nil
	case string(SyncModeDisabled), "off", "none":
		return SyncModeDisabled, nil
	default:
		return "", fmt.Errorf("hatJournal: unsupported sync mode %q", mode)
	}
}

func (policy SyncPolicy) normalized() (SyncPolicy, error) {
	defaultMode, err := normalizeSyncMode(policy.Default)
	if err != nil {
		return SyncPolicy{}, err
	}
	if len(policy.Rules) > maxSyncPolicyRules {
		return SyncPolicy{}, fmt.Errorf("hatJournal: sync policy rules must be <= %d", maxSyncPolicyRules)
	}
	normalized := SyncPolicy{
		Default:          defaultMode,
		PeriodicInterval: policy.PeriodicInterval,
		Rules:            make([]SyncPolicyRule, len(policy.Rules)),
	}
	if normalized.PeriodicInterval < 0 {
		return SyncPolicy{}, errors.New("hatJournal: periodic sync interval must be non-negative")
	}
	seen := make(map[string]struct{}, len(policy.Rules))
	hasPeriodic := defaultMode == SyncModePeriodic
	for index, rule := range policy.Rules {
		prefix := strings.TrimSpace(rule.SpacePrefix)
		if prefix == "" {
			return SyncPolicy{}, errors.New("hatJournal: sync policy space prefix is required")
		}
		if _, exists := seen[prefix]; exists {
			return SyncPolicy{}, fmt.Errorf("hatJournal: duplicate sync policy space prefix %q", prefix)
		}
		seen[prefix] = struct{}{}
		mode, modeErr := normalizeSyncMode(rule.Mode)
		if modeErr != nil {
			return SyncPolicy{}, fmt.Errorf("sync policy rule %d: %w", index, modeErr)
		}
		normalized.Rules[index] = SyncPolicyRule{SpacePrefix: prefix, Mode: mode}
		hasPeriodic = hasPeriodic || mode == SyncModePeriodic
	}
	if hasPeriodic && normalized.PeriodicInterval <= 0 {
		return SyncPolicy{}, errors.New("hatJournal: periodic sync interval must be positive when periodic mode is used")
	}
	sort.SliceStable(normalized.Rules, func(left, right int) bool {
		return len(normalized.Rules[left].SpacePrefix) > len(normalized.Rules[right].SpacePrefix)
	})
	return normalized, nil
}

// ModeForKey returns the validated mode for a key. It is allocation-free on
// the hot path and uses the longest matching space prefix.
func (policy SyncPolicy) ModeForKey(key string) SyncMode {
	for _, rule := range policy.Rules {
		if strings.HasPrefix(key, rule.SpacePrefix) {
			return rule.Mode
		}
	}
	if policy.Default == "" {
		return SyncModeSynchronous
	}
	return policy.Default
}

func (policy SyncPolicy) hasPeriodicMode() bool {
	if policy.Default == SyncModePeriodic {
		return true
	}
	for _, rule := range policy.Rules {
		if rule.Mode == SyncModePeriodic {
			return true
		}
	}
	return false
}

// HasPeriodicMode reports whether the policy starts a periodic write boundary
// or uses one for any configured space.
func (policy SyncPolicy) HasPeriodicMode() bool {
	return policy.hasPeriodicMode()
}
