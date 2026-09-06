package hatStorage

import (
	"errors"
	"sort"
	"strings"
	"time"
)

var (
	ErrStorageTierPolicyInvalid = errors.New("hatriecache: storage tier policy is invalid")
	ErrStorageTierUnavailable   = errors.New("hatriecache: storage tier is unavailable")
)

// StorageTierRule selects a placement policy after a part reaches MinAge.
// Rules are ordered by MinAge during construction, so callers may configure
// them in any order.
type StorageTierRule struct {
	Name      string
	MinAge    time.Duration
	Placement DiskPlacementPolicy
}

// StorageTierSelection is the tier and path selected for a part.
type StorageTierSelection struct {
	Tier string
	Path string
}

// StorageTierPolicy is immutable after construction and classifies parts by
// age before selecting a path from the matching disk placement policy.
type StorageTierPolicy struct {
	rules []StorageTierRule
}

// NewStorageTierPolicy validates and copies age-based tier rules. A zero-age
// rule is required so every non-negative age has a deterministic tier.
func NewStorageTierPolicy(rules []StorageTierRule) (StorageTierPolicy, error) {
	if len(rules) == 0 {
		return StorageTierPolicy{}, ErrStorageTierPolicyInvalid
	}
	rulesCopy := make([]StorageTierRule, len(rules))
	copy(rulesCopy, rules)
	seenNames := make(map[string]struct{}, len(rulesCopy))
	for index := range rulesCopy {
		rule := &rulesCopy[index]
		rule.Name = strings.TrimSpace(rule.Name)
		if rule.Name == "" || rule.MinAge < 0 || len(rule.Placement.Rules()) == 0 {
			return StorageTierPolicy{}, ErrStorageTierPolicyInvalid
		}
		if _, exists := seenNames[rule.Name]; exists {
			return StorageTierPolicy{}, ErrStorageTierPolicyInvalid
		}
		seenNames[rule.Name] = struct{}{}
	}
	sort.Slice(rulesCopy, func(left, right int) bool {
		return rulesCopy[left].MinAge < rulesCopy[right].MinAge
	})
	if rulesCopy[0].MinAge != 0 {
		return StorageTierPolicy{}, ErrStorageTierPolicyInvalid
	}
	for index := 1; index < len(rulesCopy); index++ {
		if rulesCopy[index-1].MinAge == rulesCopy[index].MinAge {
			return StorageTierPolicy{}, ErrStorageTierPolicyInvalid
		}
	}
	return StorageTierPolicy{rules: rulesCopy}, nil
}

// Rules returns a copy of the normalized, age-sorted tier rules.
func (policy StorageTierPolicy) Rules() []StorageTierRule {
	rules := make([]StorageTierRule, len(policy.rules))
	copy(rules, policy.rules)
	return rules
}

// Select chooses the latest tier whose threshold is not newer than age and
// delegates path selection to that tier's immutable placement policy.
func (policy StorageTierPolicy) Select(age time.Duration, key string) (StorageTierSelection, error) {
	if age < 0 || len(policy.rules) == 0 {
		return StorageTierSelection{}, ErrStorageTierUnavailable
	}
	rule := policy.rules[0]
	for index := 1; index < len(policy.rules); index++ {
		if age < policy.rules[index].MinAge {
			break
		}
		rule = policy.rules[index]
	}
	path, err := rule.Placement.SelectPath(key)
	if err != nil {
		return StorageTierSelection{}, err
	}
	return StorageTierSelection{Tier: rule.Name, Path: path}, nil
}
