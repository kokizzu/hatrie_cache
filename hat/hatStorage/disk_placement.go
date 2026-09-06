package hatStorage

import "errors"

var (
	ErrDiskPlacementPolicyInvalid = errors.New("hatriecache: disk placement policy is invalid")
	ErrDiskPlacementUnavailable   = errors.New("hatriecache: disk placement has no configured rules")
)

// DiskPlacementRule assigns a relative selection weight to one storage path.
type DiskPlacementRule struct {
	Path   string
	Weight uint64
}

// DiskPlacementPolicy is immutable after construction and selects paths
// deterministically from a key using weighted hash buckets.
type DiskPlacementPolicy struct {
	name        string
	rules       []DiskPlacementRule
	totalWeight uint64
}

// NewDiskPlacementPolicy validates and copies weighted placement rules.
func NewDiskPlacementPolicy(name string, rules []DiskPlacementRule) (DiskPlacementPolicy, error) {
	if name == "" || len(rules) == 0 {
		return DiskPlacementPolicy{}, ErrDiskPlacementPolicyInvalid
	}
	rulesCopy := make([]DiskPlacementRule, len(rules))
	copy(rulesCopy, rules)
	seen := make(map[string]struct{}, len(rulesCopy))
	var totalWeight uint64
	for _, rule := range rulesCopy {
		if rule.Path == "" || rule.Weight == 0 {
			return DiskPlacementPolicy{}, ErrDiskPlacementPolicyInvalid
		}
		if _, exists := seen[rule.Path]; exists {
			return DiskPlacementPolicy{}, ErrDiskPlacementPolicyInvalid
		}
		seen[rule.Path] = struct{}{}
		if totalWeight > ^uint64(0)-rule.Weight {
			return DiskPlacementPolicy{}, ErrDiskPlacementPolicyInvalid
		}
		totalWeight += rule.Weight
	}
	return DiskPlacementPolicy{name: name, rules: rulesCopy, totalWeight: totalWeight}, nil
}

// Name returns the policy name.
func (policy DiskPlacementPolicy) Name() string {
	return policy.name
}

// Rules returns a copy of the placement rules.
func (policy DiskPlacementPolicy) Rules() []DiskPlacementRule {
	rules := make([]DiskPlacementRule, len(policy.rules))
	copy(rules, policy.rules)
	return rules
}

// SelectPath chooses a configured path deterministically for key. The valid
// selection path performs no allocation.
func (policy DiskPlacementPolicy) SelectPath(key string) (string, error) {
	if len(policy.rules) == 0 || policy.totalWeight == 0 {
		return "", ErrDiskPlacementUnavailable
	}
	bucket := diskPlacementHash(key) % policy.totalWeight
	for _, rule := range policy.rules {
		if bucket < rule.Weight {
			return rule.Path, nil
		}
		bucket -= rule.Weight
	}
	return "", ErrDiskPlacementUnavailable
}

func diskPlacementHash(value string) uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	hash := offset
	for index := 0; index < len(value); index++ {
		hash ^= uint64(value[index])
		hash *= prime
	}
	return hash
}
