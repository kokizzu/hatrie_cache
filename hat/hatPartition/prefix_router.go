package hatPartition

import (
	"fmt"
	"sort"
	"strings"
)

// PrefixRule maps a key prefix to an explicit logical partition. Prefixes are
// case-sensitive and overlapping prefixes are allowed; the longest matching
// prefix wins.
type PrefixRule struct {
	Prefix    string
	Partition string
}

// PrefixRouter is an immutable, allocation-free key router. The zero value is
// disabled and never applies an implicit default partition.
type PrefixRouter struct {
	rules []PrefixRule
}

// NewPrefixRouter validates and compiles explicit prefix rules. Rule strings
// are trimmed once at construction, while lookup keys are matched verbatim.
func NewPrefixRouter(rules []PrefixRule) (PrefixRouter, error) {
	if len(rules) == 0 {
		return PrefixRouter{}, nil
	}
	compiled := make([]PrefixRule, len(rules))
	seen := make(map[string]struct{}, len(rules))
	for index, rule := range rules {
		rule.Prefix = strings.TrimSpace(rule.Prefix)
		rule.Partition = strings.TrimSpace(rule.Partition)
		if rule.Prefix == "" {
			return PrefixRouter{}, fmt.Errorf("hatriecache: prefix rule %d has an empty prefix", index)
		}
		if rule.Partition == "" {
			return PrefixRouter{}, fmt.Errorf("hatriecache: prefix rule %d has an empty partition", index)
		}
		if _, exists := seen[rule.Prefix]; exists {
			return PrefixRouter{}, fmt.Errorf("hatriecache: duplicate prefix rule %q", rule.Prefix)
		}
		seen[rule.Prefix] = struct{}{}
		compiled[index] = rule
	}
	sort.SliceStable(compiled, func(left, right int) bool {
		if len(compiled[left].Prefix) != len(compiled[right].Prefix) {
			return len(compiled[left].Prefix) > len(compiled[right].Prefix)
		}
		return compiled[left].Prefix < compiled[right].Prefix
	})
	return PrefixRouter{rules: compiled}, nil
}

// Route returns the explicit partition for key. It allocates nothing and
// returns false when no configured prefix matches.
func (router PrefixRouter) Route(key string) (partition string, ok bool) {
	for _, rule := range router.rules {
		if strings.HasPrefix(key, rule.Prefix) {
			return rule.Partition, true
		}
	}
	return "", false
}

// Rules returns an independent normalized snapshot of the router rules.
func (router PrefixRouter) Rules() []PrefixRule {
	if len(router.rules) == 0 {
		return nil
	}
	rules := make([]PrefixRule, len(router.rules))
	copy(rules, router.rules)
	return rules
}
