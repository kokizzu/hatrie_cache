package hatCache

import (
	"fmt"
	"strings"
)

// WriteQuorumRule assigns a synchronous write quorum to one logical keyspace.
// KeyPrefix is matched against the beginning of a public command key.
type WriteQuorumRule struct {
	KeyPrefix string
	Quorum    int
}

// WriteQuorumPolicy enables synchronous quorum only for selected keyspaces
// while leaving unrelated keys on the existing replication path.
//
// The longest matching prefix wins. A nil policy or an empty Rules slice is
// disabled and preserves the legacy asynchronous or best-effort behavior.
type WriteQuorumPolicy struct {
	Rules []WriteQuorumRule
}

// RequiredForKey returns the configured quorum for key, or zero when no rule
// matches. Invalid rules are rejected before a matching command is applied.
func (policy *WriteQuorumPolicy) RequiredForKey(key string) (int, error) {
	if policy == nil {
		return 0, nil
	}
	longestPrefix := -1
	required := 0
	for index, rule := range policy.Rules {
		if rule.KeyPrefix == "" {
			return 0, fmt.Errorf("write quorum rule %d: key prefix must not be empty", index)
		}
		if rule.Quorum < 1 {
			return 0, fmt.Errorf("write quorum rule %d: quorum must be positive", index)
		}
		for previous := 0; previous < index; previous++ {
			if policy.Rules[previous].KeyPrefix == rule.KeyPrefix {
				return 0, fmt.Errorf("write quorum rule %d: duplicate key prefix %q", index, rule.KeyPrefix)
			}
		}
		if len(rule.KeyPrefix) > longestPrefix && strings.HasPrefix(key, rule.KeyPrefix) {
			longestPrefix = len(rule.KeyPrefix)
			required = rule.Quorum
		}
	}
	return required, nil
}
