package hatAuth

import "strings"

// Role grants access through one or more rules. Empty policy remains disabled
// for compatibility; configured roles default-deny requests with no match.
type Role struct {
	Name  string `json:"name"`
	Rules []Rule `json:"rules"`
}

// Rule matches cache commands, key namespaces, and SQL sources. An empty
// selector is unrestricted for that selector; a trailing * is a prefix match.
type Rule struct {
	Commands   []string `json:"commands,omitempty"`
	Namespaces []string `json:"namespaces,omitempty"`
	Sources    []string `json:"sources,omitempty"`
}

// Policy maps authenticated principals to named roles.
type Policy struct {
	Principals map[string][]string `json:"principals,omitempty"`
	Roles      []Role              `json:"roles,omitempty"`
}

// Authorize reports whether principal has one role rule matching every
// supplied non-empty request dimension.
func (policy Policy) Authorize(principal, command, namespace, source string) bool {
	if len(policy.Principals) == 0 && len(policy.Roles) == 0 {
		return true
	}
	roles := make(map[string]struct{})
	for _, role := range policy.Principals[strings.TrimSpace(principal)] {
		roles[strings.TrimSpace(role)] = struct{}{}
	}
	for _, role := range policy.Roles {
		if _, ok := roles[role.Name]; !ok {
			continue
		}
		for _, rule := range role.Rules {
			if selectorMatches(rule.Commands, command) && selectorMatches(rule.Namespaces, namespace) && selectorMatches(rule.Sources, source) {
				return true
			}
		}
	}
	return false
}

func selectorMatches(selectors []string, value string) bool {
	if len(selectors) == 0 || strings.TrimSpace(value) == "" {
		return true
	}
	value = strings.ToUpper(strings.TrimSpace(value))
	for _, selector := range selectors {
		selector = strings.ToUpper(strings.TrimSpace(selector))
		if selector == "*" || selector == value {
			return true
		}
		if strings.HasSuffix(selector, "*") && strings.HasPrefix(value, strings.TrimSuffix(selector, "*")) {
			return true
		}
	}
	return false
}
