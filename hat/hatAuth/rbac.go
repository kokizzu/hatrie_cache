package hatAuth

import "strings"

// Role grants access through one or more rules. Empty policy remains disabled
// for compatibility; configured roles default-deny requests with no match.
type Role struct {
	Name  string `json:"name"`
	Rules []Rule `json:"rules"`
}

// Rule matches cache commands, key namespaces, SQL sources, and optional
// objects. An empty selector is unrestricted for that selector; a trailing *
// is a prefix match.
type Rule struct {
	Commands   []string `json:"commands,omitempty"`
	Namespaces []string `json:"namespaces,omitempty"`
	Sources    []string `json:"sources,omitempty"`
	Objects    []string `json:"objects,omitempty"`
}

// Policy maps authenticated principals to named roles.
type Policy struct {
	Principals map[string][]string `json:"principals,omitempty"`
	Roles      []Role              `json:"roles,omitempty"`
}

// AuthorizationRequest contains the dimensions used by a policy rule. Object
// is optional for legacy callers but is required when a matching rule has an
// object selector.
type AuthorizationRequest struct {
	Command   string
	Namespace string
	Source    string
	Object    string
}

// Authorize reports whether principal has one role rule matching every
// supplied non-empty request dimension. It preserves the legacy API; callers
// that need object grants should use AuthorizeObject or AuthorizeRequest.
func (policy Policy) Authorize(principal, command, namespace, source string) bool {
	return policy.AuthorizeRequest(principal, AuthorizationRequest{
		Command:   command,
		Namespace: namespace,
		Source:    source,
	})
}

// AuthorizeObject reports whether principal has a role rule matching the
// supplied command, namespace, source, and object dimensions.
func (policy Policy) AuthorizeObject(principal, command, namespace, source, object string) bool {
	return policy.AuthorizeRequest(principal, AuthorizationRequest{
		Command:   command,
		Namespace: namespace,
		Source:    source,
		Object:    object,
	})
}

// AuthorizeRequest reports whether principal has a role rule matching every
// supplied request dimension. Object selectors fail closed when Object is
// empty so they cannot be bypassed by an older call site.
func (policy Policy) AuthorizeRequest(principal string, request AuthorizationRequest) bool {
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
			if commandSelectorMatches(rule.Commands, request.Command) &&
				selectorMatches(rule.Namespaces, request.Namespace) &&
				selectorMatches(rule.Sources, request.Source) &&
				requiredSelectorMatches(rule.Objects, request.Object) {
				return true
			}
		}
	}
	return false
}

func requiredSelectorMatches(selectors []string, value string) bool {
	if len(selectors) == 0 {
		return true
	}
	if strings.TrimSpace(value) == "" {
		return false
	}
	return selectorMatches(selectors, value)
}

func commandSelectorMatches(selectors []string, value string) bool {
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

func selectorMatches(selectors []string, value string) bool {
	if len(selectors) == 0 || strings.TrimSpace(value) == "" {
		return true
	}
	value = strings.TrimSpace(value)
	for _, selector := range selectors {
		selector = strings.TrimSpace(selector)
		if selector == "*" || selector == value {
			return true
		}
		if strings.HasSuffix(selector, "*") && strings.HasPrefix(value, strings.TrimSuffix(selector, "*")) {
			return true
		}
	}
	return false
}
