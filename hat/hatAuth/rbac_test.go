package hatAuth

import "testing"

func TestPolicyAuthorizesScopedRole(t *testing.T) {
	policy := Policy{Principals: map[string][]string{"reader-token": {"reader"}}, Roles: []Role{{Name: "reader", Rules: []Rule{{Commands: []string{"GET"}, Namespaces: []string{"tenant-a:*"}, Sources: []string{"orders"}}}}}}
	if !policy.Authorize("reader-token", "GET", "tenant-a:42", "orders") {
		t.Fatal("expected scoped request allowed")
	}
	if policy.Authorize("reader-token", "SETSTR", "tenant-a:42", "orders") {
		t.Fatal("expected command denied")
	}
	if policy.Authorize("reader-token", "GET", "tenant-b:42", "orders") {
		t.Fatal("expected namespace denied")
	}
	if policy.Authorize("reader-token", "GET", "tenant-a:42", "users") {
		t.Fatal("expected source denied")
	}
}

func TestPolicyDefaultsAndScopeMatchingAreConservative(t *testing.T) {
	if !(Policy{}).Authorize("", "SETSTR", "any", "any") {
		t.Fatal("empty policy must remain disabled for compatibility")
	}
	policy := Policy{
		Principals: map[string][]string{"operator-token": {"operator"}},
		Roles: []Role{{Name: "operator", Rules: []Rule{{
			Commands:   []string{"get"},
			Namespaces: []string{"Tenant-A:*"},
			Sources:    []string{"Orders"},
		}}}},
	}
	if !policy.Authorize("operator-token", "GET", "Tenant-A:42", "Orders") {
		t.Fatal("expected exact scoped request allowed")
	}
	if policy.Authorize("operator-token", "GET", "tenant-a:42", "Orders") {
		t.Fatal("namespace matching must preserve key case")
	}
	if policy.Authorize("operator-token", "GET", "Tenant-A:42", "orders") {
		t.Fatal("source matching must preserve source case")
	}
	if policy.Authorize("operator-token", "GET", "Tenant-A-42", "Orders") {
		t.Fatal("prefix selector must not match a different namespace delimiter")
	}
}
