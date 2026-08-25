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
