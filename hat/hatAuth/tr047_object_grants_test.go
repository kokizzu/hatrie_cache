package hatAuth

import "testing"

func TestPolicyAuthorizesObjectScopedRole(t *testing.T) {
	policy := Policy{
		Principals: map[string][]string{"reader-token": {"reader"}},
		Roles: []Role{{Name: "reader", Rules: []Rule{{
			Commands:   []string{"GET"},
			Namespaces: []string{"tenant-a:*"},
			Objects:    []string{"space:orders"},
		}}}},
	}

	if !policy.AuthorizeObject("reader-token", "GET", "tenant-a:1", "", "space:orders") {
		t.Fatal("expected object-scoped request allowed")
	}
	if policy.AuthorizeObject("reader-token", "GET", "tenant-a:1", "", "space:users") {
		t.Fatal("expected different object denied")
	}
	if policy.AuthorizeObject("reader-token", "GET", "tenant-b:1", "", "space:orders") {
		t.Fatal("expected different namespace denied")
	}
	if policy.Authorize("reader-token", "GET", "tenant-a:1", "") {
		t.Fatal("legacy authorization must not bypass an object-constrained rule")
	}
}

func TestPolicyObjectGrantRequiresObjectAndPreservesLegacyRules(t *testing.T) {
	policy := Policy{
		Principals: map[string][]string{"reader-token": {"reader"}},
		Roles: []Role{{Name: "reader", Rules: []Rule{
			{Commands: []string{"GET"}, Objects: []string{"space:orders:*"}},
			{Commands: []string{"GET"}, Namespaces: []string{"tenant-b:*"}},
		}}},
	}

	if policy.AuthorizeObject("reader-token", "GET", "tenant-a:1", "", "") {
		t.Fatal("object-constrained rule must reject a missing object")
	}
	if !policy.AuthorizeObject("reader-token", "GET", "tenant-a:1", "", "space:orders:archive") {
		t.Fatal("expected object prefix grant allowed")
	}
	if policy.AuthorizeObject("reader-token", "GET", "tenant-a:1", "", "space:users") {
		t.Fatal("expected unmatched object denied")
	}
	if !policy.AuthorizeObject("reader-token", "GET", "tenant-b:1", "", "space:users") {
		t.Fatal("expected legacy namespace-only rule preserved")
	}
}
