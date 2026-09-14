package hatAuth

import "testing"

func tr047BenchmarkPolicy(command, namespace, source, object string) Policy {
	rule := Rule{Commands: []string{command}}
	if namespace != "" {
		rule.Namespaces = []string{namespace}
	}
	if source != "" {
		rule.Sources = []string{source}
	}
	if object != "" {
		rule.Objects = []string{object}
	}
	return Policy{
		Principals: map[string][]string{"reader-token": {"reader"}},
		Roles:      []Role{{Name: "reader", Rules: []Rule{rule}}},
	}
}

func BenchmarkPolicyAuthorizeLegacy(b *testing.B) {
	policy := tr047BenchmarkPolicy("GET", "tenant-a:*", "people", "")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !policy.Authorize("reader-token", "GET", "tenant-a:people", "people") {
			b.Fatal("legacy authorization denied")
		}
	}
}

func BenchmarkPolicyAuthorizeObject(b *testing.B) {
	policy := tr047BenchmarkPolicy("GET", "tenant-a:*", "people", "tenant-a:people")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !policy.AuthorizeObject("reader-token", "GET", "tenant-a:people", "people", "tenant-a:people") {
			b.Fatal("object authorization denied")
		}
	}
}

func BenchmarkPolicyAuthorizeObjectSource(b *testing.B) {
	policy := tr047BenchmarkPolicy("SQL", "", "people", "people")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !policy.AuthorizeObject("reader-token", "SQL", "people", "people", "people") {
			b.Fatal("source authorization denied")
		}
	}
}
