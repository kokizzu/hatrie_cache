package hatAuth

import "testing"

var mu021PolicyBenchmarkSink bool

func BenchmarkMU021BeforePolicyAuthorize(b *testing.B) {
	policy := Policy{
		Principals: map[string][]string{"alice": {"reader"}},
		Roles: []Role{{Name: "reader", Rules: []Rule{{
			Commands:   []string{"GET"},
			Namespaces: []string{"tenant-eu:*"},
			Sources:    []string{"orders"},
		}}}},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		mu021PolicyBenchmarkSink = policy.Authorize("alice", "GET", "tenant-eu:orders", "orders")
	}
}
