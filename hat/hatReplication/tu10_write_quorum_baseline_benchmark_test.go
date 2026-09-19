//go:build tu10baseline

package hatReplication

import "testing"

func BenchmarkTU10BaselineWriteQuorumDecision(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		decision, err := EvaluateWriteQuorum(3, 3, 2)
		if err != nil || !decision.Satisfied {
			b.Fatalf("EvaluateWriteQuorum() = %#v/%v", decision, err)
		}
	}
}
