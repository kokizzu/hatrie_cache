package hatReplication_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestEvaluateWriteQuorumReportsSatisfiedAndUnsatisfiedDecisions(t *testing.T) {
	decision, err := hatReplication.EvaluateWriteQuorum(5, 3, 3)
	if err != nil || !decision.Satisfied || decision.Total != 5 || decision.Acknowledged != 3 || decision.Required != 3 {
		t.Fatalf("satisfied quorum = %#v, %v", decision, err)
	}
	decision, err = hatReplication.EvaluateWriteQuorum(5, 2, 3)
	if !errors.Is(err, hatReplication.ErrWriteQuorumUnsatisfied) || decision.Satisfied || decision.Acknowledged != 2 {
		t.Fatalf("unsatisfied quorum = %#v, %v", decision, err)
	}
}

func TestEvaluateWriteQuorumRejectsInvalidInputs(t *testing.T) {
	for name, values := range map[string][3]int{
		"zero total":        {0, 0, 1},
		"negative total":    {-1, 0, 1},
		"negative acks":     {3, -1, 1},
		"acks above total":  {3, 4, 1},
		"zero required":     {3, 1, 0},
		"required too high": {3, 1, 4},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := hatReplication.EvaluateWriteQuorum(values[0], values[1], values[2]); !errors.Is(err, hatReplication.ErrWriteQuorumInvalid) {
				t.Fatalf("EvaluateWriteQuorum() error = %v, want ErrWriteQuorumInvalid", err)
			}
		})
	}
}

func BenchmarkEvaluateWriteQuorum(b *testing.B) {
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := hatReplication.EvaluateWriteQuorum(5, 3, 3); err != nil {
			b.Fatal(err)
		}
	}
}
