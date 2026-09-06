package hatReplication_test

import (
	"errors"
	"testing"

	hatReplication "hatrie_cache/hat/hatReplication"
)

func TestQuorumPolicyEvaluatesReadAndWriteWithMajorityDefaults(t *testing.T) {
	policy, err := hatReplication.NewDefaultQuorumPolicy(5)
	if err != nil {
		t.Fatalf("NewDefaultQuorumPolicy() error = %v", err)
	}
	if policy.Total != 5 || policy.ReadRequired != 3 || policy.WriteRequired != 3 {
		t.Fatalf("default policy = %#v, want total=5 read/write=3", policy)
	}

	read, err := policy.EvaluateRead(3)
	if err != nil || !read.Satisfied || read.Acknowledged != 3 {
		t.Fatalf("EvaluateRead() = %#v, %v", read, err)
	}
	write, err := policy.EvaluateWrite(3)
	if err != nil || !write.Satisfied || write.Acknowledged != 3 {
		t.Fatalf("EvaluateWrite() = %#v, %v", write, err)
	}
}

func TestQuorumPolicyReportsUnsatisfiedAndRejectsInvalidInput(t *testing.T) {
	policy, err := hatReplication.NewQuorumPolicy(4, 2, 3)
	if err != nil {
		t.Fatalf("NewQuorumPolicy() error = %v", err)
	}
	if _, err := policy.EvaluateRead(1); !errors.Is(err, hatReplication.ErrReadQuorumUnsatisfied) {
		t.Fatalf("EvaluateRead(1) error = %v, want ErrReadQuorumUnsatisfied", err)
	}
	if _, err := policy.EvaluateWrite(2); !errors.Is(err, hatReplication.ErrWriteQuorumUnsatisfied) {
		t.Fatalf("EvaluateWrite(2) error = %v, want ErrWriteQuorumUnsatisfied", err)
	}

	for name, values := range map[string][3]int{
		"zero total":      {0, 1, 1},
		"read too large":  {3, 4, 2},
		"write too small": {3, 2, 0},
		"read too small":  {3, 0, 4},
		"negative total":  {-1, 1, 1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := hatReplication.NewQuorumPolicy(values[0], values[1], values[2]); !errors.Is(err, hatReplication.ErrQuorumPolicyInvalid) {
				t.Fatalf("NewQuorumPolicy(%v) error = %v, want ErrQuorumPolicyInvalid", values, err)
			}
		})
	}
}
