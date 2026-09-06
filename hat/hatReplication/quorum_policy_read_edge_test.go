package hatReplication_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestQuorumPolicyRejectsZeroValueAndInvalidAcknowledgements(t *testing.T) {
	var zero hatReplication.QuorumPolicy
	if _, err := zero.EvaluateRead(0); !errors.Is(err, hatReplication.ErrQuorumPolicyInvalid) {
		t.Fatalf("zero-value policy error = %v, want ErrQuorumPolicyInvalid", err)
	}

	policy, err := hatReplication.NewDefaultQuorumPolicy(3)
	if err != nil {
		t.Fatalf("NewDefaultQuorumPolicy: %v", err)
	}

	if _, err := policy.EvaluateRead(-1); !errors.Is(err, hatReplication.ErrReadQuorumInvalid) {
		t.Fatalf("negative read acknowledgements error = %v, want ErrReadQuorumInvalid", err)
	}
	if _, err := policy.EvaluateRead(4); !errors.Is(err, hatReplication.ErrReadQuorumInvalid) {
		t.Fatalf("excess read acknowledgements error = %v, want ErrReadQuorumInvalid", err)
	}
	if _, err := policy.EvaluateWrite(-1); !errors.Is(err, hatReplication.ErrWriteQuorumInvalid) {
		t.Fatalf("negative write acknowledgements error = %v, want ErrWriteQuorumInvalid", err)
	}
	if _, err := policy.EvaluateWrite(4); !errors.Is(err, hatReplication.ErrWriteQuorumInvalid) {
		t.Fatalf("excess write acknowledgements error = %v, want ErrWriteQuorumInvalid", err)
	}
}
