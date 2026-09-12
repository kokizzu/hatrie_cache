package hatReplication

import (
	"errors"
	"testing"
)

func TestBuildReplicaRPOStatusClampsSequenceAndAppliesBudget(t *testing.T) {
	within := BuildReplicaRPOStatus("node-a", 10, 7, 3)
	if within.Node != "node-a" || within.SourceSequence != 10 || within.AppliedSequence != 7 || within.LagSequences != 3 || !within.RPOWithinBudget {
		t.Fatalf("within-budget status = %#v", within)
	}
	behind := BuildReplicaRPOStatus("node-b", 10, 7, 2)
	if behind.LagSequences != 3 || behind.RPOWithinBudget {
		t.Fatalf("over-budget status = %#v", behind)
	}
	ahead := BuildReplicaRPOStatus("node-c", 10, 12, 1)
	if ahead.LagSequences != 0 || !ahead.RPOWithinBudget {
		t.Fatalf("ahead status = %#v, want zero lag and within budget", ahead)
	}
	if unlimited := BuildReplicaRPOStatus("node-d", 10, 0, 0); !unlimited.RPOWithinBudget {
		t.Fatalf("zero-budget status = %#v, want disabled budget to pass", unlimited)
	}
}

func TestBuildReplicaRPOStatusesPreservesOrderAndBoundsInput(t *testing.T) {
	statuses, err := BuildReplicaRPOStatuses(20, []ReplicaRPOInput{
		{Node: "west", AppliedSequence: 19},
		{Node: "east", AppliedSequence: 12},
	}, 2)
	if err != nil {
		t.Fatalf("BuildReplicaRPOStatuses() error = %v", err)
	}
	if len(statuses) != 2 || statuses[0].Node != "west" || statuses[1].Node != "east" || statuses[1].LagSequences != 8 {
		t.Fatalf("statuses = %#v, want input order and exact lag", statuses)
	}
	tooMany := make([]ReplicaRPOInput, MaxReplicaRPOStatuses+1)
	if _, err := BuildReplicaRPOStatuses(1, tooMany, 1); !errors.Is(err, ErrReplicaRPOStatusLimit) {
		t.Fatalf("oversized status input error = %v, want limit error", err)
	}
}
