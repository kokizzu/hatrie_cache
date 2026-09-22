package hatReplication

import (
	"context"
	"errors"
	"testing"
)

func TestT201PerSpaceWriteQuorumOnlyEnforcesConfiguredSpaces(t *testing.T) {
	quorum, err := NewPerSpaceWriteQuorum(PerSpaceWriteQuorumOptions{
		Policies: []PerSpaceWriteQuorumPolicy{{
			Space:    "critical_orders",
			Voters:   []string{"local", "east", "west"},
			Required: 2,
		}},
	})
	if err != nil {
		t.Fatalf("NewPerSpaceWriteQuorum() error = %v", err)
	}
	proposal := JournalWriteQuorumProposal{Sequence: 17, FenceToken: 4}
	called := make([]string, 3)
	result, err := quorum.Execute(context.Background(), "critical_orders", proposal, func(_ context.Context, space, node string, got JournalWriteQuorumProposal) (JournalWriteQuorumAcknowledgement, error) {
		index := 0
		switch node {
		case "east":
			index = 1
		case "west":
			index = 2
		}
		called[index] = space + ":" + node
		return JournalWriteQuorumAcknowledgement{Node: node, Sequence: got.Sequence, FenceToken: got.FenceToken, Accepted: node != "west"}, nil
	})
	if err != nil {
		t.Fatalf("critical Execute() error = %v", err)
	}
	if !result.Enforced || !result.Decision.Satisfied || result.Decision.Acknowledged != 2 {
		t.Fatalf("critical result = %+v, want enforced satisfied quorum with two acknowledgements", result)
	}
	if len(called) != 3 || called[0] != "critical_orders:local" || called[1] != "critical_orders:east" || called[2] != "critical_orders:west" {
		t.Fatalf("critical callback order = %#v", called)
	}

	ordinaryCalled := false
	ordinary, err := quorum.Execute(context.Background(), "ordinary_events", proposal, func(context.Context, string, string, JournalWriteQuorumProposal) (JournalWriteQuorumAcknowledgement, error) {
		ordinaryCalled = true
		return JournalWriteQuorumAcknowledgement{}, nil
	})
	if err != nil {
		t.Fatalf("ordinary Execute() error = %v", err)
	}
	if ordinary.Enforced || ordinaryCalled {
		t.Fatalf("ordinary result/callback = %+v/%v, want disabled without callback", ordinary, ordinaryCalled)
	}
	if got := quorum.ConfiguredSpaces(); len(got) != 1 || got[0] != "critical_orders" || !quorum.EnabledFor(" critical_orders ") {
		t.Fatalf("configured spaces/enabled = %#v/%v, want normalized critical space", got, quorum.EnabledFor(" critical_orders "))
	}
}

func TestT201PerSpaceWriteQuorumDefaultsToStrictMajorityAndRejectsDuplicateSpaces(t *testing.T) {
	quorum, err := NewPerSpaceWriteQuorum(PerSpaceWriteQuorumOptions{
		Policies: []PerSpaceWriteQuorumPolicy{{Space: "critical", Voters: []string{"a", "b", "c"}}},
	})
	if err != nil {
		t.Fatalf("strict-majority configuration: %v", err)
	}
	result, err := quorum.Execute(context.Background(), "critical", JournalWriteQuorumProposal{Sequence: 1}, func(_ context.Context, _, node string, proposal JournalWriteQuorumProposal) (JournalWriteQuorumAcknowledgement, error) {
		return JournalWriteQuorumAcknowledgement{Node: node, Sequence: proposal.Sequence, Accepted: node != "c"}, nil
	})
	if err != nil || !result.Decision.Satisfied || result.Decision.Required != 2 {
		t.Fatalf("strict-majority result/error = %+v/%v, want required=2 and success", result, err)
	}
	evaluated, err := quorum.Evaluate(" critical ", JournalWriteQuorumProposal{Sequence: 1}, []JournalWriteQuorumAcknowledgement{
		{Node: "a", Sequence: 1, Accepted: true},
		{Node: "b", Sequence: 1, Accepted: true},
	})
	if err != nil || !evaluated.Enforced || !evaluated.Decision.Satisfied {
		t.Fatalf("Evaluate() result/error = %+v/%v, want enforced success", evaluated, err)
	}
	_, err = NewPerSpaceWriteQuorum(PerSpaceWriteQuorumOptions{
		Policies: []PerSpaceWriteQuorumPolicy{{Space: "critical", Voters: []string{"a"}}, {Space: " critical ", Voters: []string{"b"}}},
	})
	if !errors.Is(err, ErrPerSpaceWriteQuorumInvalidOptions) {
		t.Fatalf("duplicate space error = %v, want ErrPerSpaceWriteQuorumInvalidOptions", err)
	}
}
