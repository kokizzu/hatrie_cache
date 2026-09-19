package hatReplication

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestJournalWriteQuorumUsesStrictMajorityAndBindsProposal(t *testing.T) {
	quorum, err := NewJournalWriteQuorum(JournalWriteQuorumOptions{
		Enabled: true,
		Voters:  []string{"node-a", "node-b", "node-c"},
	})
	if err != nil {
		t.Fatalf("NewJournalWriteQuorum() error = %v", err)
	}
	proposal := JournalWriteQuorumProposal{Sequence: 42, FenceToken: 7}
	decision, err := quorum.Evaluate(proposal, []JournalWriteQuorumAcknowledgement{
		{Node: "node-c", Sequence: 42, FenceToken: 7, Accepted: true},
		{Node: "node-a", Sequence: 42, FenceToken: 7, Accepted: true},
		{Node: "node-b", Sequence: 42, FenceToken: 7},
	})
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if !decision.Satisfied || decision.Required != 2 || decision.Acknowledged != 2 || decision.Rejected != 1 || decision.Stale != 0 {
		t.Fatalf("decision = %#v, want strict-majority 2/3", decision)
	}
	if decision.Sequence != proposal.Sequence || decision.FenceToken != proposal.FenceToken {
		t.Fatalf("decision proposal = %#v, want %#v", decision, proposal)
	}
}

func TestJournalWriteQuorumRejectsStaleAcknowledgements(t *testing.T) {
	quorum, err := NewJournalWriteQuorum(JournalWriteQuorumOptions{
		Enabled:  true,
		Voters:   []string{"node-a", "node-b", "node-c"},
		Required: 2,
	})
	if err != nil {
		t.Fatalf("NewJournalWriteQuorum() error = %v", err)
	}
	decision, err := quorum.Evaluate(JournalWriteQuorumProposal{Sequence: 8, FenceToken: 3}, []JournalWriteQuorumAcknowledgement{
		{Node: "node-a", Sequence: 7, FenceToken: 3, Accepted: true},
		{Node: "node-b", Sequence: 8, FenceToken: 3, Accepted: true},
		{Node: "node-c", Sequence: 8, FenceToken: 2, Accepted: true},
	})
	if !errors.Is(err, ErrJournalWriteQuorumUnsatisfied) {
		t.Fatalf("Evaluate() error = %v, want unsatisfied quorum", err)
	}
	if decision.Satisfied || decision.Acknowledged != 1 || decision.Rejected != 0 || decision.Stale != 2 {
		t.Fatalf("decision = %#v, want one valid ack and two stale acks", decision)
	}
}

func TestJournalWriteQuorumRejectsDuplicateAndUnknownAcknowledgements(t *testing.T) {
	quorum, err := NewJournalWriteQuorum(JournalWriteQuorumOptions{Enabled: true, Voters: []string{"node-a", "node-b"}, Required: 1})
	if err != nil {
		t.Fatalf("NewJournalWriteQuorum() error = %v", err)
	}
	proposal := JournalWriteQuorumProposal{Sequence: 1}
	for name, acknowledgements := range map[string][]JournalWriteQuorumAcknowledgement{
		"duplicate": {
			{Node: "node-a", Sequence: 1, Accepted: true},
			{Node: "node-a", Sequence: 1, Accepted: true},
		},
		"unknown": {{Node: "node-x", Sequence: 1, Accepted: true}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := quorum.Evaluate(proposal, acknowledgements); !errors.Is(err, ErrJournalWriteQuorumInvalidAcknowledgement) {
				t.Fatalf("Evaluate() error = %v, want invalid acknowledgement", err)
			}
		})
	}
}

func TestJournalWriteQuorumDisabledByDefaultAndValidatesConfiguration(t *testing.T) {
	quorum, err := NewJournalWriteQuorum(JournalWriteQuorumOptions{})
	if err != nil {
		t.Fatalf("default NewJournalWriteQuorum() error = %v", err)
	}
	if quorum.Enabled() {
		t.Fatal("default quorum is enabled")
	}
	if _, err := quorum.Evaluate(JournalWriteQuorumProposal{Sequence: 1}, nil); !errors.Is(err, ErrJournalWriteQuorumDisabled) {
		t.Fatalf("disabled Evaluate() error = %v, want disabled", err)
	}
	for name, options := range map[string]JournalWriteQuorumOptions{
		"empty voters":     {Enabled: true},
		"duplicate voters": {Enabled: true, Voters: []string{"node-a", "node-a"}},
		"invalid required": {Enabled: true, Voters: []string{"node-a"}, Required: 2},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewJournalWriteQuorum(options); !errors.Is(err, ErrJournalWriteQuorumInvalidOptions) {
				t.Fatalf("NewJournalWriteQuorum(%+v) error = %v, want invalid options", options, err)
			}
		})
	}
}

func TestJournalWriteQuorumExecuteRunsAcknowledgementsAndHonorsCancellation(t *testing.T) {
	quorum, err := NewJournalWriteQuorum(JournalWriteQuorumOptions{Enabled: true, Voters: []string{"node-a", "node-b", "node-c"}, Required: 2})
	if err != nil {
		t.Fatalf("NewJournalWriteQuorum() error = %v", err)
	}
	var calls atomic.Int32
	decision, err := quorum.Execute(context.Background(), JournalWriteQuorumProposal{Sequence: 9, FenceToken: 2}, func(_ context.Context, node string, proposal JournalWriteQuorumProposal) (JournalWriteQuorumAcknowledgement, error) {
		calls.Add(1)
		return JournalWriteQuorumAcknowledgement{Node: node, Sequence: proposal.Sequence, FenceToken: proposal.FenceToken, Accepted: node != "node-c"}, nil
	})
	if err != nil || !decision.Satisfied || decision.Acknowledged != 2 || calls.Load() != 3 {
		t.Fatalf("Execute() = %#v/%v, calls=%d; want satisfied 2/3", decision, err, calls.Load())
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := quorum.Execute(ctx, JournalWriteQuorumProposal{Sequence: 10}, func(context.Context, string, JournalWriteQuorumProposal) (JournalWriteQuorumAcknowledgement, error) {
		t.Fatal("canceled Execute() invoked acknowledgement callback")
		return JournalWriteQuorumAcknowledgement{}, nil
	}); !errors.Is(err, ErrJournalWriteQuorumContextCanceled) {
		t.Fatalf("canceled Execute() error = %v, want context cancellation", err)
	}
}

func TestJournalWriteQuorumExecuteDoesNotCountMismatchedCallbackNode(t *testing.T) {
	quorum, err := NewJournalWriteQuorum(JournalWriteQuorumOptions{Enabled: true, Voters: []string{"node-a", "node-b", "node-c"}, Required: 2})
	if err != nil {
		t.Fatalf("NewJournalWriteQuorum() error = %v", err)
	}
	decision, err := quorum.Execute(context.Background(), JournalWriteQuorumProposal{Sequence: 11}, func(_ context.Context, node string, proposal JournalWriteQuorumProposal) (JournalWriteQuorumAcknowledgement, error) {
		if node == "node-a" {
			return JournalWriteQuorumAcknowledgement{Node: "node-b", Sequence: proposal.Sequence, Accepted: true}, nil
		}
		if node == "node-c" {
			return JournalWriteQuorumAcknowledgement{}, errors.New("unavailable")
		}
		return JournalWriteQuorumAcknowledgement{Node: node, Sequence: proposal.Sequence, Accepted: true}, nil
	})
	if !errors.Is(err, ErrJournalWriteQuorumUnsatisfied) || decision.Satisfied || decision.Acknowledged != 1 {
		t.Fatalf("Execute() = %#v/%v, want one counted acknowledgement and unsatisfied quorum", decision, err)
	}
}
