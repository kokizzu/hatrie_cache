package hatReplication

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestT201SpaceWriteQuorumAppliesOnlyToConfiguredCriticalSpaces(t *testing.T) {
	quorum, err := NewSpaceWriteQuorum(SpaceWriteQuorumOptions{
		Enabled: true,
		Spaces: map[string]JournalWriteQuorumOptions{
			"payments": {
				Enabled:  true,
				Voters:   []string{"primary", "replica-a", "replica-b"},
				Required: 2,
			},
		},
	})
	if err != nil {
		t.Fatalf("NewSpaceWriteQuorum() error = %v", err)
	}
	if !quorum.Enabled() || !quorum.EnabledFor(" payments ") || quorum.EnabledFor("cache") {
		t.Fatalf("space enablement = enabled=%v payments=%v cache=%v", quorum.Enabled(), quorum.EnabledFor(" payments "), quorum.EnabledFor("cache"))
	}
	selected, err := quorum.ForSpace(" payments ")
	if err != nil {
		t.Fatalf("ForSpace(payments) error = %v", err)
	}
	proposal := JournalWriteQuorumProposal{Sequence: 17, FenceToken: 9}
	decision, err := selected.Evaluate(proposal, []JournalWriteQuorumAcknowledgement{
		{Node: "primary", Sequence: 17, FenceToken: 9, Accepted: true},
		{Node: "replica-a", Sequence: 17, FenceToken: 9, Accepted: true},
	})
	if err != nil || !decision.Satisfied || decision.Acknowledged != 2 {
		t.Fatalf("critical-space decision = %#v/%v, want satisfied with two acknowledgements", decision, err)
	}
	if _, err := quorum.ForSpace("cache"); !errors.Is(err, ErrSpaceWriteQuorumNotConfigured) {
		t.Fatalf("unconfigured space error = %v, want ErrSpaceWriteQuorumNotConfigured", err)
	}

	var calls atomic.Int32
	decision, err = selected.Execute(context.Background(), proposal, func(ctx context.Context, node string, got JournalWriteQuorumProposal) (JournalWriteQuorumAcknowledgement, error) {
		calls.Add(1)
		if got != proposal {
			t.Errorf("proposal = %#v, want %#v", got, proposal)
		}
		return JournalWriteQuorumAcknowledgement{Node: node, Sequence: got.Sequence, FenceToken: got.FenceToken, Accepted: true}, nil
	})
	if err != nil || !decision.Satisfied || calls.Load() != 3 {
		t.Fatalf("critical-space Execute() = %#v/%v with %d calls, want satisfied/3", decision, err, calls.Load())
	}
}

func TestT201SpaceWriteQuorumDefaultsOffAndRejectsUnsafeConfiguration(t *testing.T) {
	disabled, err := NewSpaceWriteQuorum(SpaceWriteQuorumOptions{})
	if err != nil {
		t.Fatalf("default NewSpaceWriteQuorum() error = %v", err)
	}
	if disabled.Enabled() || disabled.EnabledFor("payments") {
		t.Fatalf("default policy = %#v, want disabled", disabled)
	}
	if _, err := disabled.ForSpace("payments"); !errors.Is(err, ErrSpaceWriteQuorumDisabled) {
		t.Fatalf("disabled ForSpace() error = %v, want ErrSpaceWriteQuorumDisabled", err)
	}
	cases := []SpaceWriteQuorumOptions{
		{Enabled: true},
		{Enabled: true, Spaces: map[string]JournalWriteQuorumOptions{"": {Enabled: true, Voters: []string{"a"}}}},
		{Enabled: true, Spaces: map[string]JournalWriteQuorumOptions{"payments": {Voters: []string{"a"}}}},
	}
	for index, options := range cases {
		if _, err := NewSpaceWriteQuorum(options); !errors.Is(err, ErrSpaceWriteQuorumInvalidOptions) {
			t.Errorf("invalid options %d error = %v, want ErrSpaceWriteQuorumInvalidOptions", index, err)
		}
	}
	if _, err := NewSpaceWriteQuorum(SpaceWriteQuorumOptions{
		Enabled: true,
		Spaces: map[string]JournalWriteQuorumOptions{
			"payments":   {Enabled: true, Voters: []string{"a"}},
			" payments ": {Enabled: true, Voters: []string{"b"}},
		},
	}); !errors.Is(err, ErrSpaceWriteQuorumInvalidOptions) {
		t.Fatalf("duplicate normalized spaces error = %v, want ErrSpaceWriteQuorumInvalidOptions", err)
	}
}

func BenchmarkT201SpaceWriteQuorumEvaluate(b *testing.B) {
	quorum, err := NewSpaceWriteQuorum(SpaceWriteQuorumOptions{
		Enabled: true,
		Spaces: map[string]JournalWriteQuorumOptions{
			"payments": {Enabled: true, Voters: []string{"primary", "replica-a", "replica-b"}, Required: 2},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	proposal := JournalWriteQuorumProposal{Sequence: 17, FenceToken: 9}
	acknowledgements := []JournalWriteQuorumAcknowledgement{
		{Node: "primary", Sequence: 17, FenceToken: 9, Accepted: true},
		{Node: "replica-a", Sequence: 17, FenceToken: 9, Accepted: true},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := quorum.Evaluate("payments", proposal, acknowledgements); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT201SpaceWriteQuorumCachedPolicy(b *testing.B) {
	quorum, err := NewSpaceWriteQuorum(SpaceWriteQuorumOptions{
		Enabled: true,
		Spaces: map[string]JournalWriteQuorumOptions{
			"payments": {Enabled: true, Voters: []string{"primary", "replica-a", "replica-b"}, Required: 2},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	selected, err := quorum.ForSpace("payments")
	if err != nil {
		b.Fatal(err)
	}
	proposal := JournalWriteQuorumProposal{Sequence: 17, FenceToken: 9}
	acknowledgements := []JournalWriteQuorumAcknowledgement{
		{Node: "primary", Sequence: 17, FenceToken: 9, Accepted: true},
		{Node: "replica-a", Sequence: 17, FenceToken: 9, Accepted: true},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := selected.Evaluate(proposal, acknowledgements); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT201JournalWriteQuorumEvaluateControl(b *testing.B) {
	quorum, err := NewJournalWriteQuorum(JournalWriteQuorumOptions{
		Enabled: true, Voters: []string{"primary", "replica-a", "replica-b"}, Required: 2,
	})
	if err != nil {
		b.Fatal(err)
	}
	proposal := JournalWriteQuorumProposal{Sequence: 17, FenceToken: 9}
	acknowledgements := []JournalWriteQuorumAcknowledgement{
		{Node: "primary", Sequence: 17, FenceToken: 9, Accepted: true},
		{Node: "replica-a", Sequence: 17, FenceToken: 9, Accepted: true},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := quorum.Evaluate(proposal, acknowledgements); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT201SpaceWriteQuorumDisabled(b *testing.B) {
	quorum, err := NewSpaceWriteQuorum(SpaceWriteQuorumOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if quorum.Enabled() {
			b.Fatal("default policy unexpectedly enabled")
		}
	}
}
