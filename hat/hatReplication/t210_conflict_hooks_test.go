package hatReplication

import (
	"crypto/sha256"
	"errors"
	"testing"
)

func TestT210ConflictHookReceivesSourceSequenceAndDigest(t *testing.T) {
	digest := sha256.Sum256([]byte("orders:42"))
	local := ConflictVersion{Timestamp: 100, NodeID: "region-a", Sequence: 7}
	remote := ConflictVersion{Timestamp: 1, NodeID: "region-b", Sequence: 44}
	var observed ConflictHookContext
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{
		Mode: ConflictPolicyLastWriteWins,
		Hook: func(context ConflictHookContext) (ConflictHookDecision, error) {
			observed = context
			return ConflictHookAcceptRemote, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	winner, err := registry.ResolveWithContext(ConflictHookContext{
		Space:     "orders",
		KeyDigest: digest,
		Local:     local,
		Remote:    remote,
	})
	if err != nil {
		t.Fatalf("ResolveWithContext() error = %v", err)
	}
	if winner != remote {
		t.Fatalf("winner = %#v, want remote %#v", winner, remote)
	}
	if observed.Space != "orders" || observed.KeyDigest != digest || observed.Local != local || observed.Remote != remote {
		t.Fatalf("hook context = %#v, want space/digest/versions", observed)
	}
}

func TestT210ConflictHookCanKeepLocalOrReject(t *testing.T) {
	local := ConflictVersion{Timestamp: 1, NodeID: "region-a", Sequence: 8}
	remote := ConflictVersion{Timestamp: 2, NodeID: "region-b", Sequence: 9}
	for _, test := range []struct {
		name     string
		decision ConflictHookDecision
		want     ConflictVersion
		wantErr  error
	}{
		{name: "keep-local", decision: ConflictHookKeepLocal, want: local},
		{name: "reject", decision: ConflictHookReject, wantErr: ErrConflictHookRejected},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry, err := NewConflictPolicyRegistry(ConflictPolicy{
				Hook: func(ConflictHookContext) (ConflictHookDecision, error) {
					return test.decision, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			winner, err := registry.ResolveWithContext(ConflictHookContext{
				Space:  "orders",
				Local:  local,
				Remote: remote,
			})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			if test.wantErr == nil && winner != test.want {
				t.Fatalf("winner = %#v, want %#v", winner, test.want)
			}
		})
	}
}

func TestT210ConflictHookIsNotCalledForEqualVersions(t *testing.T) {
	calls := 0
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{
		Hook: func(ConflictHookContext) (ConflictHookDecision, error) {
			calls++
			return ConflictHookReject, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	version := ConflictVersion{Timestamp: 1, NodeID: "region-a", Sequence: 8}
	winner, err := registry.ResolveWithContext(ConflictHookContext{Space: "orders", Local: version, Remote: version})
	if err != nil {
		t.Fatalf("ResolveWithContext() error = %v", err)
	}
	if winner != version || calls != 0 {
		t.Fatalf("winner/calls = %#v/%d, want equal/0", winner, calls)
	}
}

func TestT210ConflictHookErrorsAndInvalidDecisionsAreReturned(t *testing.T) {
	sentinel := errors.New("hook failed")
	local := ConflictVersion{Timestamp: 1, NodeID: "region-a", Sequence: 1}
	remote := ConflictVersion{Timestamp: 2, NodeID: "region-b", Sequence: 2}
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{
		Hook: func(ConflictHookContext) (ConflictHookDecision, error) {
			return ConflictHookUsePolicy, sentinel
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ResolveWithContext(ConflictHookContext{Space: "orders", Local: local, Remote: remote}); !errors.Is(err, sentinel) {
		t.Fatalf("hook error = %v, want %v", err, sentinel)
	}

	registry, err = NewConflictPolicyRegistry(ConflictPolicy{
		Hook: func(ConflictHookContext) (ConflictHookDecision, error) {
			return ConflictHookDecision(99), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve("orders", local, remote); !errors.Is(err, ErrConflictHookDecision) {
		t.Fatalf("invalid hook decision error = %v, want ErrConflictHookDecision", err)
	}
}
