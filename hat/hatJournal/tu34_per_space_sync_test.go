package hatJournal

import (
	"errors"
	"testing"
	"time"
)

func TestTU34SpaceSyncPoliciesNormalizeAndCopy(t *testing.T) {
	input := map[string]SpaceSyncPolicy{
		" critical ": {Mode: SpaceSyncSynchronous},
		"bulk":       {Mode: SpaceSyncDisabled},
		"reports":    {Mode: SpaceSyncPeriodic, Interval: time.Second},
	}
	normalized, err := ValidateOptions(Options{
		GroupCommitMaxBatch: DefaultGroupCommitMaxBatch,
		SpaceSyncPolicies:   input,
	})
	if err != nil {
		t.Fatalf("ValidateOptions() error = %v", err)
	}
	if len(normalized.SpaceSyncPolicies) != len(input) {
		t.Fatalf("normalized policy count = %d, want %d", len(normalized.SpaceSyncPolicies), len(input))
	}
	if _, ok := normalized.SpaceSyncPolicies["critical"]; !ok {
		t.Fatal("normalized policies did not trim the critical space")
	}
	input["critical"] = SpaceSyncPolicy{Mode: SpaceSyncDisabled}
	if normalized.SpaceSyncPolicies["critical"].Mode != SpaceSyncSynchronous {
		t.Fatal("normalized policies alias the caller's map")
	}
}

func TestTU34SpaceSyncPoliciesRejectUnsafeShapes(t *testing.T) {
	tests := []struct {
		name     string
		policies map[string]SpaceSyncPolicy
		want     error
	}{
		{name: "empty space", policies: map[string]SpaceSyncPolicy{" ": {Mode: SpaceSyncDisabled}}, want: ErrSpaceSyncPolicyInvalid},
		{name: "unknown mode", policies: map[string]SpaceSyncPolicy{"bulk": {Mode: SpaceSyncMode("unknown")}}, want: ErrSpaceSyncPolicyInvalid},
		{name: "periodic without interval", policies: map[string]SpaceSyncPolicy{"reports": {Mode: SpaceSyncPeriodic}}, want: ErrSpaceSyncPolicyInvalid},
		{name: "interval on disabled", policies: map[string]SpaceSyncPolicy{"bulk": {Mode: SpaceSyncDisabled, Interval: time.Second}}, want: ErrSpaceSyncPolicyInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ValidateOptions(Options{GroupCommitMaxBatch: DefaultGroupCommitMaxBatch, SpaceSyncPolicies: test.policies})
			if !errors.Is(err, test.want) {
				t.Fatalf("ValidateOptions() error = %v, want %v", err, test.want)
			}
		})
	}
}
