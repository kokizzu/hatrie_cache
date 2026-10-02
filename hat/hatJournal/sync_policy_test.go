package hatJournal

import (
	"strings"
	"testing"
	"time"
)

func TestSyncPolicyDefaultsToSynchronousAndUsesLongestSpacePrefix(t *testing.T) {
	options, err := ValidateOptions(Options{
		GroupCommitMaxBatch: 1,
		SyncPolicy: SyncPolicy{
			Rules: []SyncPolicyRule{
				{SpacePrefix: "region:", Mode: SyncModePeriodic},
				{SpacePrefix: "region:hot:", Mode: SyncModeDisabled},
			},
			PeriodicInterval: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("ValidateOptions() error = %v", err)
	}
	if options.SyncPolicy.Default != SyncModeSynchronous {
		t.Fatalf("default sync mode = %q, want %q", options.SyncPolicy.Default, SyncModeSynchronous)
	}
	if got := options.SyncPolicy.ModeForKey("region:hot:events"); got != SyncModeDisabled {
		t.Fatalf("hot space sync mode = %q, want %q", got, SyncModeDisabled)
	}
	if got := options.SyncPolicy.ModeForKey("region:cold:events"); got != SyncModePeriodic {
		t.Fatalf("cold space sync mode = %q, want %q", got, SyncModePeriodic)
	}
	if got := options.SyncPolicy.ModeForKey("other:events"); got != SyncModeSynchronous {
		t.Fatalf("unmatched sync mode = %q, want %q", got, SyncModeSynchronous)
	}
}

func TestValidateOptionsRejectsInvalidSyncPolicy(t *testing.T) {
	cases := []struct {
		name    string
		policy  SyncPolicy
		wantErr string
	}{
		{
			name:    "periodic interval required",
			policy:  SyncPolicy{Default: SyncModePeriodic},
			wantErr: "periodic sync interval",
		},
		{
			name: "duplicate prefix",
			policy: SyncPolicy{
				Rules: []SyncPolicyRule{
					{SpacePrefix: "same:", Mode: SyncModeDisabled},
					{SpacePrefix: "same:", Mode: SyncModeSynchronous},
				},
			},
			wantErr: "duplicate",
		},
		{
			name:    "empty prefix",
			policy:  SyncPolicy{Rules: []SyncPolicyRule{{Mode: SyncModeDisabled}}},
			wantErr: "space prefix",
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := ValidateOptions(Options{GroupCommitMaxBatch: 1, SyncPolicy: test.policy})
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("ValidateOptions() error = %v, want substring %q", err, test.wantErr)
			}
		})
	}
}
