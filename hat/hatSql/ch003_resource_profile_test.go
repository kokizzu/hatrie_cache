package hatSql

import (
	"errors"
	"testing"
)

func TestCH003DryRunReportsSoftWarningsAndHardCaps(t *testing.T) {
	governor, err := NewNamespaceQueryGovernorWithProfiles(
		NamespaceResourceProfile{
			Soft: NamespaceResourceLimits{MaxRows: 50, MaxJoinBytes: 500},
			Hard: NamespaceResourceLimits{MaxRows: 200, MaxJoinBytes: 2_000},
		},
		map[string]NamespaceResourceProfile{
			"tenant": {
				Soft: NamespaceResourceLimits{MaxRows: 40, MaxJoinBytes: 400},
				Hard: NamespaceResourceLimits{MaxRows: 100, MaxJoinBytes: 1_000},
			},
		},
	)
	if err != nil {
		t.Fatalf("NewNamespaceQueryGovernorWithProfiles() error = %v", err)
	}
	defer governor.Close()

	admission, err := governor.DryRun("tenant", SQLQueryOptions{MaxRows: 500, MaxJoinBytes: 2_000})
	if err != nil {
		t.Fatalf("DryRun() error = %v", err)
	}
	if admission.Namespace != "tenant" || admission.EffectiveOptions.MaxRows != 100 || admission.EffectiveOptions.MaxJoinBytes != 1_000 {
		t.Fatalf("admission = %#v", admission)
	}
	if !containsString(admission.HardClamps, "max_rows") || !containsString(admission.HardClamps, "max_join_bytes") {
		t.Fatalf("hard clamps = %#v", admission.HardClamps)
	}
	if !containsString(admission.SoftWarnings, "max_rows") || !containsString(admission.SoftWarnings, "max_join_bytes") {
		t.Fatalf("soft warnings = %#v", admission.SoftWarnings)
	}
	if len(governor.gates) != 0 || len(governor.quotas) != 0 {
		t.Fatalf("DryRun mutated admission state: gates=%d quotas=%d", len(governor.gates), len(governor.quotas))
	}
}

func TestCH003ProfilesPreserveLegacyGovernorAndValidateSoftBounds(t *testing.T) {
	legacy, err := NewNamespaceQueryGovernor(NamespaceResourceLimits{MaxRows: 100}, nil)
	if err != nil {
		t.Fatalf("NewNamespaceQueryGovernor() error = %v", err)
	}
	legacyAdmission, err := legacy.DryRun("legacy", SQLQueryOptions{MaxRows: 200})
	if err != nil {
		t.Fatalf("legacy DryRun() error = %v", err)
	}
	if legacyAdmission.EffectiveOptions.MaxRows != 100 || len(legacyAdmission.SoftWarnings) != 0 {
		t.Fatalf("legacy admission = %#v", legacyAdmission)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("legacy Close() error = %v", err)
	}
	if _, err := legacy.DryRun("legacy", SQLQueryOptions{}); !errors.Is(err, ErrNamespaceQueryGovernorClosed) {
		t.Fatalf("closed DryRun() error = %v", err)
	}

	governor, err := NewNamespaceQueryGovernorWithProfiles(NamespaceResourceProfile{
		Soft: NamespaceResourceLimits{MaxRows: 100},
		Hard: NamespaceResourceLimits{MaxRows: 50},
	}, nil)
	if err != nil {
		t.Fatalf("soft-over-hard profile error = %v", err)
	}
	defer governor.Close()
	admission, err := governor.DryRun("bounded", SQLQueryOptions{MaxRows: 75})
	if err != nil {
		t.Fatalf("bounded DryRun() error = %v", err)
	}
	if admission.SoftLimits.MaxRows != 50 || !containsString(admission.SoftWarnings, "max_rows") {
		t.Fatalf("soft-over-hard admission = %#v", admission)
	}

	if _, err := NewNamespaceQueryGovernorWithProfiles(NamespaceResourceProfile{
		Soft: NamespaceResourceLimits{MaxRows: -1},
	}, nil); err == nil {
		t.Fatal("negative soft resource limit was accepted")
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
