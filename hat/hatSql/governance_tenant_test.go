package hatSql

import (
	"testing"
	"time"
)

func TestNamespaceQueryGovernorAppliesTenantResourceLimits(t *testing.T) {
	governor, err := NewNamespaceQueryGovernor(
		NamespaceResourceLimits{
			MaxRows:           100,
			MaxJoinBytes:      1_000,
			MaxResultBytes:    2_000,
			MaxWorkers:        8,
			MaxSortBytes:      3_000,
			MaxGroupBytes:     4_000,
			MaxSetBytes:       5_000,
			MaxSpillBytes:     6_000,
			MaxRecursionDepth: 7,
			Timeout:           time.Minute,
		},
		map[string]NamespaceResourceLimits{
			"tenant-a": {
				MaxRows:           10,
				MaxJoinBytes:      100,
				MaxResultBytes:    200,
				MaxWorkers:        2,
				MaxSortBytes:      300,
				MaxGroupBytes:     400,
				MaxSetBytes:       500,
				MaxSpillBytes:     600,
				MaxRecursionDepth: 3,
				Timeout:           time.Second,
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	limits := governor.limitsFor("tenant-a")
	options := limits.Apply(SQLQueryOptions{
		MaxRows:           50,
		MaxJoinBytes:      200,
		MaxResultBytes:    400,
		Workers:           4,
		MaxSortBytes:      600,
		MaxGroupBytes:     800,
		MaxSetBytes:       1_000,
		MaxSpillBytes:     1_200,
		MaxRecursionDepth: 5,
		Timeout:           2 * time.Second,
	})
	want := SQLQueryOptions{
		MaxRows:           10,
		MaxJoinBytes:      100,
		MaxResultBytes:    200,
		Workers:           2,
		MaxSortBytes:      300,
		MaxGroupBytes:     400,
		MaxSetBytes:       500,
		MaxSpillBytes:     600,
		MaxRecursionDepth: 3,
		Timeout:           time.Second,
	}
	if options != want {
		t.Fatalf("tenant options = %#v, want %#v", options, want)
	}
	if got := governor.limitsFor("other").MaxRows; got != 100 {
		t.Fatalf("default MaxRows = %d, want 100", got)
	}
}
