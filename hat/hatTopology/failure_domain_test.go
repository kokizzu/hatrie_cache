package hatTopology_test

import (
	"strings"
	"testing"

	hatriecache "hatrie_cache/hat/hatTopology"
)

func TestFailureDomainPlacementValidation(t *testing.T) {
	tests := []struct {
		name    string
		domains []string
		minimum int
		wantErr string
	}{
		{name: "disabled", domains: []string{"", ""}, minimum: 0},
		{name: "distinct", domains: []string{"zone-a", "zone-b"}, minimum: 2},
		{name: "duplicate", domains: []string{"zone-a", "zone-a"}, minimum: 2, wantErr: "distinct failure domains"},
		{name: "unknown", domains: []string{"zone-a", ""}, minimum: 2, wantErr: "failure domain is required"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			topology := failureDomainTestTopology(test.domains)
			normalized, err := hatriecache.Normalize(topology)
			if err != nil {
				t.Fatalf("Normalize() error = %v", err)
			}
			if err := hatriecache.ValidateFailureDomainPlacement(normalized, test.minimum); err != nil {
				if test.wantErr == "" {
					t.Fatalf("ValidateFailureDomainPlacement() error = %v, want nil", err)
				}
				if !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("ValidateFailureDomainPlacement() error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if test.wantErr != "" {
				t.Fatalf("ValidateFailureDomainPlacement() error = nil, want %q", test.wantErr)
			}
		})
	}
}

func failureDomainTestTopology(domains []string) hatriecache.ClusterTopology {
	return hatriecache.ClusterTopology{
		Version: hatriecache.Version,
		Mode:    hatriecache.TopologyModeSharded,
		Nodes: []hatriecache.TopologyNode{
			{ID: "node-a", FailureDomain: domains[0]},
			{ID: "node-b", FailureDomain: domains[1]},
		},
		Shards: []hatriecache.TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	}
}
