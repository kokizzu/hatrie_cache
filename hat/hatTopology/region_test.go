package hatTopology

import "testing"

func TestTopologyRegionSurvivesNormalizationAndFingerprint(t *testing.T) {
	base := ClusterTopology{
		Version: Version,
		Mode:    TopologyModeFullReplica,
		Nodes:   []TopologyNode{{ID: "node-a", Address: "http://node-a", Region: " asia "}},
	}
	normalized, err := Normalize(base)
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if got := normalized.Nodes[0].Region; got != "asia" {
		t.Fatalf("normalized region = %q, want asia", got)
	}
	changed := base
	changed.Nodes = append([]TopologyNode(nil), base.Nodes...)
	changed.Nodes[0].Region = "europe"
	if base.Fingerprint() == changed.Fingerprint() {
		t.Fatal("topology fingerprint did not change after region change")
	}
}
