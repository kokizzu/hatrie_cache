package hatCache

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTU07PrometheusReplicationRegionStatus(t *testing.T) {
	topology, err := NewTopologyStore(ClusterTopology{
		Version: clusterTopologyVersion,
		Mode:    TopologyModeFullReplica,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a", Region: "asia"},
			{ID: "node-b", Address: "http://node-b", Region: "europe"},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		ReplicationRegionPolicy: ReplicationRegionPolicy{
			LocalRegion:           "asia",
			RequiredRemoteRegions: []string{"europe", "us"},
			MaxRPOLagSequences:    2,
			MaxRTO:                5 * time.Second,
		},
	})
	t.Cleanup(replicator.Close)
	replicator.mu.Lock()
	replicator.queueStats.SourceSequence = 100
	replicator.queueStats.LastAcknowledgedSequenceByTarget = map[string]uint64{"node-b": 98}
	replicator.refreshReplicationLagLocked()
	replicator.mu.Unlock()

	trie := CreateHatTrie()
	t.Cleanup(trie.Destroy)
	handler := NewMonitoringHandler(trie, MonitoringOptions{
		NodeName:   "node-a",
		Replicator: replicator,
	})
	response := httptest.NewRecorder()
	handler.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("metrics response status = %d, want %d", response.Code, http.StatusOK)
	}
	metrics := response.Body.String()
	for _, want := range []string{
		"hatrie_cache_replication_rpo_configured{node=\"node-a\"} 1",
		"hatrie_cache_replication_rpo_configuration_error{node=\"node-a\"} 0",
		"hatrie_cache_replication_rpo_within_budget{node=\"node-a\"} 0",
		"hatrie_cache_replication_rpo_current_max_lag_sequences{node=\"node-a\"} 2",
		"hatrie_cache_replication_rpo_max_lag_sequences{node=\"node-a\"} 2",
		"hatrie_cache_replication_rpo_required_remote_regions{node=\"node-a\"} 2",
		"hatrie_cache_replication_rpo_available_remote_regions{node=\"node-a\"} 1",
		"hatrie_cache_replication_rpo_missing_remote_regions{node=\"node-a\"} 1",
		"hatrie_cache_replication_rto_max_millis{node=\"node-a\"} 5000",
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("prometheus metrics missing %q:\n%s", want, metrics)
		}
	}
}

func TestTU07PrometheusReplicationRegionStatusDisabledByDefault(t *testing.T) {
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a"})
	t.Cleanup(replicator.Close)
	trie := CreateHatTrie()
	t.Cleanup(trie.Destroy)
	handler := NewMonitoringHandler(trie, MonitoringOptions{
		NodeName:   "node-a",
		Replicator: replicator,
	})
	if metrics := handler.prometheusMetrics(); strings.Contains(metrics, "hatrie_cache_replication_rpo_") {
		t.Fatalf("default metrics unexpectedly contain regional RPO gauges:\n%s", metrics)
	}
}

func TestTU07PrometheusReplicationRegionStatusReportsConfigurationError(t *testing.T) {
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self: "node-a",
		ReplicationRegionPolicy: ReplicationRegionPolicy{
			LocalRegion:           "asia",
			RequiredRemoteRegions: []string{"asia"},
		},
	})
	t.Cleanup(replicator.Close)
	trie := CreateHatTrie()
	t.Cleanup(trie.Destroy)
	handler := NewMonitoringHandler(trie, MonitoringOptions{
		NodeName:   "node-a",
		Replicator: replicator,
	})
	metrics := handler.prometheusMetrics()
	for _, want := range []string{
		"hatrie_cache_replication_rpo_configured{node=\"node-a\"} 0",
		"hatrie_cache_replication_rpo_configuration_error{node=\"node-a\"} 1",
		"hatrie_cache_replication_rpo_within_budget{node=\"node-a\"} 0",
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("prometheus metrics missing invalid-policy gauge %q:\n%s", want, metrics)
		}
	}
}

func BenchmarkTU07MonitoringMetricsWithRegionalReplication(b *testing.B) {
	topology, err := NewTopologyStore(ClusterTopology{
		Version: clusterTopologyVersion,
		Mode:    TopologyModeFullReplica,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a", Region: "asia"},
			{ID: "node-b", Address: "http://node-b", Region: "europe"},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		ReplicationRegionPolicy: ReplicationRegionPolicy{
			LocalRegion:           "asia",
			RequiredRemoteRegions: []string{"europe"},
			MaxRPOLagSequences:    2,
		},
	})
	b.Cleanup(replicator.Close)
	replicator.mu.Lock()
	replicator.queueStats.SourceSequence = 100
	replicator.queueStats.LastAcknowledgedSequenceByTarget = map[string]uint64{"node-b": 98}
	replicator.refreshReplicationLagLocked()
	replicator.mu.Unlock()
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	handler := NewMonitoringHandler(trie, MonitoringOptions{
		NodeName:   "node-a",
		Replicator: replicator,
	})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = handler.prometheusMetrics()
	}
}
