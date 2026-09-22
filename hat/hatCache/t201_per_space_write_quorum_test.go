package hatCache

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"hatrie_cache/hat/hatPartition"
	"hatrie_cache/hat/hatReplication"
)

func TestWriteQuorumPolicySelectsLongestKeyPrefix(t *testing.T) {
	policy := &WriteQuorumPolicy{Rules: []WriteQuorumRule{
		{KeyPrefix: "critical:", Quorum: 2},
		{KeyPrefix: "critical:billing:", Quorum: 3},
	}}
	tests := []struct {
		name   string
		key    string
		quorum int
	}{
		{name: "specific space", key: "critical:billing:invoice", quorum: 3},
		{name: "parent space", key: "critical:profile", quorum: 2},
		{name: "unconfigured space", key: "ordinary:profile", quorum: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			quorum, err := policy.RequiredForKey(test.key)
			if err != nil {
				t.Fatalf("RequiredForKey() error = %v", err)
			}
			if quorum != test.quorum {
				t.Fatalf("RequiredForKey(%q) = %d, want %d", test.key, quorum, test.quorum)
			}
		})
	}
}

func TestExecuteCacheCommandAppliesPerSpaceWriteQuorum(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer target.Close()
	replicator := newCommandQuorumTestReplicator(t, target)
	policy := &WriteQuorumPolicy{Rules: []WriteQuorumRule{{KeyPrefix: "critical:", Quorum: 2}}}

	critical := CacheCommandRequest{Command: "SETSTR", Key: "critical:profile", Value: "value"}
	response, rejected := executeCacheCommand(context.Background(), newTestTrie(t), critical, commandExecutionOptions{
		Replicator:        replicator,
		WriteQuorumPolicy: policy,
	})
	if !rejected || response.OK {
		t.Fatalf("critical command rejected = %t, response = %#v; want quorum rejection", rejected, response)
	}
	if !strings.Contains(response.Message, hatReplication.ErrWriteQuorumUnsatisfied.Error()) {
		t.Fatalf("critical response message = %q, want write quorum error", response.Message)
	}

	ordinary := CacheCommandRequest{Command: "SETSTR", Key: "ordinary:profile", Value: "value"}
	response, rejected = executeCacheCommand(context.Background(), newTestTrie(t), ordinary, commandExecutionOptions{
		Replicator:        replicator,
		WriteQuorumPolicy: policy,
	})
	if rejected || !response.OK {
		t.Fatalf("ordinary command rejected = %t, response = %#v; want legacy asynchronous success", rejected, response)
	}
}

func TestExecuteAtomicBatchAppliesHighestPerSpaceWriteQuorum(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer target.Close()
	replicator := newCommandQuorumTestReplicator(t, target)
	policy := &WriteQuorumPolicy{Rules: []WriteQuorumRule{{KeyPrefix: "critical:", Quorum: 2}}}
	request := CacheCommandRequest{
		Command: "BATCH",
		Atomic:  true,
		Batch: []CacheCommandRequest{
			{Command: "SETSTR", Key: "ordinary:profile", Value: "ordinary"},
			{Command: "SETSTR", Key: "critical:profile", Value: "critical"},
		},
	}
	response, rejected := executeCacheCommand(context.Background(), newTestTrie(t), request, commandExecutionOptions{
		Replicator:        replicator,
		WriteQuorumPolicy: policy,
	})
	if !rejected || response.OK {
		t.Fatalf("atomic batch rejected = %t, response = %#v; want quorum rejection", rejected, response)
	}
	if !strings.Contains(response.Message, hatReplication.ErrWriteQuorumUnsatisfied.Error()) {
		t.Fatalf("atomic batch response message = %q, want write quorum error", response.Message)
	}
}

func TestPartitionedBatchDoesNotBypassPerSpaceWriteQuorum(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(2); err != nil {
		t.Fatal(err)
	}
	criticalKey := "critical:profile"
	criticalPartition := hatPartition.Index(criticalKey, 2)
	ordinaryKey := ""
	for index := 0; index < 128; index++ {
		candidate := fmt.Sprintf("ordinary:%d", index)
		if hatPartition.Index(candidate, 2) != criticalPartition {
			ordinaryKey = candidate
			break
		}
	}
	if ordinaryKey == "" {
		t.Fatal("could not find a key in the other local partition")
	}
	response, rejected := executeCacheCommand(context.Background(), trie, CacheCommandRequest{
		Command: "BATCH",
		Batch: []CacheCommandRequest{
			{Command: "SETSTR", Key: ordinaryKey, Value: "ordinary"},
			{Command: "SETSTR", Key: criticalKey, Value: "critical"},
		},
	}, commandExecutionOptions{
		WriteQuorumPolicy: &WriteQuorumPolicy{Rules: []WriteQuorumRule{{KeyPrefix: "critical:", Quorum: 2}}},
	})
	if rejected || response.OK {
		t.Fatalf("partitioned batch rejected = %t, response = %#v; want critical quorum configuration error", rejected, response)
	}
}

func TestWriteQuorumPolicyRejectsInvalidRules(t *testing.T) {
	tests := []struct {
		name   string
		policy *WriteQuorumPolicy
	}{
		{name: "empty prefix", policy: &WriteQuorumPolicy{Rules: []WriteQuorumRule{{Quorum: 2}}}},
		{name: "zero quorum", policy: &WriteQuorumPolicy{Rules: []WriteQuorumRule{{KeyPrefix: "critical:", Quorum: 0}}}},
		{name: "negative quorum", policy: &WriteQuorumPolicy{Rules: []WriteQuorumRule{{KeyPrefix: "critical:", Quorum: -1}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.policy.RequiredForKey("critical:key"); err == nil {
				t.Fatal("RequiredForKey() error = nil, want invalid policy error")
			}
		})
	}
}
