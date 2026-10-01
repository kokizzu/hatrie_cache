package hatCache

import (
	"context"
	"strings"
	"testing"
)

func TestTU06ReplicaReadOnlyDefaultsOff(t *testing.T) {
	trie := CreateHatTrie()
	t.Cleanup(trie.Destroy)

	if trie.MaintenanceReadOnly() {
		t.Fatal("new trie must be writable by default")
	}
	response := trie.ExecuteCommand(CacheCommandRequest{Command: "SET", Key: "tu06-default", Value: "ok"})
	if !response.OK {
		t.Fatalf("default SET failed: %#v", response)
	}
}

func TestTU06ReplicaReadOnlyRejectsPublicWritesAndPreservesReads(t *testing.T) {
	trie := CreateHatTrie()
	t.Cleanup(trie.Destroy)

	if response := trie.ExecuteCommand(CacheCommandRequest{Command: "SET", Key: "tu06-key", Value: "before"}); !response.OK {
		t.Fatalf("seed SET failed: %#v", response)
	}
	trie.SetMaintenanceReadOnly(true)
	if !trie.MaintenanceReadOnly() {
		t.Fatal("maintenance read-only state was not enabled")
	}

	response := trie.ExecuteCommand(CacheCommandRequest{Command: "SET", Key: "tu06-key", Value: "after"})
	if response.OK || response.Message != maintenanceReadOnlyMessage {
		t.Fatalf("public write was not rejected with the stable error: %#v", response)
	}
	response = trie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "tu06-key"})
	if !response.OK || response.Value != "before" {
		t.Fatalf("read was not preserved: %#v", response)
	}

	response = trie.ExecuteCommand(CacheCommandRequest{Command: "INTERNALDEL", Key: "tu06-key"})
	if !response.OK {
		t.Fatalf("internal replication delete should remain allowed: %#v", response)
	}
	response = trie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "tu06-key"})
	if !response.OK || response.Message != "key not found" {
		t.Fatalf("internal replication delete did not apply: %#v", response)
	}
}

func TestTU06ReplicaReadOnlyRejectsPublicBatchBeforeMutation(t *testing.T) {
	trie := CreateHatTrie()
	t.Cleanup(trie.Destroy)

	trie.SetMaintenanceReadOnly(true)
	response := trie.ExecuteCommand(CacheCommandRequest{
		Command: "BATCH",
		Atomic:  true,
		Batch: []CacheCommandRequest{
			{Command: "SET", Key: "tu06-batch", Value: "blocked"},
		},
	})
	if response.OK || response.Message != maintenanceReadOnlyMessage {
		t.Fatalf("public batch was not rejected: %#v", response)
	}
	response = trie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "tu06-batch"})
	if !response.OK || response.Message != "key not found" {
		t.Fatalf("rejected batch mutated the trie: %#v", response)
	}
}

func TestTU06ReplicaReadOnlyCanBeDisabled(t *testing.T) {
	trie := CreateHatTrie()
	t.Cleanup(trie.Destroy)

	trie.SetMaintenanceReadOnly(true)
	trie.SetMaintenanceReadOnly(false)
	if trie.MaintenanceReadOnly() {
		t.Fatal("maintenance read-only state was not disabled")
	}
	response := trie.ExecuteCommand(CacheCommandRequest{Command: "SET", Key: "tu06-reenable", Value: "ok"})
	if !response.OK {
		t.Fatalf("write after disabling maintenance read-only failed: %#v", response)
	}
}

func TestTU06ReplicaReadOnlyRejectsSQLMutationAndTransactionCommit(t *testing.T) {
	trie := CreateHatTrie()
	t.Cleanup(trie.Destroy)
	trie.SetMaintenanceReadOnly(true)

	result, err := ExecuteSQLMutation(context.Background(), trie, "INSERT INTO CACHE (key, value) VALUES ('tu06-sql', 'value')", nil, SQLQueryOptions{})
	if err == nil || !strings.Contains(err.Error(), maintenanceReadOnlyMessage) || result.Response.OK {
		t.Fatalf("SQL mutation was not rejected: result=%#v err=%v", result, err)
	}

	transaction, err := BeginSQLTransaction(trie)
	if err != nil {
		t.Fatalf("BeginSQLTransaction() error = %v", err)
	}
	if _, err := transaction.Execute("INSERT INTO CACHE (key, value) VALUES ('tu06-transaction', 'value')"); err != nil {
		t.Fatalf("staging a transaction mutation should remain possible: %v", err)
	}
	if err := transaction.Commit(); err == nil || !strings.Contains(err.Error(), maintenanceReadOnlyMessage) {
		t.Fatalf("read-only transaction commit error = %v, want maintenance read-only", err)
	}
	response := trie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "tu06-sql"})
	if !response.OK || response.Message != "key not found" {
		t.Fatalf("rejected SQL mutation changed the trie: %#v", response)
	}
}

func TestTU06ReplicaReadOnlyOptionsEnableTrieGate(t *testing.T) {
	trie := CreateHatTrie()
	t.Cleanup(trie.Destroy)
	NewMonitoringHandler(trie, MonitoringOptions{MaintenanceReadOnly: true})
	if !trie.MaintenanceReadOnly() {
		t.Fatal("monitoring maintenance-read-only option did not enable trie gate")
	}

	other := CreateHatTrie()
	t.Cleanup(other.Destroy)
	NewCacheGRPCServer(other, CacheGRPCOptions{MaintenanceReadOnly: true})
	if !other.MaintenanceReadOnly() {
		t.Fatal("gRPC maintenance-read-only option did not enable trie gate")
	}
}
