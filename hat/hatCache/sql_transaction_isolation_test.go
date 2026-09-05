package hatCache

import "testing"

func TestBeginSQLTransactionWithOptionsSupportsSerializableIsolation(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	transaction, err := BeginSQLTransactionWithOptions(trie, SQLTransactionOptions{
		Isolation: SQLTransactionIsolationSerializable,
	})
	if err != nil {
		t.Fatalf("BeginSQLTransactionWithOptions() error = %v", err)
	}
	if transaction.Isolation() != SQLTransactionIsolationSerializable {
		t.Fatalf("Isolation() = %v, want serializable", transaction.Isolation())
	}
	if trie.commandTransactionMu.TryRLock() {
		trie.commandTransactionMu.RUnlock()
		t.Fatal("serializable transaction did not hold the command read lock")
	}
	if err := transaction.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if !trie.commandTransactionMu.TryRLock() {
		t.Fatal("serializable transaction did not release the command lock")
	}
	trie.commandTransactionMu.RUnlock()
}

func TestSerializableTransactionRejectsConcurrentTypedMutation(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	transaction, err := BeginSQLTransactionWithOptions(trie, SQLTransactionOptions{
		Isolation: SQLTransactionIsolationSerializable,
	})
	if err != nil {
		t.Fatalf("BeginSQLTransactionWithOptions() error = %v", err)
	}
	if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('tx_key', 'tx-value')"); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	trie.UpsertString("direct_key", "direct-value")
	if err := transaction.Commit(); err == nil {
		t.Fatal("Commit() accepted a concurrent typed mutation")
	}
	got := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "tx_key"})
	if !got.OK || got.Message != "key not found" {
		t.Fatalf("rolled-back transaction key response = %+v", got)
	}
	direct := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "direct_key"})
	if !direct.OK || direct.Value != "direct-value" {
		t.Fatalf("direct mutation response = %+v", direct)
	}
}

func TestBeginSQLTransactionDefaultRemainsOptimisticSnapshot(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	transaction, err := BeginSQLTransaction(trie)
	if err != nil {
		t.Fatalf("BeginSQLTransaction() error = %v", err)
	}
	defer transaction.Rollback()
	if transaction.Isolation() != SQLTransactionIsolationSnapshot {
		t.Fatalf("Isolation() = %v, want snapshot", transaction.Isolation())
	}
	if !trie.commandTransactionMu.TryRLock() {
		t.Fatal("default snapshot transaction unexpectedly held the command lock")
	}
	trie.commandTransactionMu.RUnlock()
}

func TestParseSQLTransactionIsolation(t *testing.T) {
	tests := map[string]SQLTransactionIsolation{
		"snapshot":     SQLTransactionIsolationSnapshot,
		"serializable": SQLTransactionIsolationSerializable,
		"":             SQLTransactionIsolationSnapshot,
	}
	for input, want := range tests {
		got, err := ParseSQLTransactionIsolation(input)
		if err != nil || got != want {
			t.Fatalf("ParseSQLTransactionIsolation(%q) = %v, %v; want %v", input, got, err, want)
		}
	}
	if _, err := ParseSQLTransactionIsolation("read-uncommitted"); err == nil {
		t.Fatal("ParseSQLTransactionIsolation() accepted unsupported isolation")
	}
}
