package hatCache

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestTU06ReplicaReadOnlyGuardsSQLMutationAndPendingTransaction(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	if err := trie.SetReplicaReadOnly(true); err != nil {
		t.Fatal(err)
	}
	if _, err := ExecuteSQLMutation(context.Background(), trie, "this is intentionally not SQL", nil, SQLQueryOptions{}); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("ExecuteSQLMutation error = %v, want ErrReplicaReadOnly", err)
	}

	if err := trie.SetReplicaReadOnly(false); err != nil {
		t.Fatal(err)
	}
	transaction, err := BeginSQLTransaction(trie)
	if err != nil {
		t.Fatal(err)
	}
	transaction.staged = []CacheCommandRequest{{Command: "SET", Key: "txn", Value: "new"}}
	if err := trie.SetReplicaReadOnly(true); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("transaction.Commit error = %v, want ErrReplicaReadOnly", err)
	}
	if got := trie.GetString("txn"); got != "" {
		t.Fatalf("blocked transaction changed live cache: %q", got)
	}
	if err := trie.SetReplicaReadOnly(false); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := trie.GetString("txn"); got != "new" {
		t.Fatalf("committed transaction value = %q, want new", got)
	}
}

func TestTU06ReplicaReadOnlyGuardsInPlaceTypedMutations(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	trie.PushSlice("slice", "one")
	trie.AddSet("set", "one")
	trie.PushPriorityQueue("queue", 1, "one")
	trie.PutRadixTree("radix", "sub", "one")
	trie.AddRoaringBitmap("roaring", 1)
	trie.AddSparseBitset("sparse", 1)
	if err := trie.UpsertCuckooFilter("cuckoo", 32, 0.01); err != nil {
		t.Fatal(err)
	}
	trie.AddCuckooFilter("cuckoo", "one")

	if err := trie.SetReplicaReadOnly(true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := trie.PopSliceChecked("slice"); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("PopSliceChecked error = %v, want ErrReplicaReadOnly", err)
	}
	if _, err := trie.RemoveSetChecked("set", "one"); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("RemoveSetChecked error = %v, want ErrReplicaReadOnly", err)
	}
	if _, _, err := trie.PopPriorityQueueChecked("queue"); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("PopPriorityQueueChecked error = %v, want ErrReplicaReadOnly", err)
	}
	if _, err := trie.DeleteRadixTreeChecked("radix", "sub"); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("DeleteRadixTreeChecked error = %v, want ErrReplicaReadOnly", err)
	}
	if _, err := trie.RemoveRoaringBitmapChecked("roaring", 1); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("RemoveRoaringBitmapChecked error = %v, want ErrReplicaReadOnly", err)
	}
	if _, err := trie.RemoveSparseBitsetChecked("sparse", 1); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("RemoveSparseBitsetChecked error = %v, want ErrReplicaReadOnly", err)
	}
	if _, err := trie.DeleteCuckooFilterChecked("cuckoo", "one"); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("DeleteCuckooFilterChecked error = %v, want ErrReplicaReadOnly", err)
	}
}

func TestTU06ReplicaReadOnlyDefaultOffAndGuardsWrites(t *testing.T) {
	trie := newTestTrie(t)
	defer trie.Destroy()

	if trie.ReplicaReadOnly() {
		t.Fatal("replica read-only must default to false")
	}
	for _, setup := range []func() error{
		func() error { return trie.UpsertStringChecked("tu06:string", "before") },
		func() error { return trie.UpsertCounterChecked("tu06:counter", 1) },
		func() error { return trie.UpsertBytesChecked("tu06:bytes", []byte("before")) },
		func() error { return trie.UpsertRoaringBitmapChecked("tu06:bitmap") },
	} {
		if err := setup(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := trie.AddRoaringBitmapChecked("tu06:bitmap", 1); err != nil {
		t.Fatal(err)
	}

	if err := trie.SetReplicaReadOnly(true); err != nil {
		t.Fatal(err)
	}
	if !trie.ReplicaReadOnly() {
		t.Fatal("replica read-only state was not enabled")
	}

	mutations := []struct {
		name string
		call func() error
	}{
		{name: "upsert string", call: func() error { return trie.UpsertStringChecked("tu06:string", "after") }},
		{name: "increment counter", call: func() error {
			_, err := trie.IncrementCounterChecked("tu06:counter", 1)
			return err
		}},
		{name: "upsert bytes", call: func() error { return trie.UpsertBytesChecked("tu06:bytes", []byte("after")) }},
		{name: "add roaring bitmap", call: func() error {
			_, err := trie.AddRoaringBitmapChecked("tu06:bitmap", 2)
			return err
		}},
		{name: "delete", call: func() error {
			_, err := trie.DeleteChecked("tu06:string")
			return err
		}},
		{name: "expire", call: func() error {
			_, err := trie.ExpireChecked("tu06:string", time.Minute)
			return err
		}},
		{name: "compare and swap", call: func() error {
			_, err := trie.CompareAndSwapString("tu06:string", "before", "after")
			return err
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			if err := mutation.call(); !errors.Is(err, ErrReplicaReadOnly) {
				t.Fatalf("error = %v, want %v", err, ErrReplicaReadOnly)
			}
		})
	}

	response := trie.ExecuteCommand(CacheCommandRequest{Command: "SET", Key: "tu06:string", Value: "after"})
	if response.OK || !strings.Contains(response.Message, ErrReplicaReadOnly.Error()) {
		t.Fatalf("read-only SET response = %#v", response)
	}
	response = trie.ExecuteCommand(CacheCommandRequest{
		Command: "BATCH",
		Atomic:  true,
		Batch:   []CacheCommandRequest{{Command: "SET", Key: "tu06:string", Value: "after"}},
	})
	if response.OK || !strings.Contains(response.Message, ErrReplicaReadOnly.Error()) {
		t.Fatalf("read-only BATCH response = %#v", response)
	}
	if value, ok, err := trie.GetStringChecked("tu06:string"); err != nil || !ok || value != "before" {
		t.Fatalf("string after rejected writes = %q/%v/%v", value, ok, err)
	}
	if value, ok, err := trie.GetCounterChecked("tu06:counter"); err != nil || !ok || value != 1 {
		t.Fatalf("counter after rejected writes = %d/%v/%v", value, ok, err)
	}
	if value, err := trie.GetBytesChecked("tu06:bytes"); err != nil || string(value) != "before" {
		t.Fatalf("bytes after rejected writes = %q/%v", value, err)
	}
	if contains, err := trie.HasRoaringBitmapChecked("tu06:bitmap", 2); err != nil || contains {
		t.Fatalf("bitmap after rejected write = %v/%v", contains, err)
	}

	atomicResponse, atomicErr := trie.RunAtomic(func(batch *AtomicCommandBatch) error {
		return batch.Add(CacheCommandRequest{Command: "SET", Key: "tu06:atomic", Value: "must-not-apply"})
	})
	if atomicErr == nil && atomicResponse.OK {
		t.Fatalf("read-only RunAtomic unexpectedly succeeded: %#v", atomicResponse)
	}
	if trie.Exists("tu06:atomic") {
		t.Fatal("read-only RunAtomic created a key")
	}

	readResponse := trie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "tu06:string"})
	if !readResponse.OK || readResponse.Value != "before" {
		t.Fatalf("read-only GET response = %#v", readResponse)
	}

	operation := snapshotOperation{entry: snapshotEntry{Key: "tu06:replicated", Type: "string", String: "replicated"}}
	replicationResponse := executePreparedInternalReplicationCommand(
		trie,
		CacheCommandRequest{Command: replicationSetCompactCommand, Key: "tu06:replicated"},
		&operation,
	)
	if !replicationResponse.OK {
		t.Fatalf("trusted replication response = %#v", replicationResponse)
	}
	if got := trie.GetString("tu06:replicated"); got != "replicated" {
		t.Fatalf("trusted replication value = %q", got)
	}

	if err := trie.SetReplicaReadOnly(false); err != nil {
		t.Fatal(err)
	}
	if err := trie.UpsertStringChecked("tu06:string", "after"); err != nil {
		t.Fatalf("write after disabling read-only = %v", err)
	}
}

func TestTU06ReplicaReadOnlyPropagatesToLocalPartitions(t *testing.T) {
	trie := newTestTrie(t)
	defer trie.Destroy()

	if err := trie.ConfigureLocalPartitions(2); err != nil {
		t.Fatal(err)
	}
	if err := trie.SetReplicaReadOnly(true); err != nil {
		t.Fatal(err)
	}
	if err := trie.UpsertStringChecked("tu06:partitioned", "must-not-apply"); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("partitioned write error = %v, want %v", err, ErrReplicaReadOnly)
	}
	if trie.Exists("tu06:partitioned") {
		t.Fatal("partitioned read-only write created a key")
	}
	if err := trie.SetReplicaReadOnly(false); err != nil {
		t.Fatal(err)
	}
	if err := trie.UpsertStringChecked("tu06:partitioned", "allowed"); err != nil {
		t.Fatal(err)
	}
}
