package hatCache

import "testing"

func TestSnapshotRestoreWorkerConfiguration(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	if trie.snapshotRestoreWorkers != DefaultSnapshotRestoreWorkers {
		t.Fatalf("default snapshot restore workers = %d, want %d", trie.snapshotRestoreWorkers, DefaultSnapshotRestoreWorkers)
	}
	if err := trie.ConfigureSnapshotRestoreWorkers(-1); err == nil {
		t.Fatal("negative snapshot restore workers should fail")
	}
	if err := trie.ConfigureSnapshotRestoreWorkers(MaxLocalPartitions + 1); err == nil {
		t.Fatal("too many snapshot restore workers should fail")
	}
	if err := trie.ConfigureSnapshotRestoreWorkers(4); err != nil {
		t.Fatalf("configure snapshot restore workers: %v", err)
	}
	if trie.snapshotRestoreWorkers != 4 {
		t.Fatalf("configured snapshot restore workers = %d, want 4", trie.snapshotRestoreWorkers)
	}

	if err := trie.ConfigureLocalPartitions(8); err != nil {
		t.Fatalf("configure local partitions: %v", err)
	}
	set := trie.localPartitionSet()
	for index, child := range set.tries {
		if child.snapshotRestoreWorkers != 4 {
			t.Fatalf("partition %d snapshot restore workers = %d, want 4", index, child.snapshotRestoreWorkers)
		}
	}
	if err := trie.ConfigureSnapshotRestoreWorkers(2); err != nil {
		t.Fatalf("reconfigure snapshot restore workers: %v", err)
	}
	for index, child := range set.tries {
		if child.snapshotRestoreWorkers != 2 {
			t.Fatalf("partition %d updated snapshot restore workers = %d, want 2", index, child.snapshotRestoreWorkers)
		}
	}

	stage, err := newSnapshotRestoreStage(trie)
	if err != nil {
		t.Fatalf("create snapshot restore stage: %v", err)
	}
	defer stage.Destroy()
	if stage.snapshotRestoreWorkers != 2 {
		t.Fatalf("staged snapshot restore workers = %d, want 2", stage.snapshotRestoreWorkers)
	}
	stagedSet := stage.localPartitionSet()
	for index, child := range stagedSet.tries {
		if child.snapshotRestoreWorkers != 2 {
			t.Fatalf("staged partition %d snapshot restore workers = %d, want 2", index, child.snapshotRestoreWorkers)
		}
	}
}

func TestSnapshotRestoreWorkerCount(t *testing.T) {
	tests := []struct {
		name       string
		partitions int
		configured int
		want       int
	}{
		{name: "serial", partitions: 8, configured: 1, want: 1},
		{name: "bounded", partitions: 8, configured: 3, want: 3},
		{name: "clamped-to-partitions", partitions: 4, configured: 8, want: 4},
		{name: "empty-partition-set", partitions: 0, configured: 0, want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := localPartitionRestoreWorkerCount(test.partitions, test.configured); got != test.want {
				t.Fatalf("worker count = %d, want %d", got, test.want)
			}
		})
	}
}
