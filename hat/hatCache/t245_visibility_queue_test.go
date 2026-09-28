package hatCache

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestT245PriorityQueueVisibilityClaimAndAck(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	if _, err := trie.PushPriorityQueueChecked("jobs", 1, "job-1"); err != nil {
		t.Fatalf("PushPriorityQueueChecked() error = %v", err)
	}

	lease, ok, err := trie.ClaimPriorityQueueChecked("jobs", time.Second)
	if err != nil || !ok {
		t.Fatalf("ClaimPriorityQueueChecked() = %#v/%v/%v, want lease", lease, ok, err)
	}
	if lease.Token == "" || lease.Item.Value != "job-1" {
		t.Fatalf("lease = %#v, want token and job-1", lease)
	}
	if _, visible, err := trie.PeekPriorityQueueChecked("jobs"); err != nil || visible {
		t.Fatalf("PeekPriorityQueueChecked() = visible %v/error %v, want no visible item", visible, err)
	}

	acked, err := trie.AckPriorityQueueChecked("jobs", lease.Token)
	if err != nil || !acked {
		t.Fatalf("AckPriorityQueueChecked() = %v/%v, want true/nil", acked, err)
	}
	if acked, err := trie.AckPriorityQueueChecked("jobs", lease.Token); err != nil || acked {
		t.Fatalf("second AckPriorityQueueChecked() = %v/%v, want false/nil", acked, err)
	}
}

func TestT245PriorityQueueVisibilityExpiryRequeuesItem(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	now := time.Unix(100, 0)
	trie.now = func() time.Time { return now }
	if _, err := trie.PushPriorityQueueChecked("jobs", 1, "job-1"); err != nil {
		t.Fatalf("PushPriorityQueueChecked() error = %v", err)
	}

	lease, ok, err := trie.ClaimPriorityQueueChecked("jobs", time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("ClaimPriorityQueueChecked() = %#v/%v/%v, want lease", lease, ok, err)
	}
	now = now.Add(2 * time.Millisecond)

	item, ok, err := trie.PopPriorityQueueChecked("jobs")
	if err != nil || !ok || item.Value != "job-1" {
		t.Fatalf("PopPriorityQueueChecked() = %#v/%v/%v, want expired job-1", item, ok, err)
	}
	if acked, err := trie.AckPriorityQueueChecked("jobs", lease.Token); err != nil || acked {
		t.Fatalf("AckPriorityQueueChecked(expired) = %v/%v, want false/nil", acked, err)
	}
}

func TestT245PriorityQueueLegacyPopRemainsDestructive(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	if _, err := trie.PushPriorityQueueChecked("jobs", 1, "job-1"); err != nil {
		t.Fatalf("PushPriorityQueueChecked() error = %v", err)
	}
	item, ok, err := trie.PopPriorityQueueChecked("jobs")
	if err != nil || !ok || item.Value != "job-1" {
		t.Fatalf("PopPriorityQueueChecked() = %#v/%v/%v, want job-1", item, ok, err)
	}
	if _, ok, err := trie.PopPriorityQueueChecked("jobs"); err != nil || ok {
		t.Fatalf("second PopPriorityQueueChecked() = %v/%v, want false/nil", ok, err)
	}
}

func TestT245PriorityQueueVisibilityCommands(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	priority := int64(1)
	if response := trie.ExecuteCommand(CacheCommandRequest{
		Command:  "PUSHPQ",
		Key:      "jobs",
		Value:    "job-1",
		Priority: &priority,
	}); !response.OK {
		t.Fatalf("PUSHPQ response = %#v", response)
	}
	seconds := int64(60)
	claim := trie.ExecuteCommand(CacheCommandRequest{
		Command:    "CLAIMPQ",
		Key:        "jobs",
		TTLSeconds: &seconds,
	})
	if !claim.OK {
		t.Fatalf("CLAIMPQ response = %#v", claim)
	}
	var lease struct {
		Token string `json:"token"`
		Item  struct {
			Value interface{} `json:"value"`
		} `json:"item"`
	}
	if err := json.Unmarshal([]byte(claim.Value), &lease); err != nil {
		t.Fatalf("CLAIMPQ value %q: %v", claim.Value, err)
	}
	if lease.Token == "" || lease.Item.Value != "job-1" {
		t.Fatalf("CLAIMPQ lease = %#v, want token/job-1", lease)
	}
	ack := trie.ExecuteCommand(CacheCommandRequest{Command: "ACKPQ", Key: "jobs", Value: lease.Token})
	if !ack.OK || ack.Value != "1" {
		t.Fatalf("ACKPQ response = %#v, want acknowledged/1", ack)
	}
	if invalid := trie.ExecuteCommand(CacheCommandRequest{Command: "CLAIMPQ", Key: "jobs", TTLSeconds: new(int64)}); invalid.OK {
		t.Fatalf("CLAIMPQ zero TTL response = %#v, want error", invalid)
	}
}

func TestT245PriorityQueueSnapshotRetainsClaimedItemForRecovery(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	if _, err := trie.PushPriorityQueueChecked("jobs", 1, "job-1"); err != nil {
		t.Fatalf("PushPriorityQueueChecked() error = %v", err)
	}
	if _, ok, err := trie.ClaimPriorityQueueChecked("jobs", time.Minute); err != nil || !ok {
		t.Fatalf("ClaimPriorityQueueChecked() = %v/%v, want lease", ok, err)
	}
	hval := trie.Get("jobs")
	items := trie.priorityQueues.array[hval.Index].SnapshotItems()
	if len(items) != 1 || items[0].value() != "job-1" {
		t.Fatalf("SnapshotItems() = %#v, want claimed job-1", items)
	}
	restored := newPriorityQueueDataFromItems(items)
	item, ok := restored.Pop()
	if !ok || item.Value != "job-1" {
		t.Fatalf("restored priority queue pop = %#v/%v, want job-1", item, ok)
	}
}

func TestT245PriorityQueueBackupRestoreMakesClaimedItemAvailable(t *testing.T) {
	source := CreateHatTrie()
	defer source.Destroy()
	if _, err := source.PushPriorityQueueChecked("jobs", 1, "job-1"); err != nil {
		t.Fatalf("PushPriorityQueueChecked() error = %v", err)
	}
	lease, ok, err := source.ClaimPriorityQueueChecked("jobs", time.Minute)
	if err != nil || !ok {
		t.Fatalf("ClaimPriorityQueueChecked() = %#v/%v/%v, want lease", lease, ok, err)
	}

	snapshot := filepath.Join(t.TempDir(), "jobs.snapshot")
	if err := source.SaveSnapshot(snapshot); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	restored := CreateHatTrie()
	defer restored.Destroy()
	if err := restored.LoadSnapshot(snapshot); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	item, ok, err := restored.PopPriorityQueueChecked("jobs")
	if err != nil || !ok || item.Value != "job-1" {
		t.Fatalf("restored PopPriorityQueueChecked() = %#v/%v/%v, want job-1", item, ok, err)
	}
	if acked, err := restored.AckPriorityQueueChecked("jobs", lease.Token); err != nil || acked {
		t.Fatalf("restored AckPriorityQueueChecked() = %v/%v, want false/nil", acked, err)
	}
}
