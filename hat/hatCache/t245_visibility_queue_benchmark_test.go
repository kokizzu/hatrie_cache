package hatCache

import (
	"testing"
	"time"
)

func BenchmarkT245VisibilityClaimAck(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := trie.PushPriorityQueueChecked("jobs", 1, "job"); err != nil {
			b.Fatal(err)
		}
		lease, ok, err := trie.ClaimPriorityQueueChecked("jobs", time.Minute)
		if err != nil || !ok {
			b.Fatalf("ClaimPriorityQueueChecked() = %v/%v", ok, err)
		}
		if acked, err := trie.AckPriorityQueueChecked("jobs", lease.Token); err != nil || !acked {
			b.Fatalf("AckPriorityQueueChecked() = %v/%v", acked, err)
		}
	}
}
