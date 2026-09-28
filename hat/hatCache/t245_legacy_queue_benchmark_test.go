package hatCache

import "testing"

func BenchmarkT245LegacyPriorityQueuePushPop(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := trie.PushPriorityQueueChecked("jobs", 1, "job"); err != nil {
			b.Fatal(err)
		}
		if _, ok, err := trie.PopPriorityQueueChecked("jobs"); err != nil || !ok {
			b.Fatalf("PopPriorityQueueChecked() = %v/%v", ok, err)
		}
	}
}
