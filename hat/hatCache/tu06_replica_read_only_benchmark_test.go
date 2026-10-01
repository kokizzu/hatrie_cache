package hatCache

import "testing"

func BenchmarkTU06ReplicaReadOnlyOffUpsertStringChecked(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := trie.UpsertStringChecked("tu06:benchmark", "value"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU06ReplicaReadOnlyOffExecuteSet(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		response := trie.ExecuteCommand(CacheCommandRequest{Command: "SET", Key: "tu06:benchmark", Value: "value"})
		if !response.OK {
			b.Fatal(response.Message)
		}
	}
}
