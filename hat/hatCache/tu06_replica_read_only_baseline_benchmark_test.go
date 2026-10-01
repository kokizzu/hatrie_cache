package hatCache

import "testing"

var tu06BaselineResponse CacheCommandResponse

func BenchmarkTU06BeforeExecuteCommandWrite(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	request := CacheCommandRequest{Command: "SET", Key: "tu06-benchmark", Value: "value"}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		request.Value = "value"
		tu06BaselineResponse = trie.ExecuteCommand(request)
	}
}
