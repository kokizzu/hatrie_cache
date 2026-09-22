package hatCache

import "testing"

// BenchmarkT234SQLTransactionEarlyConflict measures the opt-in epoch check on
// staged mutations while keeping the default transaction path as a baseline.
func BenchmarkT234SQLTransactionEarlyConflict(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	b.ReportAllocs()

	for _, test := range []struct {
		name    string
		options SQLTransactionOptions
	}{
		{name: "default", options: SQLTransactionOptions{}},
		{name: "early_detection", options: SQLTransactionOptions{EarlyConflictDetection: true}},
	} {
		b.Run(test.name, func(b *testing.B) {
			transaction, err := BeginSQLTransactionWithOptions(trie, test.options)
			if err != nil {
				b.Fatal(err)
			}
			defer transaction.Rollback()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('benchmark_key', 'value')"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
