package hatCache

import "testing"

func BenchmarkCH060BitmapSecondaryCombination(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	trie.UpsertString("events", ch060BitmapSecondaryData(100_000))
	if err := trie.CreateSQLJSONBitmapIndex("events", "state"); err != nil {
		b.Fatal(err)
	}
	if err := trie.CreateSQLJSONBitmapIndex("events", "team"); err != nil {
		b.Fatal(err)
	}

	for _, test := range []struct {
		name      string
		operation string
	}{
		{name: "AND", operation: "AND"},
		{name: "OR", operation: "OR"},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				rows, available, err := trie.ResolveSQLSecondaryIndexedSource(
					"CACHE",
					"events",
					test.operation,
					[]string{"state", "team"},
					[]interface{}{"ready", "edge"},
				)
				if err != nil || !available || len(rows) == 0 {
					b.Fatalf("bitmap secondary lookup failed: available=%v rows=%d err=%v", available, len(rows), err)
				}
			}
		})
	}
}
