//go:build tu34

package hatCache

import (
	"testing"
	"time"
)

func BenchmarkTU34SpaceSyncPolicies(b *testing.B) {
	for _, test := range []struct {
		name   string
		policy CommandJournalSpaceSyncPolicy
		legacy bool
	}{
		{name: "legacy-synchronous", legacy: true},
		{name: "space-synchronous", policy: CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncModeSynchronous}},
		{name: "space-periodic", policy: CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncModePeriodic, Interval: time.Hour}},
		{name: "space-disabled", policy: CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncModeDisabled}},
	} {
		b.Run(test.name, func(b *testing.B) {
			journal, err := OpenCommandJournalWithOptions(b.TempDir()+"/commands.journal", CommandJournalOptions{GroupCommitMaxBatch: 1})
			if err != nil {
				b.Fatal(err)
			}
			journal.syncHook = func() error { return nil }
			b.Cleanup(func() { _ = journal.Close() })
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			var space *CommandJournalSpace
			if !test.legacy {
				space, err = journal.OpenSpace("benchmark", test.policy)
				if err != nil {
					b.Fatal(err)
				}
			}
			request := CacheCommandRequest{Command: "SETSTR", Key: "benchmark", Value: "value"}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if test.legacy {
					if response := journal.ExecuteCommand(trie, request); !response.OK {
						b.Fatal(response)
					}
					continue
				}
				if response := space.ExecuteCommand(trie, request); !response.OK {
					b.Fatal(response)
				}
			}
		})
	}
}
