package hatCache

import (
	"path/filepath"
	"testing"
)

func BenchmarkMZ050ManualReplay(b *testing.B) {
	journal, _ := benchmarkMZ050Journal(b)
	targetSequence := journal.Sequence()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		branch := CreateHatTrie()
		if _, err := journal.ReplayThrough(branch, 0, targetSequence); err != nil {
			b.Fatal(err)
		}
		branch.Destroy()
	}
}

func BenchmarkMZ050BranchAt(b *testing.B) {
	journal, _ := benchmarkMZ050Journal(b)
	targetSequence := journal.Sequence()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		branch, err := journal.BranchAt(targetSequence)
		if err != nil {
			b.Fatal(err)
		}
		branch.Close()
	}
}

func BenchmarkMZ050ManualBranchReplay(b *testing.B) {
	journal, _ := benchmarkMZ050Journal(b)
	targetSequence := journal.Sequence()
	request := CacheCommandRequest{Command: "SETSTR", Key: "answer", Value: "what-if"}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		branch := CreateHatTrie()
		if _, err := journal.ReplayThrough(branch, 0, targetSequence); err != nil {
			b.Fatal(err)
		}
		if response := branch.ExecuteCommand(request); !response.OK {
			b.Fatal(response.Message)
		}
		branch.Destroy()
	}
}

func BenchmarkMZ050BranchReplayInto(b *testing.B) {
	journal, _ := benchmarkMZ050Journal(b)
	branch, err := journal.BranchAt(journal.Sequence())
	if err != nil {
		b.Fatal(err)
	}
	if response := branch.Execute(CacheCommandRequest{Command: "SETSTR", Key: "answer", Value: "what-if"}); !response.OK {
		b.Fatal(response.Message)
	}
	b.Cleanup(branch.Close)
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		target := CreateHatTrie()
		if err := branch.ReplayInto(target); err != nil {
			b.Fatal(err)
		}
		target.Destroy()
	}
}

func benchmarkMZ050Journal(b *testing.B) (*CommandJournal, *HatTrie) {
	b.Helper()
	journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "journal.log"))
	if err != nil {
		b.Fatal(err)
	}
	source := CreateHatTrie()
	for index := 0; index < 128; index++ {
		response := journal.ExecuteCommand(source, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "timeline:key:" + string(rune('a'+index%26)),
			Value:   "value",
		})
		if !response.OK {
			b.Fatal(response.Message)
		}
	}
	b.Cleanup(func() {
		journal.Close()
		source.Destroy()
	})
	return journal, source
}
