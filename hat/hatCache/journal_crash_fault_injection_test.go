package hatCache

import (
	"errors"
	"io"
	"path/filepath"
	"testing"
)

func TestCommandJournalCrashFaultMatrixKeepsDurablePrefix(t *testing.T) {
	for _, format := range []CommandJournalFormat{CommandJournalFormatBinary, CommandJournalFormatJSON} {
		for _, fault := range []struct {
			name  string
			setup func(t *testing.T, journal *CommandJournal)
		}{
			{
				name: "partial active tail",
				setup: func(t *testing.T, journal *CommandJournal) {
					t.Helper()
					entry, err := marshalCommandJournalEntry(commandJournalEntry{
						Version:  commandJournalVersion,
						Sequence: 2,
						Request:  CacheCommandRequest{Command: "SETSTR", Key: "after", Value: "journal"},
					}, journal.format)
					if err != nil {
						t.Fatalf("marshalCommandJournalEntry() error = %v", err)
					}
					prefix := entry[:len(entry)/2]
					if _, err := journal.file.Write(prefix); err != nil {
						t.Fatalf("write partial tail error = %v", err)
					}
				},
			},
			{
				name: "short write",
				setup: func(t *testing.T, journal *CommandJournal) {
					t.Helper()
					journal.writeHook = func(data []byte) (int, error) {
						n := len(data) / 2
						if n == 0 {
							n = 1
						}
						if _, err := journal.file.Write(data[:n]); err != nil {
							return 0, err
						}
						return n, io.ErrShortWrite
					}
				},
			},
			{
				name: "write error after prefix",
				setup: func(t *testing.T, journal *CommandJournal) {
					t.Helper()
					journal.writeHook = func(data []byte) (int, error) {
						n := len(data) / 2
						if n == 0 {
							n = 1
						}
						if _, err := journal.file.Write(data[:n]); err != nil {
							return 0, err
						}
						return n, errors.New("injected journal write failure")
					}
				},
			},
			{
				name: "sync failure",
				setup: func(t *testing.T, journal *CommandJournal) {
					t.Helper()
					journal.syncHook = func() error {
						return errors.New("injected journal sync failure")
					}
				},
			},
		} {
			t.Run(string(format)+"/"+fault.name, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "commands.journal")
				journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
					Format:              format,
					GroupCommitMaxBatch: 1,
				})
				if err != nil {
					t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
				}
				trie := newTestTrie(t)
				first := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "before", Value: "durable"})
				if !first.OK {
					journal.Close()
					t.Fatalf("baseline ExecuteCommand() = %#v, want success", first)
				}

				fault.setup(t, journal)
				if fault.name != "partial active tail" {
					applied, response := journal.executeJournalRecordsBatch(trie, []CommandJournalRecord{{
						Request: CacheCommandRequest{Command: "SETSTR", Key: "after", Value: "journal"},
					}})
					if applied != 0 || response.OK {
						journal.Close()
						t.Fatalf("faulted executeJournalRecordsBatch() = %d/%#v, want failure", applied, response)
					}
				}
				if err := journal.Close(); err != nil {
					t.Fatalf("Close() error = %v", err)
				}

				reopened, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
					Format:              format,
					GroupCommitMaxBatch: 1,
				})
				if err != nil {
					t.Fatalf("reopen after %s error = %v", fault.name, err)
				}
				defer reopened.Close()
				replayed := newTestTrie(t)
				if _, err := reopened.Replay(replayed, 0); err != nil {
					t.Fatalf("Replay() after %s error = %v", fault.name, err)
				}
				if got := replayed.GetString("before"); got != "durable" {
					t.Fatalf("replayed before = %q, want durable", got)
				}
				if replayed.Exists("after") {
					t.Fatalf("replayed after = %q, want absent after %s", replayed.GetString("after"), fault.name)
				}
				if got := reopened.Sequence(); got != 1 {
					t.Fatalf("reopened sequence = %d, want durable prefix sequence 1", got)
				}
			})
		}
	}
}
