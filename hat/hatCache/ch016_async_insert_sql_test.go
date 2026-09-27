package hatCache

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestCH016AsyncInsertBufferAcceptsSQLInsert(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	buffer, err := NewAsyncInsertBuffer(journal, trie, AsyncInsertBufferOptions{
		BatchSize:     1,
		Capacity:      4,
		FlushInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer buffer.Close(context.Background())

	receipt, err := buffer.SubmitSQL(context.Background(), "INSERT INTO cache (key, value) VALUES ('async:sql', 'queued')")
	if err != nil {
		t.Fatalf("SubmitSQL() error = %v", err)
	}
	response, err := receipt.Wait(context.Background())
	if err != nil || !response.OK {
		t.Fatalf("SubmitSQL() response = %#v/%v", response, err)
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "async:sql"}); !got.OK || got.Value != "queued" {
		t.Fatalf("async SQL insert result = %#v, want queued value", got)
	}
}

func TestCH016AsyncInsertBufferRejectsNonLiteralSQLInserts(t *testing.T) {
	tests := []string{
		"SELECT value FROM cache WHERE key = 'read'",
		"UPDATE cache SET value = 'changed' WHERE key = 'key'",
		"INSERT INTO cache (key, value) FROM VALUES ('key', 'value') AS rows(key, value) SELECT key, value",
		"INSERT INTO cache (key, value) VALUES ('key', 'value') ON CONFLICT (key) DO NOTHING",
		"INSERT INTO cache (key, value) VALUES ('key', 'value') RETURNING key",
		"INSERT INTO cache (key, value) VALUES ('key', 'value'); INSERT INTO cache (key, value) VALUES ('other', 'value')",
	}
	for _, source := range tests {
		t.Run(source, func(t *testing.T) {
			if _, err := compileAsyncInsertSQL(source); !errors.Is(err, ErrAsyncInsertSQLUnsupported) {
				t.Fatalf("compileAsyncInsertSQL(%q) error = %v, want ErrAsyncInsertSQLUnsupported", source, err)
			}
		})
	}
}
