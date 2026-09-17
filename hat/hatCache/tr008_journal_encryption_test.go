package hatCache

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"hatrie_cache/hat/hatJournal"
)

func TestTR008EncryptedCommandJournalRoundTripAndKeyRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.log")
	oldKey := []byte("01234567890123456789012345678901")
	newKey := []byte("abcdefghijklmnopqrstuvwxyzABCDEF")

	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitWindow:   DefaultJournalGroupCommitWindow,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
		Encryption: hatJournal.EncryptionOptions{
			KeyID: "old-key",
			Key:   oldKey,
		},
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	trie := newTestTrie(t)
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{
		Command: "SETSTR",
		Key:     "secret",
		Value:   "old-value",
	}); !response.OK {
		t.Fatalf("old-key ExecuteCommand() response = %#v", response)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if bytes.Contains(raw, []byte("old-value")) {
		t.Fatalf("encrypted journal contains plaintext command value: %q", raw)
	}
	if !bytes.Contains(raw, []byte("old-key")) {
		t.Fatalf("encrypted journal does not contain its rotation key ID: %q", raw)
	}
	tampered := append([]byte(nil), raw...)
	tampered[len(tampered)-1] ^= 1
	if err := os.WriteFile(path, tampered, 0o600); err != nil {
		t.Fatalf("WriteFile(tampered journal) error = %v", err)
	}
	if _, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitWindow:   DefaultJournalGroupCommitWindow,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
		Encryption: hatJournal.EncryptionOptions{
			KeyID: "old-key",
			Key:   oldKey,
		},
	}); !errors.Is(err, hatJournal.ErrEncryptionAuthentication) {
		t.Fatalf("OpenCommandJournalWithOptions() with tampered record error = %v, want authentication error", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile(restored journal) error = %v", err)
	}

	rotated, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitWindow:   DefaultJournalGroupCommitWindow,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
		Encryption: hatJournal.EncryptionOptions{
			KeyID: "new-key",
			Key:   newKey,
			Keyring: map[string][]byte{
				"old-key": oldKey,
			},
		},
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() after rotation error = %v", err)
	}
	rotatedTrie := newTestTrie(t)
	tail, err := rotated.Tail(0, 10)
	if err != nil || len(tail.Entries) != 1 || tail.Entries[0].Request.Value != "old-value" {
		t.Fatalf("Tail() after rotation = %#v/%v, want old-value", tail, err)
	}
	if sequence, err := rotated.Replay(rotatedTrie, 0); err != nil || sequence != 1 {
		t.Fatalf("Replay() after rotation = %d/%v, want 1/nil", sequence, err)
	}
	if got := rotatedTrie.GetString("secret"); got != "old-value" {
		t.Fatalf("replayed old value = %q, want old-value", got)
	}
	if response := rotated.ExecuteCommand(rotatedTrie, CacheCommandRequest{
		Command: "SETSTR",
		Key:     "secret",
		Value:   "new-value",
	}); !response.OK {
		t.Fatalf("new-key ExecuteCommand() response = %#v", response)
	}
	if err := rotated.Close(); err != nil {
		t.Fatalf("rotated Close() error = %v", err)
	}

	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() after rotation error = %v", err)
	}
	if bytes.Contains(raw, []byte("new-value")) {
		t.Fatalf("rotated encrypted journal contains plaintext command value: %q", raw)
	}
	if !bytes.Contains(raw, []byte("new-key")) {
		t.Fatalf("rotated encrypted journal does not contain new key ID: %q", raw)
	}
	inspection, err := InspectCommandJournal(path, CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitWindow:   DefaultJournalGroupCommitWindow,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
		Encryption: hatJournal.EncryptionOptions{
			KeyID: "new-key",
			Key:   newKey,
			Keyring: map[string][]byte{
				"old-key": oldKey,
			},
		},
	})
	if err != nil || inspection.RecordCount != 2 || inspection.ValidBytes != int64(len(raw)) {
		t.Fatalf("InspectCommandJournal() = %#v/%v, want two records and all bytes valid", inspection, err)
	}

	wrongKey := append([]byte(nil), oldKey...)
	wrongKey[0]++
	if _, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitWindow:   DefaultJournalGroupCommitWindow,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
		Encryption: hatJournal.EncryptionOptions{
			KeyID: "old-key",
			Key:   wrongKey,
		},
	}); err == nil {
		t.Fatal("OpenCommandJournalWithOptions() with wrong key succeeded")
	}
	reopened, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitWindow:   DefaultJournalGroupCommitWindow,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
		Encryption: hatJournal.EncryptionOptions{
			KeyID: "new-key",
			Key:   newKey,
			Keyring: map[string][]byte{
				"old-key": oldKey,
			},
		},
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() after rotation error = %v", err)
	}
	finalTrie := newTestTrie(t)
	if sequence, err := reopened.Replay(finalTrie, 0); err != nil || sequence != 2 {
		t.Fatalf("Replay() after rotation reopen = %d/%v, want 2/nil", sequence, err)
	}
	if got := finalTrie.GetString("secret"); got != "new-value" {
		t.Fatalf("replayed rotated value = %q, want new-value", got)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("reopened Close() error = %v", err)
	}
}

func TestTR008LegacyJournalCanBeReadBeforeEncryptedAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.log")

	legacy, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	legacyTrie := newTestTrie(t)
	if response := legacy.ExecuteCommand(legacyTrie, CacheCommandRequest{
		Command: "SETSTR",
		Key:     "legacy",
		Value:   "legacy HJE1 value",
	}); !response.OK {
		t.Fatalf("legacy ExecuteCommand() response = %#v", response)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("legacy Close() error = %v", err)
	}

	key := []byte("12345678901234567890123456789012")
	encrypted, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		GroupCommitWindow:   DefaultJournalGroupCommitWindow,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
		Encryption:          hatJournal.EncryptionOptions{KeyID: "current", Key: key},
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() for legacy journal error = %v", err)
	}
	encryptedTrie := newTestTrie(t)
	if sequence, err := encrypted.Replay(encryptedTrie, 0); err != nil || sequence != 1 {
		t.Fatalf("Replay() of legacy journal = %d/%v, want 1/nil", sequence, err)
	}
	if got := encryptedTrie.GetString("legacy"); got != "legacy HJE1 value" {
		t.Fatalf("replayed legacy value = %q, want legacy HJE1 value", got)
	}
	if response := encrypted.ExecuteCommand(encryptedTrie, CacheCommandRequest{
		Command: "SETSTR",
		Key:     "encrypted",
		Value:   "encrypted-value",
	}); !response.OK {
		t.Fatalf("encrypted append response = %#v", response)
	}
	if err := encrypted.Close(); err != nil {
		t.Fatalf("encrypted Close() error = %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !bytes.Contains(raw, []byte("legacy HJE1 value")) {
		t.Fatalf("legacy journal record disappeared during encrypted append")
	}
	if bytes.Contains(raw, []byte("encrypted-value")) {
		t.Fatalf("encrypted append contains plaintext command value: %q", raw)
	}
	reopened, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		GroupCommitWindow:   DefaultJournalGroupCommitWindow,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
		Encryption:          hatJournal.EncryptionOptions{KeyID: "current", Key: key},
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() mixed journal error = %v", err)
	}
	replayed := newTestTrie(t)
	if sequence, err := reopened.Replay(replayed, 0); err != nil || sequence != 2 {
		t.Fatalf("Replay() mixed journal = %d/%v, want 2/nil", sequence, err)
	}
	if replayed.GetString("legacy") != "legacy HJE1 value" || replayed.GetString("encrypted") != "encrypted-value" {
		t.Fatalf("mixed journal replay = legacy %q/encrypted %q", replayed.GetString("legacy"), replayed.GetString("encrypted"))
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("mixed reopened Close() error = %v", err)
	}
}

func TestTR008EncryptedCommandJournalRecordBatch(t *testing.T) {
	for _, format := range []CommandJournalFormat{CommandJournalFormatBinary, CommandJournalFormatJSON} {
		t.Run(string(format), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "journal.log")
			key := []byte("01234567890123456789012345678901")
			options := CommandJournalOptions{
				Format:              format,
				GroupCommitWindow:   DefaultJournalGroupCommitWindow,
				GroupCommitMaxBatch: 1,
				Encryption:          hatJournal.EncryptionOptions{KeyID: "current", Key: key},
			}
			journal, err := OpenCommandJournalWithOptions(path, options)
			if err != nil {
				t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
			}
			trie := newTestTrie(t)
			records := []CommandJournalRecord{
				{Sequence: 1, Request: CacheCommandRequest{Command: "SETSTR", Key: "batch:first", Value: "first-value"}},
				{Sequence: 2, Request: CacheCommandRequest{Command: "SETSTR", Key: "batch:second", Value: "second-value"}},
			}
			if applied, response := journal.executeJournalRecordsBatch(trie, records); applied != len(records) || !response.OK {
				t.Fatalf("executeJournalRecordsBatch() = %d/%#v", applied, response)
			}
			if err := journal.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile() error = %v", err)
			}
			if bytes.Contains(raw, []byte("first-value")) || bytes.Contains(raw, []byte("second-value")) {
				t.Fatalf("encrypted batch journal contains plaintext value: %q", raw)
			}
			reopened, err := OpenCommandJournalWithOptions(path, options)
			if err != nil {
				t.Fatalf("OpenCommandJournalWithOptions(reopen) error = %v", err)
			}
			replayed := newTestTrie(t)
			if sequence, err := reopened.Replay(replayed, 0); err != nil || sequence != 2 {
				t.Fatalf("Replay() = %d/%v, want 2/nil", sequence, err)
			}
			if replayed.GetString("batch:first") != "first-value" || replayed.GetString("batch:second") != "second-value" {
				t.Fatalf("batch replay values = %q/%q", replayed.GetString("batch:first"), replayed.GetString("batch:second"))
			}
			if err := reopened.Close(); err != nil {
				t.Fatalf("reopened Close() error = %v", err)
			}
		})
	}
}

func TestTR008EncryptedSegmentedJournalReplaysAndInspects(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.log")
	key := []byte("12345678901234567890123456789012")
	options := CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitWindow:   DefaultJournalGroupCommitWindow,
		GroupCommitMaxBatch: 1,
		SegmentMaxBytes:     1,
		RetainedSegments:    8,
		Encryption:          hatJournal.EncryptionOptions{KeyID: "current", Key: key},
	}
	journal, err := OpenCommandJournalWithOptions(path, options)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	trie := newTestTrie(t)
	for index, value := range []string{"one-value", "two-value", "three-value"} {
		if response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "segment:" + string(rune('a'+index)),
			Value:   value,
		}); !response.OK {
			t.Fatalf("segment ExecuteCommand(%d) response = %#v", index, response)
		}
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	segments, err := hatJournal.ListSegments(path)
	if err != nil || len(segments) == 0 {
		t.Fatalf("ListSegments() = %#v/%v, want archived segments", segments, err)
	}
	for _, segment := range segments {
		raw, err := os.ReadFile(segment.Path)
		if err != nil {
			t.Fatalf("ReadFile(%q) error = %v", segment.Path, err)
		}
		if bytes.Contains(raw, []byte("one-value")) || bytes.Contains(raw, []byte("two-value")) || bytes.Contains(raw, []byte("three-value")) {
			t.Fatalf("encrypted segment %q contains plaintext value", segment.Path)
		}
	}
	inspection, err := InspectCommandJournal(path, options)
	if err != nil || inspection.RecordCount != 5 || inspection.LastSequence != 3 || len(inspection.Segments) == 0 {
		t.Fatalf("InspectCommandJournal() = %#v/%v, want three mutations plus two checkpoints", inspection, err)
	}
	reopened, err := OpenCommandJournalWithOptions(path, options)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions(reopen) error = %v", err)
	}
	replayed := newTestTrie(t)
	if sequence, err := reopened.Replay(replayed, 0); err != nil || sequence != 3 {
		t.Fatalf("Replay() segmented = %d/%v, want 3/nil", sequence, err)
	}
	if replayed.GetString("segment:a") != "one-value" || replayed.GetString("segment:b") != "two-value" || replayed.GetString("segment:c") != "three-value" {
		t.Fatalf("segmented replay values = %q/%q/%q", replayed.GetString("segment:a"), replayed.GetString("segment:b"), replayed.GetString("segment:c"))
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("reopened Close() error = %v", err)
	}
}
