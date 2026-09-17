package hatCache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"hatrie_cache/hat/hatJournal"
)

func TestTR008EncryptedOfflineCheckpointInstaller(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	segmentDir := commandJournalSegmentDir(path)
	if err := os.MkdirAll(segmentDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(segmentDir, "stale.segment"), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	options := CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitWindow:   DefaultJournalGroupCommitWindow,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
		Encryption: hatJournal.EncryptionOptions{
			KeyID: "current",
			Key:   []byte("12345678901234567890123456789012"),
		},
	}
	if err := InstallCommandJournalCheckpointWithOptions(path, options, 7); err != nil {
		t.Fatalf("InstallCommandJournalCheckpointWithOptions() error = %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(raw, []byte{0x48, 0x4a, 0x45, 0x31}) {
		t.Fatalf("encrypted checkpoint prefix = %x, want HJE1", raw[:min(len(raw), 4)])
	}
	if _, err := os.Stat(segmentDir); !os.IsNotExist(err) {
		t.Fatalf("segment directory stat = %v, want not exist", err)
	}
	report, err := InspectCommandJournal(path, options)
	if err != nil {
		t.Fatalf("InspectCommandJournal() error = %v", err)
	}
	if report.RecordCount != 1 || report.LastSequence != 7 {
		t.Fatalf("encrypted checkpoint report = %#v, want one record at sequence 7", report)
	}
}
