package hatriecache

import "testing"

func TestCommandJournalRetainedBytesConstants(t *testing.T) {
	if DefaultCommandJournalRetainedBytes != 0 {
		t.Fatalf("default retained bytes = %d, want 0", DefaultCommandJournalRetainedBytes)
	}
	if MaxCommandJournalRetainedBytes <= DefaultCommandJournalRetainedBytes {
		t.Fatalf("maximum retained bytes = %d, want greater than default", MaxCommandJournalRetainedBytes)
	}
	options := CommandJournalOptions{GroupCommitMaxBatch: 1, RetainedBytes: 1}
	if options.RetainedBytes != 1 {
		t.Fatalf("public CommandJournalOptions retained bytes = %d, want 1", options.RetainedBytes)
	}
}
