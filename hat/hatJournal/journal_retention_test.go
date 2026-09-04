package hatJournal

import "testing"

func TestValidateOptionsRetainedBytes(t *testing.T) {
	if _, err := ValidateOptions(Options{GroupCommitMaxBatch: DefaultGroupCommitMaxBatch, RetainedBytes: 1}); err != nil {
		t.Fatalf("positive retained bytes rejected: %v", err)
	}
	if _, err := ValidateOptions(Options{GroupCommitMaxBatch: DefaultGroupCommitMaxBatch, RetainedBytes: -1}); err == nil {
		t.Fatal("negative retained bytes accepted")
	}
	if _, err := ValidateOptions(Options{GroupCommitMaxBatch: DefaultGroupCommitMaxBatch, RetainedBytes: MaxRetainedBytes + 1}); err == nil {
		t.Fatal("retained bytes above the maximum accepted")
	}
}
