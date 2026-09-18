//go:build !tr007baseline

package hatCache

import (
	"path/filepath"
	"testing"
	"time"
)

func TestTR007AdaptiveGroupCommitWindowUsesQueuePressure(t *testing.T) {
	base := 40 * time.Millisecond
	tests := []struct {
		name       string
		queued     int
		wantWindow time.Duration
	}{
		{name: "idle", queued: 0, wantWindow: base},
		{name: "some pressure", queued: 1, wantWindow: base / 4},
		{name: "half full", queued: 4, wantWindow: 0},
		{name: "full", queued: 7, wantWindow: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := adaptiveGroupCommitWindow(base, 8, test.queued); got != test.wantWindow {
				t.Fatalf("adaptiveGroupCommitWindow(%s, 8, %d) = %s, want %s", base, test.queued, got, test.wantWindow)
			}
		})
	}
	if got := adaptiveGroupCommitWindow(0, 8, 4); got != 0 {
		t.Fatalf("adaptiveGroupCommitWindow(0, 8, 4) = %s, want 0", got)
	}
	if got := adaptiveGroupCommitWindow(base, 1, 1); got != base {
		t.Fatalf("adaptiveGroupCommitWindow(%s, 1, 1) = %s, want %s", base, got, base)
	}
}

func TestTR007AdaptiveGroupCommitOptionIsExplicit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	adaptive, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		GroupCommitWindow:   time.Millisecond,
		GroupCommitMaxBatch: 4,
		AdaptiveGroupCommit: true,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions(adaptive) error = %v", err)
	}
	if !adaptive.adaptiveGroupCommit {
		t.Fatal("adaptive journal did not retain AdaptiveGroupCommit=true")
	}
	if err := adaptive.Close(); err != nil {
		t.Fatalf("adaptive Close() error = %v", err)
	}

	fixed, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitWindow:   time.Millisecond,
		GroupCommitMaxBatch: 4,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions(default) error = %v", err)
	}
	if fixed.adaptiveGroupCommit {
		t.Fatal("default journal enabled adaptive group commit")
	}
	if err := fixed.Close(); err != nil {
		t.Fatalf("fixed Close() error = %v", err)
	}
}
