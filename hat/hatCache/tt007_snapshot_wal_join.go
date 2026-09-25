package hatCache

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var ErrCommandJournalJoinIncomplete = errors.New("hatriecache: snapshot and WAL join did not reach the source tail")

// CommandJournalJoinOptions configures a snapshot-plus-WAL join. The snapshot
// is downloaded and installed first; the journal is then pulled after the
// snapshot sequence until the source reports no more entries.
type CommandJournalJoinOptions struct {
	Source                   string
	SnapshotPath             string
	Client                   *http.Client
	AuthToken                string
	Limit                    uint64
	MaxBatches               uint64
	Timeout                  time.Duration
	DirtyTracker             *LevelDBDirtyTracker
	WireFormat               CommandJournalWireFormat
	ReplicationApplyThrottle ReplicationApplyThrottle
	Persist                  func() error
}

// CommandJournalJoinResult reports the exact snapshot boundary and the WAL
// progress applied after it. SnapshotPath is intentionally not retained for
// an automatically created temporary snapshot.
type CommandJournalJoinResult struct {
	Snapshot SnapshotMetadata
	Pull     CommandJournalPullResult
}

// JoinCommandJournalSnapshot installs one verified source snapshot and pulls
// the source journal after that snapshot until the source tail is reached.
// When SnapshotPath is empty, the snapshot is stored in a cleaned temporary
// directory. Persist is called only after the complete join succeeds.
func JoinCommandJournalSnapshot(ctx context.Context, trie *HatTrie, journal *CommandJournal, options CommandJournalJoinOptions) (CommandJournalJoinResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if trie == nil {
		return CommandJournalJoinResult{}, ErrNilHatTrie
	}
	if journal == nil {
		return CommandJournalJoinResult{}, ErrNilCommandJournal
	}
	client, err := commandJournalPullHTTPClient(options.Client, options.Timeout)
	if err != nil {
		return CommandJournalJoinResult{}, err
	}
	snapshotPath, cleanup, err := commandJournalJoinSnapshotPath(options.SnapshotPath)
	if err != nil {
		return CommandJournalJoinResult{}, err
	}
	if cleanup != nil {
		defer cleanup()
	}
	snapshot, err := PullCommandJournalSnapshot(ctx, options.Source, options.AuthToken, client, snapshotPath)
	if err != nil {
		return CommandJournalJoinResult{}, fmt.Errorf("snapshot join download: %w", err)
	}
	if _, err := journal.ReplaceWithSnapshot(trie, snapshotPath); err != nil {
		return CommandJournalJoinResult{Snapshot: snapshot}, fmt.Errorf("snapshot join install: %w", err)
	}
	pull, err := PullCommandJournal(ctx, trie, journal, CommandJournalPullOptions{
		Source:                   options.Source,
		AfterSequence:            snapshot.JournalSequence,
		Limit:                    options.Limit,
		UntilCurrent:             true,
		MaxBatches:               options.MaxBatches,
		Timeout:                  options.Timeout,
		Client:                   client,
		DirtyTracker:             options.DirtyTracker,
		AuthToken:                options.AuthToken,
		WireFormat:               options.WireFormat,
		ReplicationApplyThrottle: options.ReplicationApplyThrottle,
	})
	result := CommandJournalJoinResult{Snapshot: snapshot, Pull: pull}
	if err != nil {
		return result, fmt.Errorf("snapshot join WAL replay: %w", err)
	}
	if pull.HasMore {
		return result, fmt.Errorf("%w: applied through sequence %d after %d batch(es)", ErrCommandJournalJoinIncomplete, pull.AppliedThrough, pull.Batches)
	}
	if options.Persist != nil {
		if err := options.Persist(); err != nil {
			return result, fmt.Errorf("snapshot join persistence: %w", err)
		}
	}
	return result, nil
}

func commandJournalJoinSnapshotPath(path string) (string, func(), error) {
	path = strings.TrimSpace(path)
	if path != "" {
		return path, nil, nil
	}
	directory, err := os.MkdirTemp("", ".hatrie-command-journal-join-*")
	if err != nil {
		return "", nil, err
	}
	return filepath.Join(directory, "snapshot.hc"), func() { _ = os.RemoveAll(directory) }, nil
}
