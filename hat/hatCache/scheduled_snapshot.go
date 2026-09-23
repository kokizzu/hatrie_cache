package hatCache

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	json "github.com/goccy/go-json"
)

const ScheduledSnapshotManifestVersion = 1

var (
	ErrScheduledSnapshotIntervalInvalid = errors.New("hatriecache: scheduled snapshot interval must be positive")
	ErrScheduledSnapshotPathRequired    = errors.New("hatriecache: scheduled snapshot path is required")
	ErrScheduledSnapshotManifestInvalid = errors.New("hatriecache: scheduled snapshot manifest is invalid")
	ErrScheduledSnapshotClosed          = errors.New("hatriecache: scheduled snapshot scheduler is closed")
)

// ScheduledSnapshotOptions controls an opt-in periodic snapshot worker.
// SnapshotPath is atomically replaced before ManifestPath is published.
type ScheduledSnapshotOptions struct {
	Interval       time.Duration
	SnapshotPath   string
	ManifestPath   string
	Format         SnapshotFormat
	RunImmediately bool
}

// ScheduledSnapshotManifest identifies one atomically published snapshot and
// its journal checkpoint. SnapshotPath is relative to the manifest directory
// whenever both paths can be represented that way.
type ScheduledSnapshotManifest struct {
	Version      int              `json:"version"`
	SnapshotPath string           `json:"snapshot_path"`
	Snapshot     SnapshotManifest `json:"snapshot"`
	PublishedAt  time.Time        `json:"published_at"`
}

// ScheduledSnapshotResult describes one completed snapshot publication.
type ScheduledSnapshotResult struct {
	Manifest ScheduledSnapshotManifest
	Duration time.Duration
}

// ScheduledSnapshotStatus is a point-in-time view of scheduler progress.
type ScheduledSnapshotStatus struct {
	LastResult ScheduledSnapshotResult
	LastError  error
	Completed  uint64
	Failed     uint64
}

// ScheduledSnapshotScheduler periodically publishes a snapshot and matching
// checkpoint manifest. A scheduler owns one goroutine and must be closed.
type ScheduledSnapshotScheduler struct {
	journal   *CommandJournal
	trie      *HatTrie
	options   ScheduledSnapshotOptions
	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
	runMu     sync.Mutex
	statusMu  sync.RWMutex
	closed    bool
	status    ScheduledSnapshotStatus
}

// StartScheduledSnapshots starts a periodic snapshot worker. Interval zero
// disables the feature by returning an error; callers opt in explicitly.
func StartScheduledSnapshots(ctx context.Context, journal *CommandJournal, trie *HatTrie, options ScheduledSnapshotOptions) (*ScheduledSnapshotScheduler, error) {
	if journal == nil {
		return nil, ErrNilCommandJournal
	}
	if trie == nil {
		return nil, ErrNilHatTrie
	}
	if options.Interval <= 0 {
		return nil, ErrScheduledSnapshotIntervalInvalid
	}
	if strings.TrimSpace(options.SnapshotPath) == "" {
		return nil, ErrScheduledSnapshotPathRequired
	}
	if options.ManifestPath == "" {
		options.ManifestPath = options.SnapshotPath + ".manifest.json"
	}
	if strings.TrimSpace(options.ManifestPath) == "" {
		return nil, ErrScheduledSnapshotPathRequired
	}
	if filepath.Clean(options.SnapshotPath) == filepath.Clean(options.ManifestPath) {
		return nil, fmt.Errorf("%w: snapshot and manifest paths must differ", ErrScheduledSnapshotManifestInvalid)
	}
	if options.Format == "" {
		options.Format = DefaultSnapshotFormat
	}
	format, err := ParseSnapshotFormat(string(options.Format))
	if err != nil {
		return nil, err
	}
	options.Format = format
	if ctx == nil {
		ctx = context.Background()
	}
	workerContext, cancel := context.WithCancel(ctx)
	scheduler := &ScheduledSnapshotScheduler{
		journal: journal,
		trie:    trie,
		options: options,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	go scheduler.run(workerContext)
	return scheduler, nil
}

func (scheduler *ScheduledSnapshotScheduler) run(ctx context.Context) {
	defer close(scheduler.done)
	if scheduler.options.RunImmediately {
		_, _ = scheduler.RunOnce(ctx)
	}
	ticker := time.NewTicker(scheduler.options.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = scheduler.RunOnce(ctx)
		}
	}
}

// RunOnce publishes one snapshot immediately. It is serialized with ticker
// runs, making manual checkpoint requests safe alongside periodic execution.
func (scheduler *ScheduledSnapshotScheduler) RunOnce(ctx context.Context) (ScheduledSnapshotResult, error) {
	if scheduler == nil {
		return ScheduledSnapshotResult{}, ErrScheduledSnapshotClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	scheduler.statusMu.RLock()
	closed := scheduler.closed
	scheduler.statusMu.RUnlock()
	if closed {
		return ScheduledSnapshotResult{}, ErrScheduledSnapshotClosed
	}
	scheduler.runMu.Lock()
	defer scheduler.runMu.Unlock()
	scheduler.statusMu.RLock()
	closed = scheduler.closed
	scheduler.statusMu.RUnlock()
	if closed {
		return ScheduledSnapshotResult{}, ErrScheduledSnapshotClosed
	}
	if err := ctx.Err(); err != nil {
		return ScheduledSnapshotResult{}, err
	}
	started := time.Now()
	result, err := scheduler.publish(ctx)
	result.Duration = time.Since(started)
	scheduler.statusMu.Lock()
	if err != nil {
		scheduler.status.Failed++
		scheduler.status.LastError = err
	} else {
		scheduler.status.Completed++
		scheduler.status.LastResult = result
		scheduler.status.LastError = nil
	}
	scheduler.statusMu.Unlock()
	return result, err
}

// Status returns the latest scheduler result and counters.
func (scheduler *ScheduledSnapshotScheduler) Status() ScheduledSnapshotStatus {
	if scheduler == nil {
		return ScheduledSnapshotStatus{}
	}
	scheduler.statusMu.RLock()
	defer scheduler.statusMu.RUnlock()
	return scheduler.status
}

// Close stops the worker and waits for an in-flight snapshot to finish.
func (scheduler *ScheduledSnapshotScheduler) Close() error {
	if scheduler == nil {
		return nil
	}
	scheduler.closeOnce.Do(func() {
		scheduler.statusMu.Lock()
		scheduler.closed = true
		scheduler.statusMu.Unlock()
		scheduler.cancel()
	})
	scheduler.runMu.Lock()
	scheduler.runMu.Unlock()
	<-scheduler.done
	return nil
}

func (scheduler *ScheduledSnapshotScheduler) publish(ctx context.Context) (ScheduledSnapshotResult, error) {
	if err := ctx.Err(); err != nil {
		return ScheduledSnapshotResult{}, err
	}
	scheduler.journal.snapshotMu.Lock()
	defer scheduler.journal.snapshotMu.Unlock()

	relativeSnapshotPath, err := filepath.Rel(filepath.Dir(scheduler.options.ManifestPath), scheduler.options.SnapshotPath)
	if err != nil {
		return ScheduledSnapshotResult{}, err
	}
	var publication ScheduledSnapshotManifest
	snapshot, err := writeScheduledSnapshotPairAtomic(
		scheduler.options.SnapshotPath,
		scheduler.options.ManifestPath,
		func(writer io.Writer) (SnapshotManifest, error) {
			return scheduler.journal.writeSnapshotWithManifestLocked(scheduler.trie, writer, scheduler.options.Format)
		},
		func(writer io.Writer, snapshot SnapshotManifest) error {
			publication = ScheduledSnapshotManifest{
				Version:      ScheduledSnapshotManifestVersion,
				SnapshotPath: relativeSnapshotPath,
				Snapshot:     snapshot,
				PublishedAt:  time.Now().UTC(),
			}
			encoder := json.NewEncoder(writer)
			encoder.SetIndent("", "  ")
			return encoder.Encode(publication)
		},
	)
	if err != nil {
		return ScheduledSnapshotResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return ScheduledSnapshotResult{}, err
	}
	scheduler.journal.mu.Lock()
	defer scheduler.journal.mu.Unlock()
	if scheduler.journal.closed {
		return ScheduledSnapshotResult{}, ErrCommandJournalClosed
	}
	if err := scheduler.journal.compactLocked(snapshot.JournalSequence); err != nil {
		return ScheduledSnapshotResult{}, err
	}
	return ScheduledSnapshotResult{Manifest: publication}, nil
}

func writeScheduledSnapshotPairAtomic(snapshotPath, manifestPath string, writeSnapshot func(io.Writer) (SnapshotManifest, error), writeManifest func(io.Writer, SnapshotManifest) error) (SnapshotManifest, error) {
	snapshotDir := filepath.Dir(snapshotPath)
	manifestDir := filepath.Dir(manifestPath)
	if err := os.MkdirAll(snapshotDir, 0o700); err != nil {
		return SnapshotManifest{}, err
	}
	if manifestDir != snapshotDir {
		if err := os.MkdirAll(manifestDir, 0o700); err != nil {
			return SnapshotManifest{}, err
		}
	}
	snapshotFile, err := os.CreateTemp(snapshotDir, filepath.Base(snapshotPath)+".tmp-*")
	if err != nil {
		return SnapshotManifest{}, err
	}
	manifestFile, err := os.CreateTemp(manifestDir, filepath.Base(manifestPath)+".tmp-*")
	if err != nil {
		_ = snapshotFile.Close()
		_ = os.Remove(snapshotFile.Name())
		return SnapshotManifest{}, err
	}
	snapshotName := snapshotFile.Name()
	manifestName := manifestFile.Name()
	cleanup := func() {
		_ = snapshotFile.Close()
		_ = manifestFile.Close()
		_ = os.Remove(snapshotName)
		_ = os.Remove(manifestName)
	}
	snapshotWriter := bufio.NewWriter(snapshotFile)
	snapshot, err := writeSnapshot(snapshotWriter)
	if err != nil {
		cleanup()
		return SnapshotManifest{}, err
	}
	if err := snapshotWriter.Flush(); err != nil {
		cleanup()
		return SnapshotManifest{}, err
	}
	if err := snapshotFile.Sync(); err != nil {
		cleanup()
		return SnapshotManifest{}, err
	}
	if err := snapshotFile.Close(); err != nil {
		_ = os.Remove(snapshotName)
		_ = manifestFile.Close()
		_ = os.Remove(manifestName)
		return SnapshotManifest{}, err
	}
	manifestWriter := bufio.NewWriter(manifestFile)
	if err := writeManifest(manifestWriter, snapshot); err != nil {
		cleanup()
		return SnapshotManifest{}, err
	}
	if err := manifestWriter.Flush(); err != nil {
		cleanup()
		return SnapshotManifest{}, err
	}
	if err := manifestFile.Sync(); err != nil {
		cleanup()
		return SnapshotManifest{}, err
	}
	if err := manifestFile.Close(); err != nil {
		_ = os.Remove(snapshotName)
		_ = os.Remove(manifestName)
		return SnapshotManifest{}, err
	}
	if err := os.Rename(snapshotName, snapshotPath); err != nil {
		_ = os.Remove(snapshotName)
		_ = os.Remove(manifestName)
		return SnapshotManifest{}, err
	}
	if err := os.Rename(manifestName, manifestPath); err != nil {
		_ = os.Remove(manifestName)
		return SnapshotManifest{}, err
	}
	if err := syncDirectory(snapshotDir); err != nil {
		return SnapshotManifest{}, err
	}
	if manifestDir != snapshotDir {
		if err := syncDirectory(manifestDir); err != nil {
			return SnapshotManifest{}, err
		}
	}
	return snapshot, nil
}

// ReadScheduledSnapshotManifest reads and validates a checkpoint manifest.
func ReadScheduledSnapshotManifest(path string) (ScheduledSnapshotManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ScheduledSnapshotManifest{}, err
	}
	var manifest ScheduledSnapshotManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return ScheduledSnapshotManifest{}, err
	}
	if manifest.Version != ScheduledSnapshotManifestVersion || strings.TrimSpace(manifest.SnapshotPath) == "" {
		return ScheduledSnapshotManifest{}, ErrScheduledSnapshotManifestInvalid
	}
	return manifest, nil
}

// VerifyScheduledSnapshotManifest verifies the snapshot named by a manifest.
// Readers should call this before loading a scheduled snapshot after restart.
func VerifyScheduledSnapshotManifest(path string) (ScheduledSnapshotManifest, error) {
	manifest, err := ReadScheduledSnapshotManifest(path)
	if err != nil {
		return ScheduledSnapshotManifest{}, err
	}
	snapshotPath := manifest.SnapshotPath
	if !filepath.IsAbs(snapshotPath) {
		snapshotPath = filepath.Join(filepath.Dir(path), snapshotPath)
	}
	if err := VerifySnapshotManifest(snapshotPath, manifest.Snapshot); err != nil {
		return ScheduledSnapshotManifest{}, err
	}
	return manifest, nil
}
