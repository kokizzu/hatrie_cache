package hatCache

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	json "github.com/goccy/go-json"
)

const scheduledSnapshotCheckpointVersion = 1

var (
	ErrScheduledSnapshotInvalid = errors.New("hatriecache: scheduled snapshot options are invalid")
	ErrScheduledSnapshotStarted = errors.New("hatriecache: scheduled snapshotter is already started")
	ErrScheduledSnapshotStopped = errors.New("hatriecache: scheduled snapshotter is not started")
)

// ScheduledSnapshotOptions configures an opt-in background snapshot schedule.
// A failed export leaves the resumable checkpoint in place for the next run.
type ScheduledSnapshotOptions struct {
	Interval       time.Duration
	Destination    string
	ManifestPath   string
	CheckpointPath string
	Format         SnapshotFormat
	RunImmediately bool
	OnPublished    func(ScheduledSnapshotReport)
	OnError        func(error)
}

// ScheduledSnapshotReport describes one successfully published snapshot.
type ScheduledSnapshotReport struct {
	Manifest    SnapshotManifest
	PublishedAt time.Time
	Resumed     bool
	Runs        uint64
}

// ScheduledSnapshotCheckpoint is the durable manifest published beside the
// latest snapshot. The manifest is written only after the snapshot target has
// been atomically renamed into place.
type ScheduledSnapshotCheckpoint struct {
	Version     int              `json:"version"`
	Destination string           `json:"destination"`
	Manifest    SnapshotManifest `json:"manifest"`
	PublishedAt time.Time        `json:"published_at"`
	Runs        uint64           `json:"runs"`
}

// ScheduledSnapshotStatus is a detached scheduler state snapshot.
type ScheduledSnapshotStatus struct {
	Started bool
	Runs    uint64
	Last    ScheduledSnapshotReport
	HasLast bool
}

// ScheduledSnapshotter periodically exports one trie snapshot and publishes
// its checkpoint manifest. It owns no data and does not alter the journal's
// default durability or retention behavior.
type ScheduledSnapshotter struct {
	journal        *CommandJournal
	trie           *HatTrie
	interval       time.Duration
	destination    string
	manifestPath   string
	checkpointPath string
	format         SnapshotFormat
	runImmediately bool
	onPublished    func(ScheduledSnapshotReport)
	onError        func(error)

	runMu   sync.Mutex
	mu      sync.RWMutex
	started bool
	stopCh  chan struct{}
	doneCh  chan struct{}
	runs    uint64
	last    ScheduledSnapshotReport
	hasLast bool
}

// NewScheduledSnapshotter validates options and loads an existing durable
// checkpoint, if present. The scheduler is stopped until Start is called.
func NewScheduledSnapshotter(journal *CommandJournal, trie *HatTrie, options ScheduledSnapshotOptions) (*ScheduledSnapshotter, error) {
	if journal == nil || trie == nil || options.Interval <= 0 || options.Destination == "" {
		return nil, ErrScheduledSnapshotInvalid
	}
	destination, err := filepath.Abs(options.Destination)
	if err != nil {
		return nil, err
	}
	format := options.Format
	if format == "" {
		format = SnapshotFormatBinary
	}
	format, err = ParseSnapshotFormat(string(format))
	if err != nil {
		return nil, err
	}
	manifestPath, err := scheduledSnapshotPath(options.ManifestPath, destination+".manifest.json")
	if err != nil {
		return nil, err
	}
	checkpointPath, err := scheduledSnapshotPath(options.CheckpointPath, destination+".resume.json")
	if err != nil {
		return nil, err
	}
	if filepath.Clean(manifestPath) == filepath.Clean(destination) ||
		filepath.Clean(checkpointPath) == filepath.Clean(destination) ||
		filepath.Clean(manifestPath) == filepath.Clean(checkpointPath) {
		return nil, fmt.Errorf("%w: snapshot paths overlap", ErrScheduledSnapshotInvalid)
	}

	scheduler := &ScheduledSnapshotter{
		journal:        journal,
		trie:           trie,
		interval:       options.Interval,
		destination:    destination,
		manifestPath:   manifestPath,
		checkpointPath: checkpointPath,
		format:         format,
		runImmediately: options.RunImmediately,
		onPublished:    options.OnPublished,
		onError:        options.OnError,
	}
	checkpoint, err := ReadScheduledSnapshotCheckpoint(manifestPath)
	if errors.Is(err, os.ErrNotExist) {
		return scheduler, nil
	}
	if err != nil {
		return nil, err
	}
	if filepath.Clean(checkpoint.Destination) != filepath.Clean(destination) {
		return nil, fmt.Errorf("%w: checkpoint destination mismatch", ErrScheduledSnapshotInvalid)
	}
	scheduler.runs = checkpoint.Runs
	scheduler.last = ScheduledSnapshotReport{
		Manifest:    checkpoint.Manifest,
		PublishedAt: checkpoint.PublishedAt,
		Runs:        checkpoint.Runs,
	}
	scheduler.hasLast = true
	return scheduler, nil
}

func scheduledSnapshotPath(value string, fallback string) (string, error) {
	if value == "" {
		value = fallback
	}
	return filepath.Abs(value)
}

// ManifestPath returns the durable checkpoint manifest path.
func (scheduler *ScheduledSnapshotter) ManifestPath() string {
	if scheduler == nil {
		return ""
	}
	return scheduler.manifestPath
}

// Start starts the background ticker. It is safe to call RunNow before Start.
func (scheduler *ScheduledSnapshotter) Start() error {
	if scheduler == nil {
		return ErrScheduledSnapshotInvalid
	}
	scheduler.mu.Lock()
	if scheduler.started {
		scheduler.mu.Unlock()
		return ErrScheduledSnapshotStarted
	}
	scheduler.stopCh = make(chan struct{})
	scheduler.doneCh = make(chan struct{})
	scheduler.started = true
	stopCh := scheduler.stopCh
	doneCh := scheduler.doneCh
	scheduler.mu.Unlock()
	go scheduler.runLoop(stopCh, doneCh)
	return nil
}

func (scheduler *ScheduledSnapshotter) runLoop(stopCh <-chan struct{}, doneCh chan<- struct{}) {
	ticker := time.NewTicker(scheduler.interval)
	defer ticker.Stop()
	defer close(doneCh)
	if scheduler.runImmediately {
		scheduler.runOnce()
	}
	for {
		select {
		case <-ticker.C:
			scheduler.runOnce()
		case <-stopCh:
			return
		}
	}
}

func (scheduler *ScheduledSnapshotter) runOnce() {
	if _, err := scheduler.RunNow(); err != nil {
		scheduler.mu.RLock()
		onError := scheduler.onError
		scheduler.mu.RUnlock()
		if onError != nil {
			onError(err)
		}
	}
}

// Stop stops the ticker and waits for an in-flight scheduled run to finish.
func (scheduler *ScheduledSnapshotter) Stop() error {
	if scheduler == nil {
		return ErrScheduledSnapshotInvalid
	}
	scheduler.mu.Lock()
	if !scheduler.started {
		scheduler.mu.Unlock()
		return ErrScheduledSnapshotStopped
	}
	stopCh := scheduler.stopCh
	doneCh := scheduler.doneCh
	scheduler.started = false
	close(stopCh)
	scheduler.mu.Unlock()
	<-doneCh
	return nil
}

// RunNow performs one serialized snapshot export and manifest publication.
func (scheduler *ScheduledSnapshotter) RunNow() (ScheduledSnapshotReport, error) {
	if scheduler == nil {
		return ScheduledSnapshotReport{}, ErrScheduledSnapshotInvalid
	}
	scheduler.runMu.Lock()

	export, err := scheduler.journal.WriteSnapshotWithResumableExport(scheduler.trie, scheduler.destination, SnapshotExportOptions{
		Format:         scheduler.format,
		CheckpointPath: scheduler.checkpointPath,
	})
	if err != nil {
		scheduler.runMu.Unlock()
		return ScheduledSnapshotReport{}, err
	}
	scheduler.mu.Lock()
	runs := scheduler.runs + 1
	scheduler.mu.Unlock()
	report := ScheduledSnapshotReport{
		Manifest:    export.Manifest,
		PublishedAt: time.Now().UTC(),
		Resumed:     export.Resumed,
		Runs:        runs,
	}
	checkpoint := ScheduledSnapshotCheckpoint{
		Version:     scheduledSnapshotCheckpointVersion,
		Destination: scheduler.destination,
		Manifest:    report.Manifest,
		PublishedAt: report.PublishedAt,
		Runs:        report.Runs,
	}
	if err := writeJSONFileAtomic(scheduler.manifestPath, checkpoint); err != nil {
		scheduler.runMu.Unlock()
		return ScheduledSnapshotReport{}, err
	}
	scheduler.mu.Lock()
	scheduler.runs = runs
	scheduler.last = report
	scheduler.hasLast = true
	onPublished := scheduler.onPublished
	scheduler.mu.Unlock()
	scheduler.runMu.Unlock()
	if onPublished != nil {
		onPublished(report)
	}
	return report, nil
}

// Status returns a detached view of the scheduler state.
func (scheduler *ScheduledSnapshotter) Status() ScheduledSnapshotStatus {
	if scheduler == nil {
		return ScheduledSnapshotStatus{}
	}
	scheduler.mu.RLock()
	defer scheduler.mu.RUnlock()
	return ScheduledSnapshotStatus{
		Started: scheduler.started,
		Runs:    scheduler.runs,
		Last:    scheduler.last,
		HasLast: scheduler.hasLast,
	}
}

// ReadScheduledSnapshotCheckpoint reads and validates a durable checkpoint
// manifest written by a ScheduledSnapshotter.
func ReadScheduledSnapshotCheckpoint(path string) (ScheduledSnapshotCheckpoint, error) {
	if path == "" {
		return ScheduledSnapshotCheckpoint{}, ErrScheduledSnapshotInvalid
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return ScheduledSnapshotCheckpoint{}, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return ScheduledSnapshotCheckpoint{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return ScheduledSnapshotCheckpoint{}, fmt.Errorf("%w: manifest is not a regular file", ErrScheduledSnapshotInvalid)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ScheduledSnapshotCheckpoint{}, err
	}
	var checkpoint ScheduledSnapshotCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return ScheduledSnapshotCheckpoint{}, fmt.Errorf("%w: %v", ErrScheduledSnapshotInvalid, err)
	}
	if err := validateScheduledSnapshotCheckpoint(checkpoint); err != nil {
		return ScheduledSnapshotCheckpoint{}, err
	}
	return checkpoint, nil
}

func validateScheduledSnapshotCheckpoint(checkpoint ScheduledSnapshotCheckpoint) error {
	if checkpoint.Version != scheduledSnapshotCheckpointVersion ||
		checkpoint.Destination == "" ||
		checkpoint.PublishedAt.IsZero() ||
		checkpoint.Runs == 0 ||
		checkpoint.Manifest.SizeBytes < 0 ||
		len(checkpoint.Manifest.SHA256) != 64 {
		return ErrScheduledSnapshotInvalid
	}
	if _, err := hex.DecodeString(checkpoint.Manifest.SHA256); err != nil {
		return ErrScheduledSnapshotInvalid
	}
	if _, err := ParseSnapshotFormat(string(checkpoint.Manifest.Format)); err != nil {
		return ErrScheduledSnapshotInvalid
	}
	return nil
}
