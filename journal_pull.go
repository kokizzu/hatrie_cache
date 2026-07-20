package hatriecache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const DefaultCommandJournalPullTimeout = 30 * time.Second

type CommandJournalPullOptions struct {
	Source        string
	AfterSequence uint64
	Limit         uint64
	UntilCurrent  bool
	MaxBatches    uint64
	Timeout       time.Duration
	Client        *http.Client
	DirtyTracker  *LevelDBDirtyTracker
	AuthToken     string
	WireFormat    CommandJournalWireFormat
}

type CommandJournalPullError struct {
	Status int
	Err    error
}

func (err *CommandJournalPullError) Error() string {
	if err == nil || err.Err == nil {
		return ""
	}
	return err.Err.Error()
}

func (err *CommandJournalPullError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

func PullCommandJournal(ctx context.Context, trie *HatTrie, journal *CommandJournal, options CommandJournalPullOptions) (CommandJournalPullResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if trie == nil {
		return CommandJournalPullResult{}, commandJournalPullError(http.StatusBadRequest, errors.New("trie is not configured"))
	}
	if journal == nil {
		return CommandJournalPullResult{}, commandJournalPullError(http.StatusConflict, errors.New("journal is not configured"))
	}
	limit, err := normalizeCommandJournalTailLimit(options.Limit)
	if err != nil {
		return CommandJournalPullResult{}, commandJournalPullError(http.StatusBadRequest, err)
	}
	maxBatches, err := normalizeCommandJournalPullBatches(options.UntilCurrent, options.MaxBatches)
	if err != nil {
		return CommandJournalPullResult{}, commandJournalPullError(http.StatusBadRequest, err)
	}
	client, err := commandJournalPullHTTPClient(options.Client, options.Timeout)
	if err != nil {
		return CommandJournalPullResult{}, commandJournalPullError(http.StatusBadRequest, err)
	}
	wireFormat, err := ParseCommandJournalWireFormat(string(options.WireFormat))
	if err != nil {
		return CommandJournalPullResult{}, commandJournalPullError(http.StatusBadRequest, err)
	}
	return pullCommandJournalTail(ctx, trie, journal, options.Source, options.AfterSequence, limit, options.UntilCurrent, maxBatches, client, options.DirtyTracker, options.AuthToken, wireFormat)
}

func commandJournalPullError(status int, err error) error {
	return &CommandJournalPullError{Status: status, Err: err}
}

func commandJournalPullHTTPClient(client *http.Client, timeout time.Duration) (*http.Client, error) {
	if timeout < 0 {
		return nil, errors.New("journal pull timeout must be non-negative")
	}
	if client != nil {
		return client, nil
	}
	if timeout == 0 {
		return http.DefaultClient, nil
	}
	return &http.Client{Timeout: timeout}, nil
}

func pullCommandJournalTail(ctx context.Context, trie *HatTrie, journal *CommandJournal, source string, afterSequence uint64, limit int, untilCurrent bool, maxBatches int, client *http.Client, dirtyTracker *LevelDBDirtyTracker, authToken string, wireFormat CommandJournalWireFormat) (CommandJournalPullResult, error) {
	result := CommandJournalPullResult{
		Source:         source,
		AfterSequence:  afterSequence,
		AppliedThrough: afterSequence,
	}
	for batch := 0; batch < maxBatches; batch++ {
		endpoint, err := commandJournalEndpoint(source, result.AppliedThrough, limit)
		if err != nil {
			return result, commandJournalPullError(http.StatusBadRequest, err)
		}
		tail, status, err := fetchCommandJournalTailAuthorizedWithFormat(ctx, client, endpoint, authToken, wireFormat)
		if err != nil {
			if status == http.StatusConflict {
				err = fmt.Errorf("%w: %v", ErrCommandJournalCompacted, err)
			}
			return result, commandJournalPullError(status, err)
		}
		batchResult, err := applyCommandJournalTail(trie, journal, source, result.AppliedThrough, tail, dirtyTracker)
		if err != nil {
			result.LastSequence = batchResult.LastSequence
			result.CompactedThrough = batchResult.CompactedThrough
			result.Applied += batchResult.Applied
			result.AppliedThrough = batchResult.AppliedThrough
			result.HasMore = batchResult.HasMore
			result.WireFormat = batchResult.WireFormat
			result.Batches++
			status := http.StatusBadGateway
			if errors.Is(err, ErrCommandJournalCompacted) {
				status = http.StatusConflict
			}
			return result, commandJournalPullError(status, err)
		}
		result.LastSequence = batchResult.LastSequence
		result.CompactedThrough = batchResult.CompactedThrough
		result.Applied += batchResult.Applied
		result.AppliedThrough = batchResult.AppliedThrough
		result.HasMore = batchResult.HasMore
		result.WireFormat = batchResult.WireFormat
		result.Batches++
		if !untilCurrent || !result.HasMore {
			return result, nil
		}
		if batchResult.Applied == 0 {
			return result, commandJournalPullError(http.StatusBadGateway, errors.New("journal source reported more entries without returning progress"))
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
	}
	return result, nil
}

func PullCommandJournalSnapshot(ctx context.Context, source string, authToken string, client *http.Client, path string, minimumSequences ...uint64) (SnapshotMetadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if client == nil {
		client = http.DefaultClient
	}
	if strings.TrimSpace(path) == "" {
		return SnapshotMetadata{}, errors.New("journal pull snapshot path is required")
	}
	endpoint, err := journalSnapshotEndpoint(source)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	request.Header.Set("Accept", snapshotContentType)
	request.Header.Set("Accept-Encoding", "identity")
	setReplicationAuthHeaders(request, authToken)
	response, err := client.Do(request)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	defer drainAndClose(response.Body)
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, readErr := readCommandJournalErrorResponseBody(response.Body)
		if readErr != nil {
			return SnapshotMetadata{}, readErr
		}
		return SnapshotMetadata{}, fmt.Errorf("journal snapshot source returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	if contentType := strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0]); contentType != "" && contentType != snapshotContentType {
		return SnapshotMetadata{}, fmt.Errorf("journal snapshot source returned content type %q", contentType)
	}
	minimumSequence := uint64(0)
	if len(minimumSequences) > 0 {
		minimumSequence = minimumSequences[0]
	}
	return writeCommandJournalSnapshotAtomic(path, response.Body, minimumSequence)
}

func PullCommandJournalCheckpoint(ctx context.Context, source string, authToken string, client *http.Client, path string, minimumSequences ...uint64) (BackupBundleManifest, error) {
	return pullCommandJournalCheckpoint(ctx, source, authToken, client, path, true, minimumSequences...)
}

// DownloadCommandJournalCheckpoint validates checkpoint metadata while leaving
// payload checksum and semantic validation to the caller's staging/startup path.
func DownloadCommandJournalCheckpoint(ctx context.Context, source string, authToken string, client *http.Client, path string, minimumSequences ...uint64) (BackupBundleManifest, error) {
	return pullCommandJournalCheckpoint(ctx, source, authToken, client, path, false, minimumSequences...)
}

func pullCommandJournalCheckpoint(ctx context.Context, source string, authToken string, client *http.Client, path string, semanticVerify bool, minimumSequences ...uint64) (BackupBundleManifest, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if client == nil {
		client = http.DefaultClient
	}
	if strings.TrimSpace(path) == "" {
		return BackupBundleManifest{}, errors.New("journal pull checkpoint path is required")
	}
	endpoint, err := journalCheckpointEndpoint(source)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	request.Header.Set("Accept", checkpointContentType)
	request.Header.Set("Accept-Encoding", "identity")
	setReplicationAuthHeaders(request, authToken)
	response, err := client.Do(request)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	defer drainAndClose(response.Body)
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, readErr := readCommandJournalErrorResponseBody(response.Body)
		if readErr != nil {
			return BackupBundleManifest{}, readErr
		}
		return BackupBundleManifest{}, fmt.Errorf("journal checkpoint source returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	if contentType := strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0]); contentType != "" && contentType != checkpointContentType {
		return BackupBundleManifest{}, fmt.Errorf("journal checkpoint source returned content type %q", contentType)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return BackupBundleManifest{}, err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return BackupBundleManifest{}, err
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}
	if _, err := io.Copy(tmp, response.Body); err != nil {
		cleanup()
		return BackupBundleManifest{}, err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return BackupBundleManifest{}, err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return BackupBundleManifest{}, err
	}
	manifest, err := readBackupBundleManifest(tmpPath)
	if err != nil {
		_ = os.Remove(tmpPath)
		return BackupBundleManifest{}, err
	}
	if backupBundleManifestMode(manifest) != BackupModePebbleCheckpoint || manifest.StorageBackend != string(StorageBackendPebble) {
		_ = os.Remove(tmpPath)
		return BackupBundleManifest{}, errors.New("journal checkpoint source did not return a Pebble checkpoint bundle")
	}
	minimumSequence := uint64(0)
	if len(minimumSequences) > 0 {
		minimumSequence = minimumSequences[0]
	}
	journalSequence := manifest.JournalSequence
	if semanticVerify {
		report, err := VerifyBackupBundle(tmpPath)
		if err != nil {
			_ = os.Remove(tmpPath)
			return BackupBundleManifest{}, err
		}
		journalSequence = report.JournalSequence
	}
	if journalSequence < minimumSequence {
		_ = os.Remove(tmpPath)
		return BackupBundleManifest{}, fmt.Errorf("journal checkpoint sequence %d is older than required sequence %d", journalSequence, minimumSequence)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return BackupBundleManifest{}, err
	}
	if err := syncDirectory(dir); err != nil {
		return BackupBundleManifest{}, err
	}
	return manifest, nil
}

func writeCommandJournalSnapshotAtomic(path string, body io.Reader, minimumSequence uint64) (SnapshotMetadata, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return SnapshotMetadata{}, err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return SnapshotMetadata{}, err
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}
	if _, err := io.Copy(tmp, body); err != nil {
		cleanup()
		return SnapshotMetadata{}, err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return SnapshotMetadata{}, err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return SnapshotMetadata{}, err
	}
	metadata, err := ReadSnapshotMetadata(tmpPath)
	if err != nil {
		_ = os.Remove(tmpPath)
		return SnapshotMetadata{}, err
	}
	if metadata.JournalSequence < minimumSequence {
		_ = os.Remove(tmpPath)
		return SnapshotMetadata{}, fmt.Errorf("journal snapshot sequence %d is older than required sequence %d", metadata.JournalSequence, minimumSequence)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return SnapshotMetadata{}, err
	}
	if err := syncDirectory(dir); err != nil {
		return SnapshotMetadata{}, err
	}
	return metadata, nil
}

func journalSnapshotEndpoint(source string) (string, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return "", errors.New("journal source is required")
	}
	if !strings.Contains(source, "://") {
		source = "http://" + source
	}
	parsed, err := url.Parse(source)
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("journal source is invalid")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/api/journal/snapshot"
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func journalCheckpointEndpoint(source string) (string, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return "", errors.New("journal source is required")
	}
	if !strings.Contains(source, "://") {
		source = "http://" + source
	}
	parsed, err := url.Parse(source)
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("journal source is invalid")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/api/journal/checkpoint"
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func setReplicationAuthHeaders(request *http.Request, authToken string) {
	authToken = normalizeAuthToken(authToken)
	if request == nil || authToken == "" {
		return
	}
	request.Header.Set("Authorization", "Bearer "+authToken)
	request.Header.Set("X-Hatrie-Replication-Token", authToken)
}

func applyCommandJournalTail(trie *HatTrie, journal *CommandJournal, source string, afterSequence uint64, tail CommandJournalTail, dirtyTrackers ...*LevelDBDirtyTracker) (CommandJournalPullResult, error) {
	var dirtyTracker *LevelDBDirtyTracker
	if len(dirtyTrackers) > 0 {
		dirtyTracker = dirtyTrackers[0]
	}
	result := CommandJournalPullResult{
		Source:           source,
		AfterSequence:    afterSequence,
		LastSequence:     tail.LastSequence,
		CompactedThrough: tail.CompactedThrough,
		AppliedThrough:   afterSequence,
		HasMore:          tail.HasMore,
		WireFormat:       string(tail.wireFormat),
	}
	if afterSequence < tail.CompactedThrough {
		return result, fmt.Errorf("%w: requested sequence %d is before compacted sequence %d", ErrCommandJournalCompacted, afterSequence, tail.CompactedThrough)
	}
	if err := validateCommandJournalTailSequences(afterSequence, tail); err != nil {
		return result, err
	}
	detachCommandJournalTailBorrowedStrings(trie, &tail, dirtyTracker)
	applied, response := journal.executeJournalRecordsBatch(trie, tail.Entries)
	for idx := 0; idx < applied; idx++ {
		dirtyTracker.markCommand(tail.Entries[idx].Request)
		result.Applied++
		result.AppliedThrough = tail.Entries[idx].Sequence
	}
	if !response.OK {
		return result, fmt.Errorf("journal entry %d failed: %s", tail.Entries[applied].Sequence, response.Message)
	}
	return result, nil
}

func detachCommandJournalTailBorrowedStrings(trie *HatTrie, tail *CommandJournalTail, dirtyTracker *LevelDBDirtyTracker) {
	if tail == nil || tail.wireFormat != CommandJournalWireFormatBinary {
		return
	}
	retainAllKeys := dirtyTracker != nil || trie.commandJournalApplyMayRetainBorrowedKeys()
	for index := range tail.Entries {
		request := &tail.Entries[index].Request
		command := request.Command
		if retainAllKeys || commandJournalRequestMayRetainBorrowedKey(request, command) {
			request.Key = strings.Clone(request.Key)
		}
		switch command {
		case "SET", "SETSTR", "SETX", "SETSTRX":
			request.Value = strings.Clone(request.Value)
		case "SETINT", "SETINTX", "INC", "DEL", "EXPIRE", "EXPIREAT", "PERSIST":
		default:
			request.Value = strings.Clone(request.Value)
			request.Subkey = strings.Clone(request.Subkey)
		}
	}
}

func (ht *HatTrie) commandJournalApplyMayRetainBorrowedKeys() bool {
	if ht == nil {
		return false
	}
	if ht.localPartitionSet() != nil {
		return true
	}
	ht.mu.RLock()
	defer ht.mu.RUnlock()
	return ht.keyStatsMode != KeyStatsModeOff ||
		ht.snapshotMutations != nil ||
		ht.levelDBSpillKeys != nil ||
		ht.levelDBHotValues != nil
}

func commandJournalRequestMayRetainBorrowedKey(request *CacheCommandRequest, command string) bool {
	if request == nil {
		return false
	}
	if request.TTLSeconds != nil || request.UnixSeconds != nil {
		return true
	}
	switch command {
	case "SET", "SETSTR", "SETINT", "INC", "DEL", "PERSIST":
		return false
	default:
		return true
	}
}

func validateCommandJournalTailSequences(afterSequence uint64, tail CommandJournalTail) error {
	appliedThrough := afterSequence
	for _, entry := range tail.Entries {
		nextSequence, err := nextCommandJournalPullSequence(appliedThrough)
		if err != nil {
			return err
		}
		if entry.Sequence != nextSequence {
			return fmt.Errorf("journal tail sequence %d does not continue after %d", entry.Sequence, appliedThrough)
		}
		if entry.Sequence > tail.LastSequence {
			return fmt.Errorf("journal tail sequence %d exceeds last sequence %d", entry.Sequence, tail.LastSequence)
		}
		appliedThrough = entry.Sequence
	}
	if tail.HasMore {
		if len(tail.Entries) == 0 {
			return errors.New("journal tail reported more entries without returning entries")
		}
		if tail.LastSequence <= appliedThrough {
			return fmt.Errorf("journal tail reported more entries after %d but last sequence is %d", appliedThrough, tail.LastSequence)
		}
		return nil
	}
	if tail.LastSequence > appliedThrough {
		return fmt.Errorf("journal tail last sequence %d exceeds applied sequence %d without has_more", tail.LastSequence, appliedThrough)
	}
	return nil
}

func nextCommandJournalPullSequence(appliedThrough uint64) (uint64, error) {
	if appliedThrough == ^uint64(0) {
		return 0, ErrCommandJournalSequenceExhausted
	}
	return appliedThrough + 1, nil
}
