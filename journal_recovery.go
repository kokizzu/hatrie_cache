package hatriecache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	json "github.com/goccy/go-json"

	"hatrie_cache/internal/jsonwire"
)

const maxCommandJournalRecoveryManifestBytes = 4 << 20

var commandJournalRecoveryRepositoryLocks sync.Map

type CommandJournalRecoveryResult struct {
	Manifest          BackupBundleManifest
	DownloadedObjects int
	ReusedObjects     int
	DownloadedBytes   int64
	ReusedBytes       int64
}

// PullCommandJournalRecovery downloads only checkpoint objects absent from a
// source-specific local repository. Every reused and downloaded object is
// checksum-verified before the new manifest becomes current.
func PullCommandJournalRecovery(ctx context.Context, source string, authToken string, client *http.Client, repositoryPath string, minimumSequence uint64) (CommandJournalRecoveryResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if client == nil {
		client = http.DefaultClient
	}
	repositoryPath = strings.TrimSpace(repositoryPath)
	if repositoryPath == "" {
		return CommandJournalRecoveryResult{}, errors.New("journal recovery repository path is required")
	}
	repositoryPath, err := filepath.Abs(repositoryPath)
	if err != nil {
		return CommandJournalRecoveryResult{}, err
	}
	mutexValue, _ := commandJournalRecoveryRepositoryLocks.LoadOrStore(repositoryPath, &sync.Mutex{})
	mutex := mutexValue.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()
	endpoint, err := journalRecoveryEndpoint(source, nil)
	if err != nil {
		return CommandJournalRecoveryResult{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return CommandJournalRecoveryResult{}, err
	}
	request.Header.Set("Accept", journalRecoveryManifestContentType)
	request.Header.Set("Accept-Encoding", "identity")
	setReplicationAuthHeaders(request, authToken)
	response, err := client.Do(request)
	if err != nil {
		return CommandJournalRecoveryResult{}, err
	}
	manifest, err := decodeCommandJournalRecoveryManifest(response)
	if err != nil {
		return CommandJournalRecoveryResult{}, err
	}
	if manifest.JournalSequence < minimumSequence {
		return CommandJournalRecoveryResult{}, fmt.Errorf("journal recovery sequence %d is older than required sequence %d", manifest.JournalSequence, minimumSequence)
	}
	if err := ensureBackupRepository(repositoryPath); err != nil {
		return CommandJournalRecoveryResult{}, err
	}

	result := CommandJournalRecoveryResult{Manifest: manifest}
	for _, file := range manifest.Files {
		objectPath, err := backupRepositoryObjectPath(repositoryPath, file.SHA256)
		if err != nil {
			return CommandJournalRecoveryResult{}, err
		}
		reused, err := verifyCommandJournalRecoveryObject(objectPath, file)
		if err != nil {
			return CommandJournalRecoveryResult{}, err
		}
		if reused {
			result.ReusedObjects++
			result.ReusedBytes += file.Size
			continue
		}
		if err := downloadCommandJournalRecoveryObject(ctx, client, source, authToken, manifest.BackupID, file, objectPath); err != nil {
			return CommandJournalRecoveryResult{}, err
		}
		result.DownloadedObjects++
		result.DownloadedBytes += file.Size
	}
	manifestData, err := jsonwire.Marshal(manifest)
	if err != nil {
		return CommandJournalRecoveryResult{}, err
	}
	manifestData = append(manifestData, '\n')
	manifestPath := filepath.Join(repositoryPath, backupRepositoryManifestsPath, manifest.BackupID+".json")
	if err := writeFileAtomic(manifestPath, manifestData); err != nil {
		return CommandJournalRecoveryResult{}, err
	}
	if _, err := readBackupRepositoryManifest(repositoryPath, manifest.BackupID); err != nil {
		return CommandJournalRecoveryResult{}, err
	}
	if err := writeFileAtomic(filepath.Join(repositoryPath, backupRepositoryLatestPath), []byte(manifest.BackupID+"\n")); err != nil {
		return CommandJournalRecoveryResult{}, err
	}
	if err := pruneBackupRepository(repositoryPath, manifest.BackupID, DefaultBackupRepositoryRetention); err != nil {
		return CommandJournalRecoveryResult{}, err
	}
	return result, nil
}

func decodeCommandJournalRecoveryManifest(response *http.Response) (BackupBundleManifest, error) {
	if response == nil {
		return BackupBundleManifest{}, errors.New("journal recovery source returned no response")
	}
	defer drainAndClose(response.Body)
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, err := readCommandJournalErrorResponseBody(response.Body)
		if err != nil {
			return BackupBundleManifest{}, err
		}
		return BackupBundleManifest{}, fmt.Errorf("journal recovery source returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	contentType := strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0])
	if contentType != "" && contentType != journalRecoveryManifestContentType {
		return BackupBundleManifest{}, fmt.Errorf("journal recovery source returned content type %q", contentType)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxCommandJournalRecoveryManifestBytes+1))
	if err != nil {
		return BackupBundleManifest{}, err
	}
	if len(data) > maxCommandJournalRecoveryManifestBytes {
		return BackupBundleManifest{}, errors.New("journal recovery manifest is too large")
	}
	var manifest BackupBundleManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return BackupBundleManifest{}, err
	}
	if err := validateCommandJournalRecoveryManifest(manifest); err != nil {
		return BackupBundleManifest{}, err
	}
	return manifest, nil
}

func validateCommandJournalRecoveryManifest(manifest BackupBundleManifest) error {
	if manifest.Version != BackupBundleVersion || manifest.Mode != BackupModePebbleIncremental || manifest.StorageBackend != string(StorageBackendPebble) || manifest.Store != backupBundleStorePath || manifest.BackupID == "" {
		return errors.New("journal recovery source returned an invalid Pebble repository manifest")
	}
	if manifest.Journal != "" && manifest.Journal != backupBundleJournalPath {
		return errors.New("journal recovery source returned an invalid journal path")
	}
	if _, err := ParseStorageFormat(manifest.StorageFormat); err != nil {
		return err
	}
	computedID, err := backupRepositoryManifestID(manifest)
	if err != nil {
		return err
	}
	if computedID != manifest.BackupID {
		return errors.New("journal recovery manifest checksum mismatch")
	}
	paths := make(map[string]struct{}, len(manifest.Files))
	for _, file := range manifest.Files {
		clean, err := cleanBackupBundlePath(file.Path)
		if err != nil || clean != file.Path || file.Size < 0 {
			return errors.New("journal recovery manifest contains an invalid file")
		}
		if _, exists := paths[file.Path]; exists {
			return errors.New("journal recovery manifest contains duplicate files")
		}
		paths[file.Path] = struct{}{}
		if _, err := backupRepositoryObjectPath(".", file.SHA256); err != nil {
			return err
		}
	}
	return nil
}

func verifyCommandJournalRecoveryObject(path string, declaration BackupBundleFile) (bool, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		_ = file.Close()
		return false, err
	}
	if err := file.Close(); err != nil {
		return false, err
	}
	if size == declaration.Size && hex.EncodeToString(hash.Sum(nil)) == declaration.SHA256 {
		return true, nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return false, nil
}

func downloadCommandJournalRecoveryObject(ctx context.Context, client *http.Client, source string, authToken string, backupID string, declaration BackupBundleFile, targetPath string) error {
	query := url.Values{"backup_id": []string{backupID}, "object": []string{declaration.SHA256}}
	endpoint, err := journalRecoveryEndpoint(source, query)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", journalRecoveryObjectContentType)
	request.Header.Set("Accept-Encoding", "identity")
	setReplicationAuthHeaders(request, authToken)
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer drainAndClose(response.Body)
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, readErr := readCommandJournalErrorResponseBody(response.Body)
		if readErr != nil {
			return readErr
		}
		return fmt.Errorf("journal recovery object source returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	contentType := strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0])
	if contentType != "" && contentType != journalRecoveryObjectContentType {
		return fmt.Errorf("journal recovery object source returned content type %q", contentType)
	}
	if length := response.Header.Get("Content-Length"); length != "" {
		parsed, err := strconv.ParseInt(length, 10, 64)
		if err != nil || parsed != declaration.Size {
			return errors.New("journal recovery object content length mismatch")
		}
	}
	return writeFileAtomicStream(targetPath, func(writer io.Writer) error {
		hash := sha256.New()
		size, err := io.Copy(io.MultiWriter(writer, hash), io.LimitReader(response.Body, declaration.Size+1))
		if err != nil {
			return err
		}
		if size != declaration.Size || hex.EncodeToString(hash.Sum(nil)) != declaration.SHA256 {
			return errors.New("journal recovery object checksum mismatch")
		}
		return nil
	})
}

func journalRecoveryEndpoint(source string, query url.Values) (string, error) {
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
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/api/journal/recovery"
	parsed.RawQuery = query.Encode()
	parsed.Fragment = ""
	return parsed.String(), nil
}
