package hatriecache

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"hatrie_cache/internal/jsonwire"
)

const (
	BackupBundleVersion      = 1
	backupBundleManifestPath = "manifest.json"
	backupBundleSnapshotPath = "snapshot.hc"
	backupBundleJournalPath  = "commands.journal"
)

type BackupBundleOptions struct {
	SnapshotFormat SnapshotFormat
	CreatedAt      time.Time
}

type BackupBundleManifest struct {
	Version         int                `json:"version"`
	CreatedAt       time.Time          `json:"created_at"`
	Snapshot        string             `json:"snapshot"`
	SnapshotFormat  string             `json:"snapshot_format"`
	Journal         string             `json:"journal,omitempty"`
	JournalFormat   string             `json:"journal_format,omitempty"`
	JournalSequence uint64             `json:"journal_sequence"`
	Files           []BackupBundleFile `json:"files"`
	RestoreHint     string             `json:"restore_hint"`
}

type BackupBundleFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

func CreateBackupBundle(path string, trie *HatTrie, journal *CommandJournal, options BackupBundleOptions) (BackupBundleManifest, error) {
	if path == "" {
		return BackupBundleManifest{}, errors.New("hatriecache: backup bundle path is required")
	}
	if trie == nil {
		return BackupBundleManifest{}, ErrNilHatTrie
	}
	snapshotFormat, err := ParseSnapshotFormat(string(options.SnapshotFormat))
	if err != nil {
		return BackupBundleManifest{}, err
	}
	createdAt := options.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	createdAt = createdAt.UTC()

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return BackupBundleManifest{}, err
	}
	tmpDir, err := os.MkdirTemp(dir, filepath.Base(path)+".work-*")
	if err != nil {
		return BackupBundleManifest{}, err
	}
	defer os.RemoveAll(tmpDir)

	if journal != nil {
		journal.mu.Lock()
		defer journal.mu.Unlock()
		if journal.closed {
			return BackupBundleManifest{}, ErrCommandJournalClosed
		}
		return createBackupBundleLocked(path, tmpDir, trie, journal.lastSequenceLocked(), journal.format, snapshotFormat, createdAt, true)
	}
	return createBackupBundleLocked(path, tmpDir, trie, 0, "", snapshotFormat, createdAt, false)
}

func createBackupBundleLocked(path string, tmpDir string, trie *HatTrie, journalSequence uint64, journalFormat CommandJournalFormat, snapshotFormat SnapshotFormat, createdAt time.Time, includeJournal bool) (BackupBundleManifest, error) {
	snapshotPath := filepath.Join(tmpDir, backupBundleSnapshotPath)
	if err := trie.SaveSnapshotWithJournalSequenceAndFormat(snapshotPath, journalSequence, snapshotFormat); err != nil {
		return BackupBundleManifest{}, err
	}
	snapshotFile, err := backupBundleFileInfo(backupBundleSnapshotPath, snapshotPath)
	if err != nil {
		return BackupBundleManifest{}, err
	}

	files := []BackupBundleFile{snapshotFile}
	var journalData []byte
	if includeJournal {
		var journalBuffer bytes.Buffer
		if journalSequence > 0 {
			if err := writeCommandJournalEntry(&journalBuffer, commandJournalEntry{
				Version:    commandJournalVersion,
				Sequence:   journalSequence,
				Checkpoint: true,
			}, journalFormat); err != nil {
				return BackupBundleManifest{}, err
			}
		}
		journalData = journalBuffer.Bytes()
		files = append(files, backupBundleBytesInfo(backupBundleJournalPath, journalData))
	}

	manifest := BackupBundleManifest{
		Version:         BackupBundleVersion,
		CreatedAt:       createdAt,
		Snapshot:        backupBundleSnapshotPath,
		SnapshotFormat:  string(snapshotFormat),
		JournalSequence: journalSequence,
		Files:           files,
		RestoreHint:     "extract snapshot.hc and commands.journal into DATA_DIR, then start with SNAPSHOT_PATH=DATA_DIR/snapshot.hc JOURNAL_PATH=DATA_DIR/commands.journal",
	}
	if includeJournal {
		manifest.Journal = backupBundleJournalPath
		manifest.JournalFormat = string(journalFormat)
	}

	manifestData, err := jsonwire.Marshal(manifest)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	manifestData = append(manifestData, '\n')

	if err := writeFileAtomicStream(path, func(writer io.Writer) error {
		return writeBackupBundleTarGzip(writer, snapshotPath, journalData, manifestData, createdAt, includeJournal)
	}); err != nil {
		return BackupBundleManifest{}, err
	}
	return manifest, nil
}

func writeBackupBundleTarGzip(writer io.Writer, snapshotPath string, journalData []byte, manifestData []byte, modTime time.Time, includeJournal bool) error {
	gzipWriter := gzip.NewWriter(writer)
	tarWriter := tar.NewWriter(gzipWriter)
	if err := writeBackupBundleBytes(tarWriter, backupBundleManifestPath, manifestData, modTime); err != nil {
		_ = tarWriter.Close()
		_ = gzipWriter.Close()
		return err
	}
	if err := writeBackupBundleFile(tarWriter, backupBundleSnapshotPath, snapshotPath, modTime); err != nil {
		_ = tarWriter.Close()
		_ = gzipWriter.Close()
		return err
	}
	if includeJournal {
		if err := writeBackupBundleBytes(tarWriter, backupBundleJournalPath, journalData, modTime); err != nil {
			_ = tarWriter.Close()
			_ = gzipWriter.Close()
			return err
		}
	}
	if err := tarWriter.Close(); err != nil {
		_ = gzipWriter.Close()
		return err
	}
	return gzipWriter.Close()
}

func writeBackupBundleFile(writer *tar.Writer, name string, path string, modTime time.Time) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("hatriecache: backup bundle file %s is not regular", path)
	}
	header := &tar.Header{
		Name:    name,
		Mode:    0o600,
		Size:    info.Size(),
		ModTime: modTime,
	}
	if err := writer.WriteHeader(header); err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(writer, file)
	return err
}

func writeBackupBundleBytes(writer *tar.Writer, name string, data []byte, modTime time.Time) error {
	header := &tar.Header{
		Name:    name,
		Mode:    0o600,
		Size:    int64(len(data)),
		ModTime: modTime,
	}
	if err := writer.WriteHeader(header); err != nil {
		return err
	}
	_, err := writer.Write(data)
	return err
}

func backupBundleFileInfo(name string, path string) (BackupBundleFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return BackupBundleFile{}, err
	}
	defer file.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return BackupBundleFile{}, err
	}
	return BackupBundleFile{Path: name, Size: size, SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

func backupBundleBytesInfo(name string, data []byte) BackupBundleFile {
	sum := sha256.Sum256(data)
	return BackupBundleFile{Path: name, Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
}
