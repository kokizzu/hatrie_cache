package hatCache

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	json "github.com/goccy/go-json"
)

const (
	snapshotExportCheckpointVersion = 1
	snapshotExportCopyChunkBytes    = 1 << 20
)

var (
	// ErrSnapshotExportCheckpointInvalid reports a malformed or unsafe resume
	// sidecar.
	ErrSnapshotExportCheckpointInvalid = errors.New("hatriecache: snapshot export checkpoint is invalid")
	// ErrSnapshotExportCheckpointMismatch reports a resume sidecar that does
	// not describe the requested target or snapshot.
	ErrSnapshotExportCheckpointMismatch = errors.New("hatriecache: snapshot export checkpoint mismatch")
)

// SnapshotExportOptions controls the opt-in resumable snapshot export path.
// An existing checkpoint is resumed automatically. The regular SaveSnapshot
// and WriteSnapshotWithManifest paths do not create sidecar files.
type SnapshotExportOptions struct {
	Format         SnapshotFormat `json:"format"`
	CheckpointPath string         `json:"checkpoint_path,omitempty"`
}

// SnapshotExportReport identifies the atomically published snapshot.
type SnapshotExportReport struct {
	Manifest     SnapshotManifest
	BytesWritten int64
	Resumed      bool
}

type snapshotExportCheckpoint struct {
	Version      int              `json:"version"`
	TargetPath   string           `json:"target_path"`
	SourcePath   string           `json:"source_path"`
	PartialPath  string           `json:"partial_path"`
	Format       SnapshotFormat   `json:"format"`
	Manifest     SnapshotManifest `json:"manifest"`
	BytesWritten int64            `json:"bytes_written"`
	PrefixSHA256 string           `json:"prefix_sha256"`
}

// WriteSnapshotWithResumableExport stages and atomically publishes one
// snapshot. If the process or destination fails during transfer, the source
// snapshot and checkpoint remain and the next call continues at the last
// fsynced byte without duplicating data.
func (journal *CommandJournal) WriteSnapshotWithResumableExport(trie *HatTrie, path string, options SnapshotExportOptions) (SnapshotExportReport, error) {
	if journal == nil {
		return SnapshotExportReport{}, ErrNilCommandJournal
	}
	if trie == nil {
		return SnapshotExportReport{}, ErrNilHatTrie
	}
	if path == "" {
		return SnapshotExportReport{}, errors.New("hatriecache: snapshot export path is required")
	}
	targetPath, err := filepath.Abs(path)
	if err != nil {
		return SnapshotExportReport{}, err
	}
	requestedFormat := options.Format
	format, err := ParseSnapshotFormat(string(requestedFormat))
	if err != nil {
		return SnapshotExportReport{}, err
	}
	checkpointPath := options.CheckpointPath
	if checkpointPath == "" {
		checkpointPath = targetPath + ".resume.json"
	} else if checkpointPath, err = filepath.Abs(checkpointPath); err != nil {
		return SnapshotExportReport{}, err
	}
	if filepath.Clean(checkpointPath) == filepath.Clean(targetPath) {
		return SnapshotExportReport{}, fmt.Errorf("%w: checkpoint path equals target path", ErrSnapshotExportCheckpointInvalid)
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o700); err != nil {
		return SnapshotExportReport{}, err
	}
	if err := os.MkdirAll(filepath.Dir(checkpointPath), 0o700); err != nil {
		return SnapshotExportReport{}, err
	}

	checkpoint, err := readSnapshotExportCheckpoint(checkpointPath)
	resumed := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return SnapshotExportReport{}, err
	}
	if !resumed {
		sourcePath := snapshotExportSourcePath(targetPath)
		partialPath := snapshotExportPartialPath(targetPath)
		if err := removeSnapshotExportPath(partialPath); err != nil {
			return SnapshotExportReport{}, err
		}
		var manifest SnapshotManifest
		if err := writeFileAtomicStream(sourcePath, func(writer io.Writer) error {
			var writeErr error
			manifest, writeErr = journal.WriteSnapshotWithManifest(trie, writer, format)
			return writeErr
		}); err != nil {
			return SnapshotExportReport{}, err
		}
		checkpoint = snapshotExportCheckpoint{
			Version:      snapshotExportCheckpointVersion,
			TargetPath:   targetPath,
			SourcePath:   sourcePath,
			PartialPath:  partialPath,
			Format:       format,
			Manifest:     manifest,
			BytesWritten: 0,
			PrefixSHA256: snapshotExportPrefixDigest(nil),
		}
		if err := writeSnapshotExportCheckpoint(checkpointPath, checkpoint); err != nil {
			_ = os.Remove(sourcePath)
			return SnapshotExportReport{}, err
		}
	} else {
		if requestedFormat != "" && checkpoint.Format != format {
			return SnapshotExportReport{}, fmt.Errorf("%w: format=%q want=%q", ErrSnapshotExportCheckpointMismatch, checkpoint.Format, format)
		}
		format = checkpoint.Format
		if _, err := ParseSnapshotFormat(string(format)); err != nil {
			return SnapshotExportReport{}, fmt.Errorf("%w: %v", ErrSnapshotExportCheckpointInvalid, err)
		}
	}
	if checkpoint.TargetPath != targetPath ||
		checkpoint.SourcePath != snapshotExportSourcePath(targetPath) ||
		checkpoint.PartialPath != snapshotExportPartialPath(targetPath) ||
		checkpoint.Format != format ||
		checkpoint.Manifest.Format != format {
		return SnapshotExportReport{}, fmt.Errorf("%w: paths or format do not match", ErrSnapshotExportCheckpointMismatch)
	}
	if err := validateSnapshotExportCheckpoint(checkpoint); err != nil {
		return SnapshotExportReport{}, err
	}
	if err := resumeSnapshotExport(checkpointPath, &checkpoint); err != nil {
		return SnapshotExportReport{}, err
	}
	return SnapshotExportReport{
		Manifest:     checkpoint.Manifest,
		BytesWritten: checkpoint.BytesWritten,
		Resumed:      resumed,
	}, nil
}

func snapshotExportSourcePath(targetPath string) string {
	return targetPath + ".resume-source"
}

func snapshotExportPartialPath(targetPath string) string {
	return targetPath + ".resume-partial"
}

func writeSnapshotExportCheckpoint(path string, checkpoint snapshotExportCheckpoint) error {
	if err := validateSnapshotExportCheckpoint(checkpoint); err != nil {
		return err
	}
	return writeJSONFileAtomic(path, checkpoint)
}

func readSnapshotExportCheckpoint(path string) (snapshotExportCheckpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return snapshotExportCheckpoint{}, err
	}
	var checkpoint snapshotExportCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return snapshotExportCheckpoint{}, fmt.Errorf("%w: %v", ErrSnapshotExportCheckpointInvalid, err)
	}
	if err := validateSnapshotExportCheckpoint(checkpoint); err != nil {
		return snapshotExportCheckpoint{}, err
	}
	return checkpoint, nil
}

func validateSnapshotExportCheckpoint(checkpoint snapshotExportCheckpoint) error {
	if checkpoint.Version != snapshotExportCheckpointVersion ||
		checkpoint.TargetPath == "" ||
		checkpoint.SourcePath == "" ||
		checkpoint.PartialPath == "" ||
		checkpoint.Format == "" ||
		checkpoint.Manifest.Format != checkpoint.Format ||
		checkpoint.Manifest.SizeBytes < 0 ||
		checkpoint.BytesWritten < 0 ||
		checkpoint.BytesWritten > checkpoint.Manifest.SizeBytes {
		return ErrSnapshotExportCheckpointInvalid
	}
	if filepath.Clean(checkpoint.SourcePath) != filepath.Clean(snapshotExportSourcePath(checkpoint.TargetPath)) ||
		filepath.Clean(checkpoint.PartialPath) != filepath.Clean(snapshotExportPartialPath(checkpoint.TargetPath)) ||
		filepath.Clean(checkpoint.TargetPath) == filepath.Clean(checkpoint.SourcePath) ||
		filepath.Clean(checkpoint.TargetPath) == filepath.Clean(checkpoint.PartialPath) {
		return ErrSnapshotExportCheckpointInvalid
	}
	if len(checkpoint.PrefixSHA256) != sha256.Size*2 {
		return ErrSnapshotExportCheckpointInvalid
	}
	if _, err := hex.DecodeString(checkpoint.PrefixSHA256); err != nil {
		return ErrSnapshotExportCheckpointInvalid
	}
	return nil
}

func resumeSnapshotExport(checkpointPath string, checkpoint *snapshotExportCheckpoint) error {
	if checkpoint.BytesWritten == checkpoint.Manifest.SizeBytes {
		if err := VerifySnapshotManifest(checkpoint.TargetPath, checkpoint.Manifest); err == nil {
			if err := removeSnapshotExportPath(checkpoint.SourcePath); err != nil {
				return err
			}
			return removeSnapshotExportPath(checkpointPath)
		}
	}
	if err := validateSnapshotExportDataPath(checkpoint.SourcePath); err != nil {
		return err
	}
	if err := validateSnapshotExportDataPath(checkpoint.PartialPath); err != nil {
		return err
	}
	source, err := os.Open(checkpoint.SourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	sourceInfo, err := source.Stat()
	if err != nil {
		return err
	}
	if sourceInfo.Size() != checkpoint.Manifest.SizeBytes {
		return fmt.Errorf("%w: source size=%d want=%d", ErrSnapshotExportCheckpointMismatch, sourceInfo.Size(), checkpoint.Manifest.SizeBytes)
	}

	partial, err := os.OpenFile(checkpoint.PartialPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer partial.Close()
	partialInfo, err := partial.Stat()
	if err != nil {
		return err
	}
	if partialInfo.Size() < checkpoint.BytesWritten {
		return fmt.Errorf("%w: partial size=%d committed=%d", ErrSnapshotExportCheckpointMismatch, partialInfo.Size(), checkpoint.BytesWritten)
	}
	if partialInfo.Size() > checkpoint.BytesWritten {
		if err := partial.Truncate(checkpoint.BytesWritten); err != nil {
			return err
		}
		if err := partial.Sync(); err != nil {
			return err
		}
	}
	prefixDigest, err := snapshotExportFilePrefixHash(partial, checkpoint.BytesWritten)
	if err != nil {
		return err
	}
	if got := hex.EncodeToString(prefixDigest.Sum(nil)); got != checkpoint.PrefixSHA256 {
		return fmt.Errorf("%w: committed prefix digest mismatch", ErrSnapshotExportCheckpointMismatch)
	}
	if _, err := source.Seek(checkpoint.BytesWritten, io.SeekStart); err != nil {
		return err
	}
	if _, err := partial.Seek(checkpoint.BytesWritten, io.SeekStart); err != nil {
		return err
	}
	bufferSize := int64(snapshotExportCopyChunkBytes)
	remaining := checkpoint.Manifest.SizeBytes - checkpoint.BytesWritten
	if remaining < bufferSize {
		bufferSize = remaining
	}
	buffer := make([]byte, int(bufferSize))
	for checkpoint.BytesWritten < checkpoint.Manifest.SizeBytes {
		n, readErr := source.Read(buffer)
		if n > 0 {
			written, writeErr := partial.Write(buffer[:n])
			if writeErr != nil {
				return writeErr
			}
			if written != n {
				return io.ErrShortWrite
			}
			if _, err := prefixDigest.Write(buffer[:n]); err != nil {
				return err
			}
			checkpoint.BytesWritten += int64(n)
			checkpoint.PrefixSHA256 = hex.EncodeToString(prefixDigest.Sum(nil))
			if err := partial.Sync(); err != nil {
				return err
			}
			if err := writeSnapshotExportCheckpoint(checkpointPath, *checkpoint); err != nil {
				return err
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return readErr
		}
	}
	if checkpoint.BytesWritten != checkpoint.Manifest.SizeBytes {
		return fmt.Errorf("%w: copied=%d want=%d", ErrSnapshotExportCheckpointMismatch, checkpoint.BytesWritten, checkpoint.Manifest.SizeBytes)
	}
	if err := partial.Sync(); err != nil {
		return err
	}
	if err := partial.Close(); err != nil {
		return err
	}
	if err := VerifySnapshotManifest(checkpoint.PartialPath, checkpoint.Manifest); err != nil {
		return err
	}
	if err := os.Rename(checkpoint.PartialPath, checkpoint.TargetPath); err != nil {
		return err
	}
	if err := syncDirectory(filepath.Dir(checkpoint.TargetPath)); err != nil {
		return err
	}
	if err := removeSnapshotExportPath(checkpoint.SourcePath); err != nil {
		return err
	}
	return removeSnapshotExportPath(checkpointPath)
}

func snapshotExportPrefixDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func snapshotExportFilePrefixHash(file *os.File, size int64) (hash.Hash, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	digest := sha256.New()
	if _, err := io.CopyN(digest, file, size); err != nil {
		return nil, err
	}
	return digest, nil
}

func removeSnapshotExportPath(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func validateSnapshotExportDataPath(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: data path is a symlink", ErrSnapshotExportCheckpointInvalid)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%w: data path is not a regular file", ErrSnapshotExportCheckpointInvalid)
	}
	return nil
}
