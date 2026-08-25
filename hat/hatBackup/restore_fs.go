package hatBackup

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// RestoreDestination is an isolated staging directory and its final restore
// location. A destination must be published explicitly after the caller has
// populated and verified StagingPath.
type RestoreDestination struct {
	target  string
	staging string
}

// PrepareRestoreDestination validates a restore target and allocates a
// sibling staging directory. It rejects source/target overlap and symlinks in
// the destination path to prevent destructive or redirected restores.
func PrepareRestoreDestination(source string, dataDir string, overwrite bool) (RestoreDestination, error) {
	target, err := filepath.Abs(dataDir)
	if err != nil {
		return RestoreDestination{}, err
	}
	target = filepath.Clean(target)
	parent := filepath.Dir(target)
	if target == parent {
		return RestoreDestination{}, errors.New("hatriecache: restore data dir must not be a filesystem root")
	}
	if err := ValidateRestorePathSeparation(source, target); err != nil {
		return RestoreDestination{}, err
	}
	if err := RejectRestoreSymlinkComponents(parent); err != nil {
		return RestoreDestination{}, err
	}
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return RestoreDestination{}, err
	}
	if err := RejectRestoreSymlinkComponents(parent); err != nil {
		return RestoreDestination{}, err
	}
	if _, err := ValidateRestoreTarget(target, overwrite); err != nil {
		return RestoreDestination{}, err
	}
	staging, err := os.MkdirTemp(parent, "."+filepath.Base(target)+".restore-stage-*")
	if err != nil {
		return RestoreDestination{}, err
	}
	return RestoreDestination{target: target, staging: staging}, nil
}

// TargetPath returns the final restore directory.
func (destination RestoreDestination) TargetPath() string { return destination.target }

// StagingPath returns the isolated directory callers should populate.
func (destination RestoreDestination) StagingPath() string { return destination.staging }

// Cleanup removes an unpublished staging directory. It is safe after Publish.
func (destination RestoreDestination) Cleanup() { _ = os.RemoveAll(destination.staging) }

// Publish atomically replaces the restore target with the durable staging
// directory. Existing target data is restored when publication cannot finish.
func (destination RestoreDestination) Publish(overwrite bool) error {
	exists, err := ValidateRestoreTarget(destination.target, overwrite)
	if err != nil {
		return err
	}
	parent := filepath.Dir(destination.target)
	if !exists {
		if err := os.Rename(destination.staging, destination.target); err != nil {
			return err
		}
		if err := syncDirectory(parent); err != nil {
			rollbackErr := os.Rename(destination.target, destination.staging)
			if rollbackErr != nil {
				return fmt.Errorf("hatriecache: sync published restore: %w; rollback failed: %v", err, rollbackErr)
			}
			return err
		}
		return nil
	}
	oldPath, err := os.MkdirTemp(parent, "."+filepath.Base(destination.target)+".restore-old-*")
	if err != nil {
		return err
	}
	if err := os.Remove(oldPath); err != nil {
		return err
	}
	if err := os.Rename(destination.target, oldPath); err != nil {
		return err
	}
	if err := os.Rename(destination.staging, destination.target); err != nil {
		rollbackErr := os.Rename(oldPath, destination.target)
		if rollbackErr != nil {
			return fmt.Errorf("hatriecache: publish restore: %w; rollback failed: %v", err, rollbackErr)
		}
		return err
	}
	if err := syncDirectory(parent); err != nil {
		rollbackErr := rollbackPublishedRestore(destination, oldPath)
		if rollbackErr != nil {
			return fmt.Errorf("hatriecache: sync published restore: %w; rollback failed: %v", err, rollbackErr)
		}
		return err
	}
	if err := os.RemoveAll(oldPath); err != nil {
		return err
	}
	return syncDirectory(parent)
}

// RejectRestoreSymlinkComponents rejects any existing symlink below path's
// filesystem root.
func RejectRestoreSymlinkComponents(path string) error {
	volume := filepath.VolumeName(path)
	root := volume + string(os.PathSeparator)
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}
	current := root
	for _, component := range strings.Split(relative, string(os.PathSeparator)) {
		if component == "" || component == "." {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("hatriecache: restore data dir path contains symlink: %s", current)
		}
	}
	return nil
}

// ValidateRestorePathSeparation rejects restores whose source and target
// overlap in either direction.
func ValidateRestorePathSeparation(source string, target string) error {
	sourceAbs, err := filepath.Abs(source)
	if err != nil {
		return err
	}
	sourceAbs = filepath.Clean(sourceAbs)
	if evaluated, err := filepath.EvalSymlinks(sourceAbs); err == nil {
		sourceAbs = filepath.Clean(evaluated)
	}
	if SameOrChildPath(target, sourceAbs) || SameOrChildPath(sourceAbs, target) {
		return fmt.Errorf("hatriecache: restore source and data dir must not overlap: %s", target)
	}
	return nil
}

// ValidateRestoreTarget validates destination existence and overwrite policy.
func ValidateRestoreTarget(target string, overwrite bool) (bool, error) {
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return false, fmt.Errorf("hatriecache: restore data dir must not be a symlink: %s", target)
	}
	if !info.IsDir() {
		return false, fmt.Errorf("hatriecache: restore data dir is not a directory: %s", target)
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		return false, err
	}
	if len(entries) > 0 && !overwrite {
		return false, fmt.Errorf("hatriecache: restore data dir is not empty: %s", target)
	}
	return true, nil
}

// SyncRestoreTree rejects non-regular filesystem objects and synchronizes all
// files and directories in deepest-first order before publication.
func SyncRestoreTree(root string) error {
	directories := make([]string, 0, 8)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("hatriecache: restore staging contains symlink: %s", path)
		}
		if entry.IsDir() {
			directories = append(directories, path)
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("hatriecache: restore staging contains non-regular file: %s", path)
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		syncErr := file.Sync()
		closeErr := file.Close()
		if syncErr != nil {
			return syncErr
		}
		return closeErr
	})
	if err != nil {
		return err
	}
	sort.Slice(directories, func(left, right int) bool {
		return strings.Count(directories[left], string(os.PathSeparator)) > strings.Count(directories[right], string(os.PathSeparator))
	})
	for _, directory := range directories {
		if err := syncDirectory(directory); err != nil {
			return err
		}
	}
	return nil
}

// EnsureRestoreDataDir creates an empty restore directory or clears it when
// overwrite is enabled.
func EnsureRestoreDataDir(path string, overwrite bool) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	if len(entries) > 0 && !overwrite {
		return fmt.Errorf("hatriecache: restore data dir is not empty: %s", path)
	}
	if overwrite {
		for _, entry := range entries {
			if err := os.RemoveAll(filepath.Join(path, entry.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// CopyBackupDirectory safely copies a backup directory into a separate
// rehearsal work directory.
func CopyBackupDirectory(source string, destination string) error {
	sourceAbs, err := filepath.Abs(source)
	if err != nil {
		return err
	}
	destinationAbs, err := filepath.Abs(destination)
	if err != nil {
		return err
	}
	if SameOrChildPath(sourceAbs, destinationAbs) {
		return fmt.Errorf("hatriecache: restore rehearsal work dir must not be inside backup directory: %s", destination)
	}
	if SameOrChildPath(destinationAbs, sourceAbs) {
		return fmt.Errorf("hatriecache: restore rehearsal work dir must not contain backup directory: %s", destination)
	}
	return copyDirectoryContents(sourceAbs, destinationAbs)
}

// SameOrChildPath reports whether child equals or is nested below parent.
func SameOrChildPath(parent string, child string) bool {
	relative, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator)))
}

func rollbackPublishedRestore(destination RestoreDestination, oldPath string) error {
	if err := os.Rename(destination.target, destination.staging); err != nil {
		return err
	}
	if err := os.Rename(oldPath, destination.target); err != nil {
		restoreErr := os.Rename(destination.staging, destination.target)
		if restoreErr != nil {
			return fmt.Errorf("restore old directory: %w; restoring new directory also failed: %v", err, restoreErr)
		}
		return err
	}
	return syncDirectory(filepath.Dir(destination.target))
}

func copyDirectoryContents(source string, destination string) error {
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		sourcePath := filepath.Join(source, entry.Name())
		destinationPath := filepath.Join(destination, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("hatriecache: restore rehearsal refuses symlink in backup directory: %s", sourcePath)
		}
		if info.IsDir() {
			if err := os.MkdirAll(destinationPath, info.Mode().Perm()); err != nil {
				return err
			}
			if err := copyDirectoryContents(sourcePath, destinationPath); err != nil {
				return err
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("hatriecache: restore rehearsal refuses non-regular backup file: %s", sourcePath)
		}
		if err := copyRegularFile(sourcePath, destinationPath, info.Mode().Perm()); err != nil {
			return err
		}
	}
	return nil
}

func copyRegularFile(source string, destination string, mode os.FileMode) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func syncDirectory(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
