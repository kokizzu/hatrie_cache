package hatCache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	restoreResumeCheckpointVersion = 1
	restoreResumePhasePrepared     = "prepared"
	restoreResumePhaseMaterialized = "materialized"
	restoreResumePhaseVerified     = "verified"
)

// ErrRestoreResumeCheckpointMismatch indicates that a retained resume sidecar
// belongs to a different source, target, manifest, or restore plan.
var ErrRestoreResumeCheckpointMismatch = errors.New("hatriecache: restore resume checkpoint does not match requested restore")

type restoreResumeCheckpoint struct {
	Version    int    `json:"version"`
	Source     string `json:"source"`
	Target     string `json:"target"`
	PlanDigest string `json:"plan_digest"`
	Phase      string `json:"phase"`
}

func restoreResumeCheckpointPath(target string) string {
	clean, err := filepath.Abs(target)
	if err != nil {
		clean = filepath.Clean(target)
	}
	return filepath.Join(filepath.Dir(clean), "."+filepath.Base(clean)+".restore-resume.checkpoint")
}

func ensureRestoreResumeCheckpoint(destination restoreDestination, source string, manifest BackupBundleManifest, options BackupBundleRestoreOptions) error {
	expected, err := newRestoreResumeCheckpoint(destination.TargetPath(), source, manifest, options)
	if err != nil {
		return err
	}
	path := restoreResumeCheckpointPath(destination.TargetPath())
	current, err := readRestoreResumeCheckpoint(path)
	if errors.Is(err, os.ErrNotExist) {
		return writeRestoreResumeCheckpoint(path, expected)
	}
	if err != nil {
		return err
	}
	if current != expected && (current.Version != expected.Version || current.Source != expected.Source || current.Target != expected.Target || current.PlanDigest != expected.PlanDigest) {
		return fmt.Errorf("%w: %s", ErrRestoreResumeCheckpointMismatch, path)
	}
	if current.Phase == "" {
		return fmt.Errorf("%w: checkpoint phase is empty: %s", ErrRestoreResumeCheckpointMismatch, path)
	}
	return nil
}

func newRestoreResumeCheckpoint(target string, source string, manifest BackupBundleManifest, options BackupBundleRestoreOptions) (restoreResumeCheckpoint, error) {
	sourceAbs, err := filepath.Abs(source)
	if err != nil {
		return restoreResumeCheckpoint{}, err
	}
	targetAbs, err := filepath.Abs(target)
	if err != nil {
		return restoreResumeCheckpoint{}, err
	}
	plan, err := json.Marshal(struct {
		Source   string
		Target   string
		Manifest BackupBundleManifest
		Options  BackupBundleRestoreOptions
	}{
		Source:   filepath.Clean(sourceAbs),
		Target:   filepath.Clean(targetAbs),
		Manifest: manifest,
		Options:  options,
	})
	if err != nil {
		return restoreResumeCheckpoint{}, err
	}
	digest := sha256.Sum256(plan)
	return restoreResumeCheckpoint{
		Version:    restoreResumeCheckpointVersion,
		Source:     filepath.Clean(sourceAbs),
		Target:     filepath.Clean(targetAbs),
		PlanDigest: hex.EncodeToString(digest[:]),
		Phase:      restoreResumePhasePrepared,
	}, nil
}

func updateRestoreResumeCheckpoint(destination restoreDestination, phase string) error {
	path := restoreResumeCheckpointPath(destination.TargetPath())
	checkpoint, err := readRestoreResumeCheckpoint(path)
	if err != nil {
		return err
	}
	if checkpoint.Version != restoreResumeCheckpointVersion || phase == "" {
		return fmt.Errorf("%w: %s", ErrRestoreResumeCheckpointMismatch, path)
	}
	checkpoint.Phase = phase
	return writeRestoreResumeCheckpoint(path, checkpoint)
}

func clearRestoreResumeCheckpoint(destination restoreDestination) error {
	path := restoreResumeCheckpointPath(destination.TargetPath())
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncRestoreResumeDirectory(filepath.Dir(path))
}

func readRestoreResumeCheckpoint(path string) (restoreResumeCheckpoint, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return restoreResumeCheckpoint{}, err
	}
	var checkpoint restoreResumeCheckpoint
	if err := json.Unmarshal(encoded, &checkpoint); err != nil {
		return restoreResumeCheckpoint{}, fmt.Errorf("%w: invalid checkpoint %s: %v", ErrRestoreResumeCheckpointMismatch, path, err)
	}
	return checkpoint, nil
}

func writeRestoreResumeCheckpoint(path string, checkpoint restoreResumeCheckpoint) error {
	encoded, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	parent := filepath.Dir(path)
	temporary, err := os.CreateTemp(parent, ".restore-resume-checkpoint-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	removeTemporary = false
	return syncRestoreResumeDirectory(parent)
}

func syncRestoreResumeDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
