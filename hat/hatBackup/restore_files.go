package hatBackup

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// MaxRestoreFileConcurrency bounds caller-configured restore workers.
const MaxRestoreFileConcurrency = 256

// ErrRestoreFileConcurrencyInvalid indicates an unsupported worker limit.
var ErrRestoreFileConcurrencyInvalid = errors.New("hatBackup: restore file concurrency is invalid")

// RestoreFile identifies one immutable source file and its checksum-verified
// restore destination.
type RestoreFile struct {
	Source      string
	Destination string
	Size        int64
	SHA256      string
}

// RestoreFileOptions controls bounded restore-file copies. Zero keeps the
// serial behavior; values above one enable bounded parallel copies.
type RestoreFileOptions struct {
	MaxConcurrency int
}

func (options RestoreFileOptions) Validate() error {
	if options.MaxConcurrency < 0 || options.MaxConcurrency > MaxRestoreFileConcurrency {
		return ErrRestoreFileConcurrencyInvalid
	}
	return nil
}

// CopyRestoreFiles copies independent immutable files and verifies every
// destination before returning. A failed copy leaves no partial destination
// for that file. The caller owns the staging directory and may remove files
// from successful sibling copies when the overall restore fails.
func CopyRestoreFiles(files []RestoreFile, options RestoreFileOptions) error {
	if err := options.Validate(); err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}
	for index, file := range files {
		if err := validateRestoreFile(file); err != nil {
			return fmt.Errorf("restore file %d (%s): %w", index, file.Destination, err)
		}
	}
	workers := options.MaxConcurrency
	if workers == 0 {
		workers = 1
	}
	if workers > len(files) {
		workers = len(files)
	}
	if workers == 1 {
		for index, file := range files {
			if err := copyRestoreFile(file); err != nil {
				return fmt.Errorf("restore file %d (%s): %w", index, file.Destination, err)
			}
		}
		return nil
	}

	errorsByFile := make([]error, len(files))
	jobs := make(chan int)
	var group sync.WaitGroup
	group.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer group.Done()
			for index := range jobs {
				errorsByFile[index] = copyRestoreFile(files[index])
			}
		}()
	}
	for index := range files {
		jobs <- index
	}
	close(jobs)
	group.Wait()
	for index, err := range errorsByFile {
		if err != nil {
			return fmt.Errorf("restore file %d (%s): %w", index, files[index].Destination, err)
		}
	}
	return nil
}

func copyRestoreFile(file RestoreFile) error {
	if err := os.MkdirAll(filepath.Dir(file.Destination), 0o700); err != nil {
		return err
	}
	source, err := os.Open(file.Source)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := os.OpenFile(file.Destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	removeTarget := true
	defer func() {
		if removeTarget {
			_ = os.Remove(file.Destination)
		}
	}()
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(target, hash), source)
	closeErr := target.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if size != file.Size || hex.EncodeToString(hash.Sum(nil)) != file.SHA256 {
		return errors.New("restore file checksum mismatch")
	}
	removeTarget = false
	return nil
}

func validateRestoreFile(file RestoreFile) error {
	if file.Source == "" || file.Destination == "" || file.Size < 0 {
		return errors.New("invalid restore file declaration")
	}
	if len(file.SHA256) != sha256.Size*2 {
		return errors.New("invalid restore file checksum")
	}
	if _, err := hex.DecodeString(file.SHA256); err != nil {
		return errors.New("invalid restore file checksum")
	}
	return nil
}
