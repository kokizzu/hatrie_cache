package hatStorage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrStorageTierReaderInvalid = errors.New("hatriecache: storage tier reader is invalid")
	ErrStorageTierPartNotFound  = errors.New("hatriecache: storage tier part was not found")
)

// StorageTierReadPart identifies a part's current placement and age. The
// current tier is tried first because a move may be pending or incomplete.
type StorageTierReadPart struct {
	Key         string
	CurrentTier string
	Age         time.Duration
}

// StorageTierReadFunc reads bytes from one selected tier. The reader owns the
// returned bytes; StorageTierReader never copies them.
type StorageTierReadFunc func(context.Context, StorageTierSelection) ([]byte, error)

// StorageTierReadResult reports the selected tier and whether a not-found
// current-tier read required an age-selected fallback.
type StorageTierReadResult struct {
	Data      []byte
	Selection StorageTierSelection
	Fallback  bool
}

// StorageTierReader routes reads through an immutable tier policy. It does not
// move files, persist metadata, or assume a local/object-store implementation.
type StorageTierReader struct {
	policy StorageTierPolicy
	read   StorageTierReadFunc
}

// NewStorageTierReader creates an injected transparent tier read router.
func NewStorageTierReader(policy StorageTierPolicy, read StorageTierReadFunc) (*StorageTierReader, error) {
	if len(policy.rules) == 0 || read == nil {
		return nil, ErrStorageTierReaderInvalid
	}
	return &StorageTierReader{policy: policy, read: read}, nil
}

// Read tries the part's current tier first. If that read returns
// ErrStorageTierPartNotFound and the policy's age-selected tier differs, it
// retries the selected tier. Other errors are returned without a fallback.
func (reader *StorageTierReader) Read(ctx context.Context, part StorageTierReadPart) (StorageTierReadResult, error) {
	if reader == nil || ctx == nil || part.Age < 0 {
		return StorageTierReadResult{}, ErrStorageTierReaderInvalid
	}
	key := strings.TrimSpace(part.Key)
	currentTier := strings.TrimSpace(part.CurrentTier)
	if len(reader.policy.rules) == 0 || key == "" || currentTier == "" {
		return StorageTierReadResult{}, ErrStorageTierReaderInvalid
	}
	current, err := reader.policy.selectStorageTier(currentTier, key)
	if err != nil {
		return StorageTierReadResult{}, fmt.Errorf("%w: current tier: %v", ErrStorageTierReaderInvalid, err)
	}
	data, readErr := reader.read(ctx, current)
	if readErr == nil {
		return StorageTierReadResult{Data: data, Selection: current}, nil
	}
	if !errors.Is(readErr, ErrStorageTierPartNotFound) {
		return StorageTierReadResult{}, readErr
	}
	desired, err := reader.policy.Select(part.Age, key)
	if err != nil {
		return StorageTierReadResult{}, fmt.Errorf("%w: age-selected tier: %v", ErrStorageTierReaderInvalid, err)
	}
	if desired.Tier == current.Tier {
		return StorageTierReadResult{Selection: current}, readErr
	}
	data, fallbackErr := reader.read(ctx, desired)
	if fallbackErr != nil {
		return StorageTierReadResult{Selection: desired, Fallback: true}, fmt.Errorf("storage tier fallback %q: %w", desired.Tier, fallbackErr)
	}
	return StorageTierReadResult{Data: data, Selection: desired, Fallback: true}, nil
}
