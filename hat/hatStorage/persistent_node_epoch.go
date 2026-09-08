package hatStorage

import (
	"path/filepath"
	"strings"
	"time"
)

const persistentNodeEpochShardID = "__hatrie_node_epoch__"

// These aliases let callers handle node epochs independently while preserving
// the lease errors and behavior already used by the underlying implementation.
var (
	// ErrPersistentNodeEpochInvalid reports invalid epoch input or state.
	ErrPersistentNodeEpochInvalid = ErrPersistentShardLeaseInvalid
	// ErrPersistentNodeEpochHeld reports that another process owns the epoch.
	ErrPersistentNodeEpochHeld = ErrPersistentShardLeaseHeld
	// ErrPersistentNodeEpochFenced reports a stale epoch.
	ErrPersistentNodeEpochFenced = ErrPersistentShardLeaseFenced
	// ErrPersistentNodeEpochReleased reports use after Release.
	ErrPersistentNodeEpochReleased = ErrPersistentShardLeaseReleased
	// ErrPersistentNodeEpochNotHeld reports validation after the owner released.
	ErrPersistentNodeEpochNotHeld = ErrPersistentShardLeaseNotHeld
	// ErrPersistentNodeEpochUnsupported reports platforms without file locks.
	ErrPersistentNodeEpochUnsupported = ErrPersistentShardLeaseUnsupported
)

// PersistentNodeEpochInfo is the durable ownership record for one node epoch.
// Active is determined from the lock file; the epoch and timestamps come from
// the atomically replaced state record.
type PersistentNodeEpochInfo struct {
	Owner      string    `json:"owner"`
	Epoch      uint64    `json:"epoch"`
	AcquiredAt time.Time `json:"acquired_at"`
	RenewedAt  time.Time `json:"renewed_at"`
	Active     bool      `json:"active"`
}

// PersistentNodeEpoch is a local-filesystem-scoped generation held by one
// process. A new acquisition durably receives a larger epoch after restart.
// Call Validate immediately before each durable mutation that carries it.
type PersistentNodeEpoch struct {
	lease *PersistentShardLease
}

// AcquirePersistentNodeEpoch claims the node epoch for owner. It returns
// ErrPersistentNodeEpochHeld immediately when another process owns it. The
// epoch is incremented and durably recorded before this returns.
func AcquirePersistentNodeEpoch(storagePath, owner string) (*PersistentNodeEpoch, error) {
	storagePath, owner, err := normalizePersistentNodeEpochInput(storagePath, owner)
	if err != nil {
		return nil, err
	}
	lease, err := AcquirePersistentShardLease(persistentNodeEpochStoragePath(storagePath), persistentNodeEpochShardID, owner)
	if err != nil {
		return nil, err
	}
	return &PersistentNodeEpoch{lease: lease}, nil
}

// Epoch returns the immutable generation assigned at acquisition.
func (epoch *PersistentNodeEpoch) Epoch() uint64 {
	if epoch == nil || epoch.lease == nil {
		return 0
	}
	return epoch.lease.Token()
}

// Owner returns the owner identity supplied at acquisition.
func (epoch *PersistentNodeEpoch) Owner() string {
	if epoch == nil || epoch.lease == nil {
		return ""
	}
	return epoch.lease.Owner()
}

// Validate confirms that this epoch is still active and has not been fenced
// by a later owner.
func (epoch *PersistentNodeEpoch) Validate() error {
	if epoch == nil || epoch.lease == nil {
		return ErrPersistentNodeEpochInvalid
	}
	return epoch.lease.Validate()
}

// Renew refreshes the durable liveness timestamp without changing the epoch.
// Ownership remains held until Release or process exit.
func (epoch *PersistentNodeEpoch) Renew() error {
	if epoch == nil || epoch.lease == nil {
		return ErrPersistentNodeEpochInvalid
	}
	return epoch.lease.Renew()
}

// Release relinquishes the epoch. It is idempotent and leaves the last epoch
// in the durable state so the next owner always receives a larger value.
func (epoch *PersistentNodeEpoch) Release() error {
	if epoch == nil || epoch.lease == nil {
		return nil
	}
	return epoch.lease.Release()
}

// ValidatePersistentNodeEpoch checks an epoch without requiring the caller to
// retain the epoch object. It is intended for write-path fencing checks.
func ValidatePersistentNodeEpoch(storagePath string, epoch uint64) error {
	storagePath, err := normalizePersistentNodeEpochStoragePath(storagePath)
	if err != nil {
		return err
	}
	return ValidatePersistentShardLeaseToken(persistentNodeEpochStoragePath(storagePath), persistentNodeEpochShardID, epoch)
}

// InspectPersistentNodeEpoch reads the durable owner record and reports
// whether its lock is currently held. It is observational and never acquires
// the epoch.
func InspectPersistentNodeEpoch(storagePath string) (PersistentNodeEpochInfo, error) {
	storagePath, err := normalizePersistentNodeEpochStoragePath(storagePath)
	if err != nil {
		return PersistentNodeEpochInfo{}, err
	}
	info, err := InspectPersistentShardLease(persistentNodeEpochStoragePath(storagePath), persistentNodeEpochShardID)
	if err != nil {
		return PersistentNodeEpochInfo{}, err
	}
	return PersistentNodeEpochInfo{
		Owner:      info.Owner,
		Epoch:      info.Token,
		AcquiredAt: info.AcquiredAt,
		RenewedAt:  info.RenewedAt,
		Active:     info.Active,
	}, nil
}

func normalizePersistentNodeEpochInput(storagePath, owner string) (string, string, error) {
	storagePath, _, owner, err := normalizePersistentShardLeaseInput(storagePath, persistentNodeEpochShardID, owner)
	if err != nil {
		return "", "", err
	}
	return storagePath, owner, nil
}

func normalizePersistentNodeEpochStoragePath(storagePath string) (string, error) {
	storagePath = strings.TrimSpace(storagePath)
	if storagePath == "" {
		return "", ErrPersistentNodeEpochInvalid
	}
	normalized, _, _, err := normalizePersistentShardLeaseInput(storagePath, persistentNodeEpochShardID, "epoch-inspection")
	if err != nil {
		return "", err
	}
	return normalized, nil
}

func persistentNodeEpochStoragePath(storagePath string) string {
	return filepath.Clean(storagePath) + ".node-epoch"
}
