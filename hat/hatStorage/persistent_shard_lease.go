package hatStorage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var (
	// ErrPersistentShardLeaseInvalid reports invalid lease input or state.
	ErrPersistentShardLeaseInvalid = errors.New("hatriecache: persistent shard lease is invalid")
	// ErrPersistentShardLeaseHeld reports that another process owns the shard.
	ErrPersistentShardLeaseHeld = errors.New("hatriecache: persistent shard lease is held")
	// ErrPersistentShardLeaseFenced reports a stale fencing token.
	ErrPersistentShardLeaseFenced = errors.New("hatriecache: persistent shard lease is fenced")
	// ErrPersistentShardLeaseReleased reports use after Release.
	ErrPersistentShardLeaseReleased = errors.New("hatriecache: persistent shard lease is released")
	// ErrPersistentShardLeaseNotHeld reports validation after the owner released.
	ErrPersistentShardLeaseNotHeld = errors.New("hatriecache: persistent shard lease is not held")
	// ErrPersistentShardLeaseUnsupported reports platforms without advisory file locks.
	ErrPersistentShardLeaseUnsupported = errors.New("hatriecache: persistent shard leases are unsupported on this platform")
)

const (
	persistentShardLeaseStateVersion = 1
	persistentShardLeaseMaxStateSize = 16 << 10
	persistentShardLeaseMaxShardID   = 1024
	persistentShardLeaseMaxOwner     = 1024
)

// PersistentShardLeaseInfo is the durable ownership record for one shard.
// Active is determined from the lock file, while the token and timestamps come
// from the atomically replaced state record.
type PersistentShardLeaseInfo struct {
	ShardID    string    `json:"shard_id"`
	Owner      string    `json:"owner"`
	Token      uint64    `json:"token"`
	AcquiredAt time.Time `json:"acquired_at"`
	RenewedAt  time.Time `json:"renewed_at"`
	Active     bool      `json:"active"`
}

type persistentShardLeaseState struct {
	Version    int       `json:"version"`
	ShardID    string    `json:"shard_id"`
	Owner      string    `json:"owner"`
	Token      uint64    `json:"token"`
	AcquiredAt time.Time `json:"acquired_at"`
	RenewedAt  time.Time `json:"renewed_at"`
}

// PersistentShardLease is a non-blocking lease on one persistent shard. The
// lock is advisory and local-filesystem scoped; callers must validate the
// fencing token immediately before every durable mutation.
type PersistentShardLease struct {
	mu          sync.Mutex
	lockFile    *os.File
	storagePath string
	shardID     string
	owner       string
	token       uint64
	statePath   string
	released    bool
}

// AcquirePersistentShardLease claims shardID for owner. It returns
// ErrPersistentShardLeaseHeld immediately when another process owns it. The
// fencing token is incremented and durably recorded before this returns.
func AcquirePersistentShardLease(storagePath, shardID, owner string) (*PersistentShardLease, error) {
	storagePath, shardID, owner, err := normalizePersistentShardLeaseInput(storagePath, shardID, owner)
	if err != nil {
		return nil, err
	}
	if !persistentShardLeaseLockSupported() {
		return nil, ErrPersistentShardLeaseUnsupported
	}
	directory := persistentShardLeaseDirectory(storagePath)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return nil, fmt.Errorf("create persistent shard lease directory: %w", err)
	}
	lockPath, statePath := persistentShardLeasePaths(storagePath, shardID)
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open persistent shard lease lock: %w", err)
	}
	if err := lockPersistentShardLease(lockFile); err != nil {
		_ = lockFile.Close()
		if persistentShardLeaseLockHeld(err) {
			return nil, fmt.Errorf("%w: %s", ErrPersistentShardLeaseHeld, shardID)
		}
		return nil, fmt.Errorf("lock persistent shard lease: %w", err)
	}
	state, err := readPersistentShardLeaseState(statePath, shardID)
	if err != nil {
		_ = unlockPersistentShardLease(lockFile)
		_ = lockFile.Close()
		return nil, err
	}
	if state.Token == ^uint64(0) {
		_ = unlockPersistentShardLease(lockFile)
		_ = lockFile.Close()
		return nil, fmt.Errorf("%w: fencing token exhausted", ErrPersistentShardLeaseInvalid)
	}
	now := time.Now().UTC()
	state = persistentShardLeaseState{
		Version:    persistentShardLeaseStateVersion,
		ShardID:    shardID,
		Owner:      owner,
		Token:      state.Token + 1,
		AcquiredAt: now,
		RenewedAt:  now,
	}
	if err := writePersistentShardLeaseState(statePath, state); err != nil {
		_ = unlockPersistentShardLease(lockFile)
		_ = lockFile.Close()
		return nil, err
	}
	return &PersistentShardLease{
		lockFile:    lockFile,
		storagePath: storagePath,
		shardID:     shardID,
		owner:       owner,
		token:       state.Token,
		statePath:   statePath,
	}, nil
}

// Token returns the immutable fencing token assigned at acquisition.
func (lease *PersistentShardLease) Token() uint64 {
	if lease == nil {
		return 0
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return lease.token
}

// Owner returns the owner identity supplied at acquisition.
func (lease *PersistentShardLease) Owner() string {
	if lease == nil {
		return ""
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return lease.owner
}

// Validate confirms that this lease is still active and has not been fenced
// by a later owner. Call it immediately before each durable mutation.
func (lease *PersistentShardLease) Validate() error {
	if lease == nil {
		return ErrPersistentShardLeaseInvalid
	}
	lease.mu.Lock()
	if lease.released {
		lease.mu.Unlock()
		return ErrPersistentShardLeaseReleased
	}
	storagePath, shardID, owner, token := lease.storagePath, lease.shardID, lease.owner, lease.token
	lease.mu.Unlock()
	return validatePersistentShardLeaseToken(storagePath, shardID, owner, token)
}

// Renew refreshes the durable timestamp without changing the fencing token.
// It does not extend a wall-clock expiration because ownership is held until
// Release or process exit; callers can use Renew as a liveness audit.
func (lease *PersistentShardLease) Renew() error {
	if lease == nil {
		return ErrPersistentShardLeaseInvalid
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.released {
		return ErrPersistentShardLeaseReleased
	}
	state, err := readPersistentShardLeaseState(lease.statePath, lease.shardID)
	if err != nil {
		return err
	}
	if state.Token != lease.token || state.Owner != lease.owner {
		return ErrPersistentShardLeaseFenced
	}
	state.RenewedAt = time.Now().UTC()
	return writePersistentShardLeaseState(lease.statePath, state)
}

// Release relinquishes the lease. It is idempotent and leaves the last token
// in the state record so the next owner always receives a larger token.
func (lease *PersistentShardLease) Release() error {
	if lease == nil {
		return nil
	}
	lease.mu.Lock()
	if lease.released {
		lease.mu.Unlock()
		return nil
	}
	lease.released = true
	lockFile := lease.lockFile
	lease.lockFile = nil
	lease.mu.Unlock()
	if lockFile == nil {
		return nil
	}
	unlockErr := unlockPersistentShardLease(lockFile)
	closeErr := lockFile.Close()
	return errors.Join(unlockErr, closeErr)
}

// ValidatePersistentShardLeaseToken checks a token without requiring the
// caller to retain the lease object. It is intended for write-path fencing
// checks in components that only persist the shard path and token.
func ValidatePersistentShardLeaseToken(storagePath, shardID string, token uint64) error {
	storagePath, shardID, _, err := normalizePersistentShardLeaseInput(storagePath, shardID, "lease-token-check")
	if err != nil {
		return err
	}
	if token == 0 {
		return ErrPersistentShardLeaseFenced
	}
	return validatePersistentShardLeaseToken(storagePath, shardID, "", token)
}

// InspectPersistentShardLease reads the durable owner record and reports
// whether its lock is currently held. It is observational and never acquires
// the lease.
func InspectPersistentShardLease(storagePath, shardID string) (PersistentShardLeaseInfo, error) {
	storagePath, shardID, _, err := normalizePersistentShardLeaseInput(storagePath, shardID, "lease-inspection")
	if err != nil {
		return PersistentShardLeaseInfo{}, err
	}
	if !persistentShardLeaseLockSupported() {
		return PersistentShardLeaseInfo{}, ErrPersistentShardLeaseUnsupported
	}
	lockPath, statePath := persistentShardLeasePaths(storagePath, shardID)
	state, err := readPersistentShardLeaseState(statePath, shardID)
	if err != nil {
		return PersistentShardLeaseInfo{}, err
	}
	info := PersistentShardLeaseInfo{
		ShardID:    shardID,
		Owner:      state.Owner,
		Token:      state.Token,
		AcquiredAt: state.AcquiredAt,
		RenewedAt:  state.RenewedAt,
	}
	lockFile, err := os.OpenFile(lockPath, os.O_RDWR, 0)
	if errors.Is(err, os.ErrNotExist) {
		return info, nil
	}
	if err != nil {
		return PersistentShardLeaseInfo{}, fmt.Errorf("open persistent shard lease lock: %w", err)
	}
	defer lockFile.Close()
	active := true
	if err := lockPersistentShardLease(lockFile); err == nil {
		active = false
		_ = unlockPersistentShardLease(lockFile)
	} else if !persistentShardLeaseLockHeld(err) {
		return PersistentShardLeaseInfo{}, fmt.Errorf("inspect persistent shard lease lock: %w", err)
	}
	info.Active = active
	return info, nil
}

func validatePersistentShardLeaseToken(storagePath, shardID, owner string, token uint64) error {
	lockPath, statePath := persistentShardLeasePaths(storagePath, shardID)
	lockFile, err := os.OpenFile(lockPath, os.O_RDWR, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrPersistentShardLeaseNotHeld
		}
		return fmt.Errorf("open persistent shard lease lock: %w", err)
	}
	lockErr := lockPersistentShardLease(lockFile)
	if lockErr == nil {
		_ = unlockPersistentShardLease(lockFile)
		_ = lockFile.Close()
		return ErrPersistentShardLeaseNotHeld
	}
	if !persistentShardLeaseLockHeld(lockErr) {
		_ = lockFile.Close()
		return fmt.Errorf("validate persistent shard lease lock: %w", lockErr)
	}
	_ = lockFile.Close()
	state, err := readPersistentShardLeaseState(statePath, shardID)
	if err != nil {
		return err
	}
	if state.Token != token || (owner != "" && state.Owner != owner) {
		return ErrPersistentShardLeaseFenced
	}
	return nil
}

func normalizePersistentShardLeaseInput(storagePath, shardID, owner string) (string, string, string, error) {
	storagePath = strings.TrimSpace(storagePath)
	shardID = strings.TrimSpace(shardID)
	owner = strings.TrimSpace(owner)
	if storagePath == "" || shardID == "" || owner == "" || len(shardID) > persistentShardLeaseMaxShardID || len(owner) > persistentShardLeaseMaxOwner || !utf8.ValidString(shardID) || !utf8.ValidString(owner) || strings.IndexByte(shardID, 0) >= 0 || strings.IndexByte(owner, 0) >= 0 {
		return "", "", "", ErrPersistentShardLeaseInvalid
	}
	absolutePath, err := filepath.Abs(storagePath)
	if err != nil {
		return "", "", "", fmt.Errorf("%w: normalize storage path: %v", ErrPersistentShardLeaseInvalid, err)
	}
	return filepath.Clean(absolutePath), shardID, owner, nil
}

func persistentShardLeaseDirectory(storagePath string) string {
	return storagePath + ".leases"
}

func persistentShardLeasePaths(storagePath, shardID string) (string, string) {
	digest := sha256.Sum256([]byte(shardID))
	base := filepath.Join(persistentShardLeaseDirectory(storagePath), hex.EncodeToString(digest[:]))
	return base + ".lock", base + ".state"
}

func readPersistentShardLeaseState(path, shardID string) (persistentShardLeaseState, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return persistentShardLeaseState{}, nil
	}
	if err != nil {
		return persistentShardLeaseState{}, fmt.Errorf("read persistent shard lease state: %w", err)
	}
	if len(data) > persistentShardLeaseMaxStateSize {
		return persistentShardLeaseState{}, fmt.Errorf("%w: state record is too large", ErrPersistentShardLeaseInvalid)
	}
	var state persistentShardLeaseState
	if err := json.Unmarshal(data, &state); err != nil {
		return persistentShardLeaseState{}, fmt.Errorf("%w: decode state record: %v", ErrPersistentShardLeaseInvalid, err)
	}
	if state.Version != persistentShardLeaseStateVersion || state.ShardID != shardID || state.Token == 0 || state.Owner == "" || len(state.ShardID) > persistentShardLeaseMaxShardID || len(state.Owner) > persistentShardLeaseMaxOwner || !utf8.ValidString(state.ShardID) || !utf8.ValidString(state.Owner) || strings.IndexByte(state.ShardID, 0) >= 0 || strings.IndexByte(state.Owner, 0) >= 0 || state.AcquiredAt.IsZero() || state.RenewedAt.IsZero() {
		return persistentShardLeaseState{}, fmt.Errorf("%w: state record metadata is invalid", ErrPersistentShardLeaseInvalid)
	}
	return state, nil
}

func writePersistentShardLeaseState(path string, state persistentShardLeaseState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode persistent shard lease state: %w", err)
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".hatrie-lease-state-")
	if err != nil {
		return fmt.Errorf("create persistent shard lease state: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set persistent shard lease state permissions: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write persistent shard lease state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync persistent shard lease state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close persistent shard lease state: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish persistent shard lease state: %w", err)
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open persistent shard lease directory: %w", err)
	}
	syncErr := directoryFile.Sync()
	closeErr := directoryFile.Close()
	return errors.Join(syncErr, closeErr)
}
