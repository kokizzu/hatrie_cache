package hatSql

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrSQLProjectionAdvisorStoreNil indicates that persistence was requested
	// without a store.
	ErrSQLProjectionAdvisorStoreNil = errors.New("hatriecache: SQL projection advisor store is nil")
	// ErrSQLProjectionAdvisorStorePath indicates an unusable snapshot path.
	ErrSQLProjectionAdvisorStorePath = errors.New("hatriecache: SQL projection advisor store path is invalid")
	// ErrSQLProjectionAdvisorSnapshotInvalid indicates a validly framed but
	// semantically invalid snapshot.
	ErrSQLProjectionAdvisorSnapshotInvalid = errors.New("hatriecache: SQL projection advisor snapshot is invalid")
	// ErrSQLProjectionAdvisorSnapshotCorrupt indicates a truncated, malformed,
	// or checksum-failing snapshot.
	ErrSQLProjectionAdvisorSnapshotCorrupt = errors.New("hatriecache: SQL projection advisor snapshot is corrupt")
	// ErrSQLProjectionAdvisorSnapshotTooLarge indicates that a bounded snapshot
	// exceeded the configured on-disk limits.
	ErrSQLProjectionAdvisorSnapshotTooLarge = errors.New("hatriecache: SQL projection advisor snapshot is too large")
)

// SQLProjectionAdvisorStore persists bounded projection-advisor history.
// Implementations should make Save atomic with respect to a concurrent Load.
type SQLProjectionAdvisorStore interface {
	LoadSQLProjectionAdvisor(context.Context) ([]SQLProjectionRecommendation, error)
	SaveSQLProjectionAdvisor(context.Context, []SQLProjectionRecommendation) error
}

// Persist writes the advisor's current recommendations to store. Persistence
// is explicit so normal query execution never performs I/O.
func (advisor *SQLProjectionAdvisor) Persist(ctx context.Context, store SQLProjectionAdvisorStore) error {
	if advisor == nil {
		return fmt.Errorf("%w: advisor is nil", ErrSQLProjectionAdvisorSnapshotInvalid)
	}
	if store == nil {
		return ErrSQLProjectionAdvisorStoreNil
	}
	if err := sqlProjectionAdvisorContextErr(ctx); err != nil {
		return err
	}
	return store.SaveSQLProjectionAdvisor(ctx, advisor.Recommendations())
}

// RestoreFrom loads recommendations from store and replaces the advisor's
// current state only after the complete snapshot has been validated.
func (advisor *SQLProjectionAdvisor) RestoreFrom(ctx context.Context, store SQLProjectionAdvisorStore) error {
	if advisor == nil {
		return fmt.Errorf("%w: advisor is nil", ErrSQLProjectionAdvisorSnapshotInvalid)
	}
	if store == nil {
		return ErrSQLProjectionAdvisorStoreNil
	}
	if err := sqlProjectionAdvisorContextErr(ctx); err != nil {
		return err
	}
	recommendations, err := store.LoadSQLProjectionAdvisor(ctx)
	if err != nil {
		return err
	}
	return advisor.Restore(recommendations)
}

// Restore validates and replaces advisor state from recommendations. The
// caller-owned slices are copied, and a failed restore leaves the old state
// untouched.
func (advisor *SQLProjectionAdvisor) Restore(recommendations []SQLProjectionRecommendation) error {
	if advisor == nil {
		return fmt.Errorf("%w: advisor is nil", ErrSQLProjectionAdvisorSnapshotInvalid)
	}
	if advisor.capacity <= 0 && len(recommendations) > 0 {
		return fmt.Errorf("%w: advisor capacity is nonpositive", ErrSQLProjectionAdvisorSnapshotInvalid)
	}
	if len(recommendations) > sqlProjectionAdvisorMaxRecommendations {
		return ErrSQLProjectionAdvisorSnapshotTooLarge
	}
	if advisor.capacity > 0 && len(recommendations) > advisor.capacity {
		return fmt.Errorf("%w: recommendation count exceeds advisor capacity", ErrSQLProjectionAdvisorSnapshotInvalid)
	}

	restored := make(map[sqlProjectionAdvisorKey]sqlProjectionAdvisorStats, len(recommendations))
	for index, recommendation := range recommendations {
		normalized, key, stats, err := sqlProjectionAdvisorNormalizeRecommendation(recommendation)
		if err != nil {
			return fmt.Errorf("%w: recommendation %d: %v", ErrSQLProjectionAdvisorSnapshotInvalid, index, err)
		}
		if _, exists := restored[key]; exists {
			return fmt.Errorf("%w: duplicate recommendation %q", ErrSQLProjectionAdvisorSnapshotInvalid, normalized.QueryID)
		}
		restored[key] = stats
	}

	advisor.mu.Lock()
	advisor.counts = restored
	advisor.mu.Unlock()
	return nil
}

const (
	sqlProjectionAdvisorSnapshotVersion       uint16 = 1
	sqlProjectionAdvisorSnapshotHeaderSize           = 8
	sqlProjectionAdvisorSnapshotChecksumSize         = 4
	sqlProjectionAdvisorSnapshotFrameOverhead        = sqlProjectionAdvisorSnapshotHeaderSize + sqlProjectionAdvisorSnapshotChecksumSize
	sqlProjectionAdvisorMaxPayload                   = 8 << 20
	sqlProjectionAdvisorMaxRecommendations           = 65536
)

var sqlProjectionAdvisorSnapshotMagic = [4]byte{'S', 'P', 'A', '1'}

type sqlProjectionAdvisorSnapshot struct {
	Version         uint16                        `json:"version"`
	Recommendations []SQLProjectionRecommendation `json:"recommendations"`
}

func sqlProjectionAdvisorNormalizeRecommendation(recommendation SQLProjectionRecommendation) (SQLProjectionRecommendation, sqlProjectionAdvisorKey, sqlProjectionAdvisorStats, error) {
	if recommendation.QueryID == "" || strings.TrimSpace(recommendation.QueryID) != recommendation.QueryID {
		return SQLProjectionRecommendation{}, sqlProjectionAdvisorKey{}, sqlProjectionAdvisorStats{}, errors.New("query ID is empty or not trimmed")
	}
	if recommendation.SlowQueries == 0 {
		return SQLProjectionRecommendation{}, sqlProjectionAdvisorKey{}, sqlProjectionAdvisorStats{}, errors.New("slow query count is zero")
	}
	if recommendation.TotalElapsed < 0 {
		return SQLProjectionRecommendation{}, sqlProjectionAdvisorKey{}, sqlProjectionAdvisorStats{}, errors.New("total elapsed time is negative")
	}

	var err error
	recommendation.Dependencies, err = sqlProjectionAdvisorNormalizeList(recommendation.Dependencies, true)
	if err != nil {
		return SQLProjectionRecommendation{}, sqlProjectionAdvisorKey{}, sqlProjectionAdvisorStats{}, fmt.Errorf("dependencies: %w", err)
	}
	recommendation.Fields, err = sqlProjectionAdvisorNormalizeList(recommendation.Fields, false)
	if err != nil {
		return SQLProjectionRecommendation{}, sqlProjectionAdvisorKey{}, sqlProjectionAdvisorStats{}, fmt.Errorf("fields: %w", err)
	}
	recommendation.FilterFields, err = sqlProjectionAdvisorNormalizeList(recommendation.FilterFields, false)
	if err != nil {
		return SQLProjectionRecommendation{}, sqlProjectionAdvisorKey{}, sqlProjectionAdvisorStats{}, fmt.Errorf("filter fields: %w", err)
	}
	recommendation.GroupByFields, err = sqlProjectionAdvisorNormalizeList(recommendation.GroupByFields, false)
	if err != nil {
		return SQLProjectionRecommendation{}, sqlProjectionAdvisorKey{}, sqlProjectionAdvisorStats{}, fmt.Errorf("group-by fields: %w", err)
	}
	recommendation.OrderByFields, err = sqlProjectionAdvisorNormalizeList(recommendation.OrderByFields, false)
	if err != nil {
		return SQLProjectionRecommendation{}, sqlProjectionAdvisorKey{}, sqlProjectionAdvisorStats{}, fmt.Errorf("order-by fields: %w", err)
	}

	key := sqlProjectionAdvisorKey{
		queryID:      recommendation.QueryID,
		dependencies: sqlProjectionAdvisorEncodeDependencies(recommendation.Dependencies),
		shape: sqlProjectionAdvisorEncodeShape(sqlProjectionAdvisorShape{
			fields:        recommendation.Fields,
			filterFields:  recommendation.FilterFields,
			groupByFields: recommendation.GroupByFields,
			orderByFields: recommendation.OrderByFields,
		}),
	}
	return recommendation, key, sqlProjectionAdvisorStats{
		slowQueries:       recommendation.SlowQueries,
		totalElapsedNanos: uint64(recommendation.TotalElapsed),
	}, nil
}

func sqlProjectionAdvisorNormalizeList(values []string, requireNonEmpty bool) ([]string, error) {
	if len(values) == 0 {
		if requireNonEmpty {
			return nil, errors.New("list is empty")
		}
		return nil, nil
	}
	normalized := append([]string(nil), values...)
	for _, value := range normalized {
		if value == "" || strings.TrimSpace(value) == "" {
			return nil, errors.New("list contains an empty value")
		}
	}
	sort.Strings(normalized)
	for index := 1; index < len(normalized); index++ {
		if normalized[index] == normalized[index-1] {
			return nil, errors.New("list contains duplicate values")
		}
	}
	return normalized, nil
}

func marshalSQLProjectionAdvisorSnapshot(recommendations []SQLProjectionRecommendation) ([]byte, error) {
	if len(recommendations) > sqlProjectionAdvisorMaxRecommendations {
		return nil, ErrSQLProjectionAdvisorSnapshotTooLarge
	}
	normalized := make([]SQLProjectionRecommendation, len(recommendations))
	seen := make(map[sqlProjectionAdvisorKey]struct{}, len(recommendations))
	for index, recommendation := range recommendations {
		clean, key, _, err := sqlProjectionAdvisorNormalizeRecommendation(recommendation)
		if err != nil {
			return nil, fmt.Errorf("%w: recommendation %d: %v", ErrSQLProjectionAdvisorSnapshotInvalid, index, err)
		}
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("%w: duplicate recommendation %q", ErrSQLProjectionAdvisorSnapshotInvalid, clean.QueryID)
		}
		seen[key] = struct{}{}
		normalized[index] = clean
	}
	payload, err := json.Marshal(sqlProjectionAdvisorSnapshot{
		Version:         sqlProjectionAdvisorSnapshotVersion,
		Recommendations: normalized,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: encode JSON: %v", ErrSQLProjectionAdvisorSnapshotInvalid, err)
	}
	if len(payload) > sqlProjectionAdvisorMaxPayload {
		return nil, ErrSQLProjectionAdvisorSnapshotTooLarge
	}
	frame := make([]byte, sqlProjectionAdvisorSnapshotFrameOverhead+len(payload))
	copy(frame[:len(sqlProjectionAdvisorSnapshotMagic)], sqlProjectionAdvisorSnapshotMagic[:])
	binary.LittleEndian.PutUint32(frame[4:8], uint32(len(payload)))
	copy(frame[sqlProjectionAdvisorSnapshotHeaderSize:], payload)
	binary.LittleEndian.PutUint32(frame[sqlProjectionAdvisorSnapshotHeaderSize+len(payload):], crc32.ChecksumIEEE(payload))
	return frame, nil
}

func unmarshalSQLProjectionAdvisorSnapshot(frame []byte) ([]SQLProjectionRecommendation, error) {
	if len(frame) < sqlProjectionAdvisorSnapshotFrameOverhead {
		return nil, ErrSQLProjectionAdvisorSnapshotCorrupt
	}
	if string(frame[:len(sqlProjectionAdvisorSnapshotMagic)]) != string(sqlProjectionAdvisorSnapshotMagic[:]) {
		return nil, ErrSQLProjectionAdvisorSnapshotCorrupt
	}
	payloadLength := uint64(binary.LittleEndian.Uint32(frame[4:8]))
	if payloadLength > sqlProjectionAdvisorMaxPayload {
		return nil, ErrSQLProjectionAdvisorSnapshotTooLarge
	}
	expectedLength := uint64(sqlProjectionAdvisorSnapshotFrameOverhead) + payloadLength
	if expectedLength != uint64(len(frame)) {
		return nil, ErrSQLProjectionAdvisorSnapshotCorrupt
	}
	payloadEnd := sqlProjectionAdvisorSnapshotHeaderSize + int(payloadLength)
	payload := frame[sqlProjectionAdvisorSnapshotHeaderSize:payloadEnd]
	if binary.LittleEndian.Uint32(frame[payloadEnd:]) != crc32.ChecksumIEEE(payload) {
		return nil, ErrSQLProjectionAdvisorSnapshotCorrupt
	}
	var snapshot sqlProjectionAdvisorSnapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return nil, fmt.Errorf("%w: decode JSON: %v", ErrSQLProjectionAdvisorSnapshotCorrupt, err)
	}
	if snapshot.Version != sqlProjectionAdvisorSnapshotVersion {
		return nil, fmt.Errorf("%w: unsupported snapshot version %d", ErrSQLProjectionAdvisorSnapshotInvalid, snapshot.Version)
	}
	if len(snapshot.Recommendations) > sqlProjectionAdvisorMaxRecommendations {
		return nil, ErrSQLProjectionAdvisorSnapshotTooLarge
	}
	for index, recommendation := range snapshot.Recommendations {
		if _, _, _, err := sqlProjectionAdvisorNormalizeRecommendation(recommendation); err != nil {
			return nil, fmt.Errorf("%w: recommendation %d: %v", ErrSQLProjectionAdvisorSnapshotInvalid, index, err)
		}
	}
	return snapshot.Recommendations, nil
}

// FileSQLProjectionAdvisorStore stores one bounded snapshot using atomic
// replacement. The file is created with owner-only permissions.
type FileSQLProjectionAdvisorStore struct {
	mu   sync.Mutex
	path string
}

// NewFileSQLProjectionAdvisorStore creates a file-backed advisor store. The
// parent directory must already exist; this avoids implicit directory creation
// during recovery or startup.
func NewFileSQLProjectionAdvisorStore(path string) (*FileSQLProjectionAdvisorStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, ErrSQLProjectionAdvisorStorePath
	}
	return &FileSQLProjectionAdvisorStore{path: filepath.Clean(path)}, nil
}

// LoadSQLProjectionAdvisor loads the current snapshot. A missing file means an
// empty advisor, which makes first boot and recovery idempotent.
func (store *FileSQLProjectionAdvisorStore) LoadSQLProjectionAdvisor(ctx context.Context) ([]SQLProjectionRecommendation, error) {
	if store == nil {
		return nil, ErrSQLProjectionAdvisorStoreNil
	}
	if err := sqlProjectionAdvisorContextErr(ctx); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	info, err := os.Lstat(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: snapshot is not a regular file", ErrSQLProjectionAdvisorSnapshotInvalid)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%w: snapshot permissions are too broad", ErrSQLProjectionAdvisorSnapshotInvalid)
	}
	if info.Size() < 0 || info.Size() > int64(sqlProjectionAdvisorSnapshotFrameOverhead+sqlProjectionAdvisorMaxPayload) {
		return nil, ErrSQLProjectionAdvisorSnapshotTooLarge
	}
	data, err := os.ReadFile(store.path)
	if err != nil {
		return nil, err
	}
	if err := sqlProjectionAdvisorContextErr(ctx); err != nil {
		return nil, err
	}
	return unmarshalSQLProjectionAdvisorSnapshot(data)
}

// SaveSQLProjectionAdvisor atomically replaces the snapshot after validating
// and encoding the complete recommendation set.
func (store *FileSQLProjectionAdvisorStore) SaveSQLProjectionAdvisor(ctx context.Context, recommendations []SQLProjectionRecommendation) error {
	if store == nil {
		return ErrSQLProjectionAdvisorStoreNil
	}
	if err := sqlProjectionAdvisorContextErr(ctx); err != nil {
		return err
	}
	frame, err := marshalSQLProjectionAdvisorSnapshot(recommendations)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := sqlProjectionAdvisorContextErr(ctx); err != nil {
		return err
	}
	parent := filepath.Dir(store.path)
	parentInfo, err := os.Stat(parent)
	if err != nil {
		return err
	}
	if !parentInfo.IsDir() {
		return fmt.Errorf("%w: parent is not a directory", ErrSQLProjectionAdvisorStorePath)
	}
	if info, err := os.Lstat(store.path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("%w: snapshot path is not a regular file", ErrSQLProjectionAdvisorSnapshotInvalid)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temporary, err := os.CreateTemp(parent, "."+filepath.Base(store.path)+".tmp-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	renamed := false
	defer func() {
		_ = temporary.Close()
		if !renamed {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return err
	}
	if written, err := temporary.Write(frame); err != nil {
		return err
	} else if written != len(frame) {
		return io.ErrShortWrite
	}
	if err := sqlProjectionAdvisorContextErr(ctx); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return err
	}
	renamed = true
	return sqlProjectionAdvisorSyncDirectory(parent)
}

func sqlProjectionAdvisorSyncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func sqlProjectionAdvisorContextErr(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
