package hatCache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const sqlJSONIndexRebuildCheckpointVersion = 1

var (
	ErrNilSQLJSONIndexRebuildCheckpointStore = errors.New("hatriecache: SQL JSON index rebuild checkpoint store is nil")
	ErrInvalidSQLJSONIndexRebuildCheckpoint  = errors.New("hatriecache: SQL JSON index rebuild checkpoint is invalid")
)

// SQLJSONIndexRebuildCheckpoint identifies one index rebuild that must remain
// durable until its atomic rebuild unit completes.
type SQLJSONIndexRebuildCheckpoint struct {
	Key   string `json:"key"`
	Field string `json:"field"`
}

// SQLJSONIndexRebuildCheckpointStore persists pending rebuild identifiers.
// Implementations must make Save durable before returning and should not call
// back into the HatTrie while a scheduler operation is in progress.
type SQLJSONIndexRebuildCheckpointStore interface {
	Load(context.Context) ([]SQLJSONIndexRebuildCheckpoint, error)
	Save(context.Context, []SQLJSONIndexRebuildCheckpoint) error
}

type sqlJSONIndexRebuildCheckpointFile struct {
	Version     int                             `json:"version"`
	Checkpoints []SQLJSONIndexRebuildCheckpoint `json:"checkpoints,omitempty"`
}

// FileSQLJSONIndexRebuildCheckpointStore atomically stores rebuild
// checkpoints in one private JSON file. It is intended for local process
// recovery; a database-backed implementation can satisfy the same interface.
type FileSQLJSONIndexRebuildCheckpointStore struct {
	mu   sync.Mutex
	path string
}

// NewFileSQLJSONIndexRebuildCheckpointStore creates a file-backed checkpoint
// store. The file and its parent directories are created on the first Save.
func NewFileSQLJSONIndexRebuildCheckpointStore(path string) (*FileSQLJSONIndexRebuildCheckpointStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("%w: checkpoint path is empty", ErrInvalidSQLJSONIndexRebuildCheckpoint)
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("checkpoint path: %w", err)
	}
	return &FileSQLJSONIndexRebuildCheckpointStore{path: absolutePath}, nil
}

// Load reads the durable pending rebuild list. A missing file is an empty
// checkpoint, which makes first startup equivalent to the disabled mode.
func (store *FileSQLJSONIndexRebuildCheckpointStore) Load(ctx context.Context) ([]SQLJSONIndexRebuildCheckpoint, error) {
	if store == nil {
		return nil, ErrNilSQLJSONIndexRebuildCheckpointStore
	}
	ctx = normalizeSQLJSONIndexRebuildCheckpointContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	info, err := os.Lstat(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load SQL JSON index rebuild checkpoints: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: checkpoint path is not a regular file", ErrInvalidSQLJSONIndexRebuildCheckpoint)
	}
	data, err := os.ReadFile(store.path)
	if err != nil {
		return nil, fmt.Errorf("load SQL JSON index rebuild checkpoints: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var file sqlJSONIndexRebuildCheckpointFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("%w: decode checkpoint file: %v", ErrInvalidSQLJSONIndexRebuildCheckpoint, err)
	}
	if file.Version != sqlJSONIndexRebuildCheckpointVersion {
		return nil, fmt.Errorf("%w: unsupported checkpoint version %d", ErrInvalidSQLJSONIndexRebuildCheckpoint, file.Version)
	}
	return normalizeSQLJSONIndexRebuildCheckpoints(file.Checkpoints)
}

// Save validates and atomically replaces the durable pending rebuild list.
func (store *FileSQLJSONIndexRebuildCheckpointStore) Save(ctx context.Context, checkpoints []SQLJSONIndexRebuildCheckpoint) error {
	if store == nil {
		return ErrNilSQLJSONIndexRebuildCheckpointStore
	}
	ctx = normalizeSQLJSONIndexRebuildCheckpointContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	checkpoints, err := normalizeSQLJSONIndexRebuildCheckpoints(checkpoints)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := writeJSONFileAtomic(store.path, sqlJSONIndexRebuildCheckpointFile{
		Version: sqlJSONIndexRebuildCheckpointVersion, Checkpoints: checkpoints,
	}); err != nil {
		return fmt.Errorf("save SQL JSON index rebuild checkpoints: %w", err)
	}
	return nil
}

func normalizeSQLJSONIndexRebuildCheckpointContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func normalizeSQLJSONIndexRebuildCheckpoints(checkpoints []SQLJSONIndexRebuildCheckpoint) ([]SQLJSONIndexRebuildCheckpoint, error) {
	if len(checkpoints) == 0 {
		return nil, nil
	}
	copyOfCheckpoints := append([]SQLJSONIndexRebuildCheckpoint(nil), checkpoints...)
	sort.Slice(copyOfCheckpoints, func(i, j int) bool {
		if copyOfCheckpoints[i].Key == copyOfCheckpoints[j].Key {
			return copyOfCheckpoints[i].Field < copyOfCheckpoints[j].Field
		}
		return copyOfCheckpoints[i].Key < copyOfCheckpoints[j].Key
	})
	for index, checkpoint := range copyOfCheckpoints {
		if checkpoint.Key == "" || checkpoint.Field == "" {
			return nil, fmt.Errorf("%w: key and field are required", ErrInvalidSQLJSONIndexRebuildCheckpoint)
		}
		if index > 0 && copyOfCheckpoints[index-1] == checkpoint {
			return nil, fmt.Errorf("%w: duplicate key %q and field %q", ErrInvalidSQLJSONIndexRebuildCheckpoint, checkpoint.Key, checkpoint.Field)
		}
	}
	return copyOfCheckpoints, nil
}

// SetSQLJSONIndexRebuildCheckpointStore attaches an optional durable store
// and immediately recovers checkpoints for indexes already configured on ht.
// Attach the store before scheduling rebuilds. A nil store disables future
// persistence while retaining the in-memory queue.
func (ht *HatTrie) SetSQLJSONIndexRebuildCheckpointStore(ctx context.Context, store SQLJSONIndexRebuildCheckpointStore) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	ctx = normalizeSQLJSONIndexRebuildCheckpointContext(ctx)
	ht.sqlJSONIndexRebuildCheckpointMu.Lock()
	defer ht.sqlJSONIndexRebuildCheckpointMu.Unlock()
	if store == nil {
		ht.sqlIndexMu.Lock()
		ht.sqlJSONIndexRebuildCheckpointStore = nil
		ht.sqlIndexMu.Unlock()
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		return err
	}
	ht.sqlIndexMu.Lock()
	current := ht.sqlJSONIndexRebuildCheckpointSnapshotLocked()
	ht.sqlIndexMu.Unlock()
	merged, err := mergeSQLJSONIndexRebuildCheckpoints(current, loaded)
	if err != nil {
		return err
	}
	if err := store.Save(ctx, merged); err != nil {
		return err
	}
	ht.sqlIndexMu.Lock()
	ht.sqlJSONIndexRebuildCheckpointStore = store
	ht.sqlJSONIndexRebuildDurable = sqlJSONIndexRebuildCheckpointMap(merged)
	ht.resumeSQLJSONIndexRebuildsLocked(merged)
	ht.sqlIndexMu.Unlock()
	return nil
}

// ResumeSQLJSONIndexRebuilds queues durable checkpoints whose indexes have
// already been configured. It is useful when checkpoint attachment happens
// before index definitions are restored during process startup.
func (ht *HatTrie) ResumeSQLJSONIndexRebuilds() int {
	if ht == nil {
		return 0
	}
	ht.sqlJSONIndexRebuildCheckpointMu.Lock()
	defer ht.sqlJSONIndexRebuildCheckpointMu.Unlock()
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	return ht.resumeSQLJSONIndexRebuildsLocked(nil)
}

func (ht *HatTrie) resumeSQLJSONIndexRebuildsLocked(checkpoints []SQLJSONIndexRebuildCheckpoint) int {
	if checkpoints == nil {
		checkpoints = sqlJSONIndexRebuildCheckpointSnapshotFromMap(ht.sqlJSONIndexRebuildDurable)
	}
	queued := 0
	for _, checkpoint := range checkpoints {
		if !ht.sqlJSONIndexConfiguredLocked(checkpoint.Key, checkpoint.Field) {
			continue
		}
		if ht.sqlJSONIndexRebuildInFlight[checkpoint.Key][checkpoint.Field] > 0 {
			continue
		}
		if ht.enqueueSQLJSONIndexRebuildLocked(sqlJSONIndexRebuildRequest{key: checkpoint.Key, field: checkpoint.Field}, false) {
			queued++
		}
	}
	return queued
}

func (ht *HatTrie) scheduleSQLJSONIndexRebuild(ctx context.Context, key, field string) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if key == "" || field == "" {
		return fmt.Errorf("SQL JSON index rebuild requires a cache key and field")
	}
	ctx = normalizeSQLJSONIndexRebuildCheckpointContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	ht.sqlJSONIndexRebuildCheckpointMu.Lock()
	defer ht.sqlJSONIndexRebuildCheckpointMu.Unlock()
	ht.sqlIndexMu.Lock()
	if !ht.sqlJSONIndexConfiguredLocked(key, field) {
		ht.sqlIndexMu.Unlock()
		return fmt.Errorf("SQL JSON index %q on %q is not configured", field, key)
	}
	if ht.sqlJSONIndexRebuildPending[key][field] {
		ht.sqlIndexMu.Unlock()
		return nil
	}
	request := sqlJSONIndexRebuildRequest{key: key, field: field}
	ht.enqueueSQLJSONIndexRebuildLocked(request, true)
	store := ht.sqlJSONIndexRebuildCheckpointStore
	if store == nil {
		ht.sqlIndexMu.Unlock()
		return nil
	}
	if ht.sqlJSONIndexRebuildDurable == nil {
		ht.sqlJSONIndexRebuildDurable = make(map[string]map[string]bool)
	}
	if ht.sqlJSONIndexRebuildDurable[key] == nil {
		ht.sqlJSONIndexRebuildDurable[key] = make(map[string]bool)
	}
	wasDurable := ht.sqlJSONIndexRebuildDurable[key][field]
	ht.sqlJSONIndexRebuildDurable[key][field] = true
	checkpoints := ht.sqlJSONIndexRebuildCheckpointSnapshotLocked()
	ht.sqlIndexMu.Unlock()
	if err := store.Save(ctx, checkpoints); err != nil {
		ht.sqlIndexMu.Lock()
		ht.removeSQLJSONIndexRebuildRequestLocked(request)
		if !wasDurable {
			delete(ht.sqlJSONIndexRebuildDurable[key], field)
			if len(ht.sqlJSONIndexRebuildDurable[key]) == 0 {
				delete(ht.sqlJSONIndexRebuildDurable, key)
			}
		}
		ht.sqlIndexMu.Unlock()
		return err
	}
	return nil
}

func (ht *HatTrie) completeSQLJSONIndexRebuild(ctx context.Context, request sqlJSONIndexRebuildRequest) error {
	ht.sqlJSONIndexRebuildCheckpointMu.Lock()
	defer ht.sqlJSONIndexRebuildCheckpointMu.Unlock()
	ht.sqlIndexMu.Lock()
	delete(ht.sqlJSONIndexRebuildDurable[request.key], request.field)
	if len(ht.sqlJSONIndexRebuildDurable[request.key]) == 0 {
		delete(ht.sqlJSONIndexRebuildDurable, request.key)
	}
	if ht.sqlJSONIndexRebuildInFlight[request.key][request.field] > 1 {
		ht.sqlJSONIndexRebuildInFlight[request.key][request.field]--
	} else {
		delete(ht.sqlJSONIndexRebuildInFlight[request.key], request.field)
	}
	if len(ht.sqlJSONIndexRebuildInFlight[request.key]) == 0 {
		delete(ht.sqlJSONIndexRebuildInFlight, request.key)
	}
	store := ht.sqlJSONIndexRebuildCheckpointStore
	if store == nil {
		ht.sqlIndexMu.Unlock()
		return nil
	}
	checkpoints := ht.sqlJSONIndexRebuildCheckpointSnapshotLocked()
	ht.sqlIndexMu.Unlock()
	if err := store.Save(normalizeSQLJSONIndexRebuildCheckpointContext(ctx), checkpoints); err != nil {
		ht.sqlIndexMu.Lock()
		if ht.sqlJSONIndexRebuildDurable == nil {
			ht.sqlJSONIndexRebuildDurable = make(map[string]map[string]bool)
		}
		if ht.sqlJSONIndexRebuildDurable[request.key] == nil {
			ht.sqlJSONIndexRebuildDurable[request.key] = make(map[string]bool)
		}
		ht.sqlJSONIndexRebuildDurable[request.key][request.field] = true
		ht.enqueueSQLJSONIndexRebuildLocked(request, false)
		ht.sqlIndexMu.Unlock()
		return err
	}
	return nil
}

func (ht *HatTrie) enqueueSQLJSONIndexRebuildLocked(request sqlJSONIndexRebuildRequest, countScheduled bool) bool {
	if ht.sqlJSONIndexRebuildPending == nil {
		ht.sqlJSONIndexRebuildPending = make(map[string]map[string]bool)
	}
	if ht.sqlJSONIndexRebuildPending[request.key] == nil {
		ht.sqlJSONIndexRebuildPending[request.key] = make(map[string]bool)
	}
	if ht.sqlJSONIndexRebuildPending[request.key][request.field] {
		return false
	}
	ht.sqlJSONIndexRebuildPending[request.key][request.field] = true
	ht.sqlJSONIndexRebuildQueue = append(ht.sqlJSONIndexRebuildQueue, request)
	if countScheduled {
		ht.sqlJSONIndexMaintenanceLocked(request.key, request.field).scheduled++
	}
	return true
}

func (ht *HatTrie) removeSQLJSONIndexRebuildRequestLocked(request sqlJSONIndexRebuildRequest) {
	for index := len(ht.sqlJSONIndexRebuildQueue) - 1; index >= 0; index-- {
		if ht.sqlJSONIndexRebuildQueue[index] != request {
			continue
		}
		copy(ht.sqlJSONIndexRebuildQueue[index:], ht.sqlJSONIndexRebuildQueue[index+1:])
		ht.sqlJSONIndexRebuildQueue[len(ht.sqlJSONIndexRebuildQueue)-1] = sqlJSONIndexRebuildRequest{}
		ht.sqlJSONIndexRebuildQueue = ht.sqlJSONIndexRebuildQueue[:len(ht.sqlJSONIndexRebuildQueue)-1]
		break
	}
	delete(ht.sqlJSONIndexRebuildPending[request.key], request.field)
	if len(ht.sqlJSONIndexRebuildPending[request.key]) == 0 {
		delete(ht.sqlJSONIndexRebuildPending, request.key)
	}
	maintenance := ht.sqlJSONIndexMaintenanceLocked(request.key, request.field)
	if maintenance.scheduled > 0 {
		maintenance.scheduled--
	}
}

func (ht *HatTrie) sqlJSONIndexRebuildCheckpointSnapshotLocked() []SQLJSONIndexRebuildCheckpoint {
	checkpoints := sqlJSONIndexRebuildCheckpointSnapshotFromMap(ht.sqlJSONIndexRebuildDurable)
	seen := sqlJSONIndexRebuildCheckpointMap(checkpoints)
	for _, request := range ht.sqlJSONIndexRebuildQueue {
		if seen[request.key] == nil {
			seen[request.key] = make(map[string]bool)
		}
		seen[request.key][request.field] = true
	}
	for key, fields := range ht.sqlJSONIndexRebuildInFlight {
		if seen[key] == nil {
			seen[key] = make(map[string]bool)
		}
		for field, count := range fields {
			if count > 0 {
				seen[key][field] = true
			}
		}
	}
	return sqlJSONIndexRebuildCheckpointSnapshotFromMap(seen)
}

func sqlJSONIndexRebuildCheckpointMap(checkpoints []SQLJSONIndexRebuildCheckpoint) map[string]map[string]bool {
	result := make(map[string]map[string]bool)
	for _, checkpoint := range checkpoints {
		if result[checkpoint.Key] == nil {
			result[checkpoint.Key] = make(map[string]bool)
		}
		result[checkpoint.Key][checkpoint.Field] = true
	}
	return result
}

func sqlJSONIndexRebuildCheckpointSnapshotFromMap(checkpoints map[string]map[string]bool) []SQLJSONIndexRebuildCheckpoint {
	result := make([]SQLJSONIndexRebuildCheckpoint, 0)
	for key, fields := range checkpoints {
		for field := range fields {
			result = append(result, SQLJSONIndexRebuildCheckpoint{Key: key, Field: field})
		}
	}
	result, _ = normalizeSQLJSONIndexRebuildCheckpoints(result)
	return result
}

func mergeSQLJSONIndexRebuildCheckpoints(groups ...[]SQLJSONIndexRebuildCheckpoint) ([]SQLJSONIndexRebuildCheckpoint, error) {
	merged := make([]SQLJSONIndexRebuildCheckpoint, 0)
	seen := make(map[string]map[string]bool)
	for _, group := range groups {
		for _, checkpoint := range group {
			if checkpoint.Key == "" || checkpoint.Field == "" {
				return nil, fmt.Errorf("%w: key and field are required", ErrInvalidSQLJSONIndexRebuildCheckpoint)
			}
			if seen[checkpoint.Key] == nil {
				seen[checkpoint.Key] = make(map[string]bool)
			}
			if seen[checkpoint.Key][checkpoint.Field] {
				continue
			}
			seen[checkpoint.Key][checkpoint.Field] = true
			merged = append(merged, checkpoint)
		}
	}
	return normalizeSQLJSONIndexRebuildCheckpoints(merged)
}
