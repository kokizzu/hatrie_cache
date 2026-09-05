package hatCache

import (
	"errors"
	"fmt"
)

var (
	// ErrNilAtomicCallback reports that RunAtomic received no callback.
	ErrNilAtomicCallback = errors.New("hatriecache: atomic callback is nil")
	// ErrNilAtomicCommandBatch reports a method call on a nil command batch.
	ErrNilAtomicCommandBatch = errors.New("hatriecache: atomic command batch is nil")
)

// AtomicCommandBatch collects public commands for one atomic commit.
// Requests are copied when added, so the caller can reuse its input buffers
// before RunAtomic commits the batch. Use BeginSQLTransaction when the
// transaction needs SQL queries or snapshot-isolated read-your-writes
// visibility.
type AtomicCommandBatch struct {
	requests []CacheCommandRequest
}

// Add stages one public command. The command is not applied until RunAtomic
// returns from its callback and commits the complete batch.
func (batch *AtomicCommandBatch) Add(request CacheCommandRequest) error {
	if batch == nil {
		return ErrNilAtomicCommandBatch
	}
	if len(batch.requests) >= maxPublicCommandBatchSize {
		return fmt.Errorf("atomic command batch size must be <= %d", maxPublicCommandBatchSize)
	}
	if request.Atomic {
		return errors.New("atomic command batch entries cannot set atomic")
	}
	if err := validatePublicCommandBatchPayload(request, len(batch.requests)); err != nil {
		return err
	}
	batch.requests = append(batch.requests, cloneAsyncCommandRequest(request))
	return nil
}

// Len returns the number of staged commands.
func (batch *AtomicCommandBatch) Len() int {
	if batch == nil {
		return 0
	}
	return len(batch.requests)
}

// RunAtomic calls build to collect public commands and commits them as one
// atomic BATCH. A callback error leaves the cache unchanged. A command-level
// rejection is returned both as the response and as a Go error. Mutations are
// rolled back together; reads are returned in the batch response.
func (ht *HatTrie) RunAtomic(build func(*AtomicCommandBatch) error) (CacheCommandResponse, error) {
	if ht == nil {
		return CacheCommandResponse{}, ErrNilHatTrie
	}
	if build == nil {
		return CacheCommandResponse{}, ErrNilAtomicCallback
	}
	batch := &AtomicCommandBatch{requests: make([]CacheCommandRequest, 0, 4)}
	if err := build(batch); err != nil {
		return CacheCommandResponse{}, err
	}
	if len(batch.requests) == 0 {
		return CacheCommandResponse{OK: true, Message: "atomic batch empty"}, nil
	}
	response := ht.ExecuteCommand(CacheCommandRequest{
		Command: "BATCH",
		Atomic:  true,
		Batch:   batch.requests,
	})
	if !response.OK {
		return response, fmt.Errorf("atomic command batch failed: %s", response.Message)
	}
	return response, nil
}
