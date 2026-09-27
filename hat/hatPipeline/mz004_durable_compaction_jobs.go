package hatPipeline

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"strings"
	"sync"
)

const (
	// DefaultFrontierCompactionJobLimit bounds the retained durable job catalog
	// when callers do not provide an explicit bound.
	DefaultFrontierCompactionJobLimit  = 1024
	maxFrontierCompactionJobLimit      = 1 << 20
	maxFrontierCompactionJobIDBytes    = 256
	maxFrontierCompactionJobBytes      = 64 << 20
	frontierCompactionJobHeaderBytes   = 5
	frontierCompactionJobChecksumBytes = 4
)

var (
	// ErrFrontierCompactionJobLedgerNil indicates a method call on a nil ledger.
	ErrFrontierCompactionJobLedgerNil = errors.New("hatPipeline: compaction job ledger is nil")
	// ErrFrontierCompactionJobLedgerOptionsInvalid indicates an invalid job
	// bound.
	ErrFrontierCompactionJobLedgerOptionsInvalid = errors.New("hatPipeline: compaction job ledger options are invalid")
	// ErrFrontierCompactionJobFrontierRequired indicates an empty frontier ID.
	ErrFrontierCompactionJobFrontierRequired = errors.New("hatPipeline: compaction job frontier is required")
	// ErrFrontierCompactionJobFrontierInvalid indicates an oversized frontier ID.
	ErrFrontierCompactionJobFrontierInvalid = errors.New("hatPipeline: compaction job frontier is invalid")
	// ErrFrontierCompactionJobLimit indicates that the bounded catalog is full.
	ErrFrontierCompactionJobLimit = errors.New("hatPipeline: compaction job limit reached")
	// ErrFrontierCompactionJobNotFound indicates an unknown job ID.
	ErrFrontierCompactionJobNotFound = errors.New("hatPipeline: compaction job was not found")
	// ErrFrontierCompactionJobTransition indicates an invalid state transition.
	ErrFrontierCompactionJobTransition = errors.New("hatPipeline: compaction job transition is invalid")
	// ErrFrontierCompactionJobSnapshotInvalid indicates malformed or corrupt
	// snapshot data.
	ErrFrontierCompactionJobSnapshotInvalid = errors.New("hatPipeline: compaction job snapshot is invalid")
	// ErrFrontierCompactionJobSnapshotNotEmpty indicates that restore would
	// overwrite an existing catalog.
	ErrFrontierCompactionJobSnapshotNotEmpty = errors.New("hatPipeline: compaction job snapshot requires an empty ledger")
	// ErrFrontierCompactionJobStoreRequired indicates that no durable store was
	// supplied to a persistence operation.
	ErrFrontierCompactionJobStoreRequired = errors.New("hatPipeline: compaction job store is required")
)

var frontierCompactionJobMagic = [4]byte{'H', 'C', 'J', '1'}
var frontierCompactionJobCRCTable = crc32.MakeTable(crc32.Castagnoli)

// FrontierCompactionJobState describes one durable compaction job.
type FrontierCompactionJobState uint8

const (
	FrontierCompactionJobPending FrontierCompactionJobState = iota + 1
	FrontierCompactionJobRunning
	FrontierCompactionJobCompleted
	FrontierCompactionJobFailed
)

func (state FrontierCompactionJobState) String() string {
	switch state {
	case FrontierCompactionJobPending:
		return "pending"
	case FrontierCompactionJobRunning:
		return "running"
	case FrontierCompactionJobCompleted:
		return "completed"
	case FrontierCompactionJobFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// FrontierCompactionJob is the durable identity and lifecycle state for one
// caller-owned compaction task. Boundary has the same meaning as Submit's
// boundary: history strictly before it is eligible for removal.
type FrontierCompactionJob struct {
	ID         uint64
	FrontierID string
	Boundary   uint64
	State      FrontierCompactionJobState
}

// FrontierCompactionJobLedgerOptions bounds one in-memory and durable job
// catalog. Zero selects DefaultFrontierCompactionJobLimit.
type FrontierCompactionJobLedgerOptions struct {
	MaxJobs int
}

// FrontierCompactionJobLedger tracks bounded compaction work independently of
// the worker scheduler. Callers can persist it before scheduling and recover
// pending work after a process restart. The ledger is safe for concurrent
// callers; compaction tasks remain caller-owned.
type FrontierCompactionJobLedger struct {
	mu      sync.Mutex
	maxJobs int
	nextID  uint64
	jobs    []FrontierCompactionJob
	indexes map[uint64]int
}

// NewFrontierCompactionJobLedger creates an empty bounded job catalog.
func NewFrontierCompactionJobLedger(options FrontierCompactionJobLedgerOptions) (*FrontierCompactionJobLedger, error) {
	maxJobs := options.MaxJobs
	if maxJobs == 0 {
		maxJobs = DefaultFrontierCompactionJobLimit
	}
	if maxJobs < 1 || maxJobs > maxFrontierCompactionJobLimit {
		return nil, ErrFrontierCompactionJobLedgerOptionsInvalid
	}
	return &FrontierCompactionJobLedger{
		maxJobs: maxJobs,
		indexes: make(map[uint64]int),
	}, nil
}

// Enqueue records a pending compaction job and assigns a monotone ID.
func (ledger *FrontierCompactionJobLedger) Enqueue(frontierID string, boundary uint64) (FrontierCompactionJob, error) {
	if ledger == nil {
		return FrontierCompactionJob{}, ErrFrontierCompactionJobLedgerNil
	}
	frontierID = strings.TrimSpace(frontierID)
	if frontierID == "" {
		return FrontierCompactionJob{}, ErrFrontierCompactionJobFrontierRequired
	}
	if len(frontierID) > maxFrontierCompactionJobIDBytes || strings.IndexByte(frontierID, 0) >= 0 {
		return FrontierCompactionJob{}, ErrFrontierCompactionJobFrontierInvalid
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if len(ledger.jobs) >= ledger.maxJobs {
		return FrontierCompactionJob{}, ErrFrontierCompactionJobLimit
	}
	if ledger.nextID == ^uint64(0) {
		return FrontierCompactionJob{}, ErrFrontierCompactionJobLimit
	}
	jobID := ledger.nextID + 1
	ledger.nextID = jobID
	job := FrontierCompactionJob{
		ID:         jobID,
		FrontierID: frontierID,
		Boundary:   boundary,
		State:      FrontierCompactionJobPending,
	}
	ledger.indexes[job.ID] = len(ledger.jobs)
	ledger.jobs = append(ledger.jobs, job)
	return job, nil
}

// Job returns a detached copy of one job.
func (ledger *FrontierCompactionJobLedger) Job(id uint64) (FrontierCompactionJob, bool) {
	if ledger == nil || id == 0 {
		return FrontierCompactionJob{}, false
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	index, ok := ledger.indexes[id]
	if !ok {
		return FrontierCompactionJob{}, false
	}
	return ledger.jobs[index], true
}

// Claim moves a pending job into the running state.
func (ledger *FrontierCompactionJobLedger) Claim(id uint64) error {
	return ledger.transition(id, FrontierCompactionJobPending, FrontierCompactionJobRunning)
}

// Complete marks a running job as successfully completed.
func (ledger *FrontierCompactionJobLedger) Complete(id uint64) error {
	return ledger.transition(id, FrontierCompactionJobRunning, FrontierCompactionJobCompleted)
}

// Fail marks a running job as failed. Failed jobs remain available for
// operator inspection until PruneCompletedThrough removes them.
func (ledger *FrontierCompactionJobLedger) Fail(id uint64) error {
	return ledger.transition(id, FrontierCompactionJobRunning, FrontierCompactionJobFailed)
}

func (ledger *FrontierCompactionJobLedger) transition(id uint64, from, to FrontierCompactionJobState) error {
	if ledger == nil {
		return ErrFrontierCompactionJobLedgerNil
	}
	if id == 0 {
		return ErrFrontierCompactionJobNotFound
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	index, ok := ledger.indexes[id]
	if !ok {
		return ErrFrontierCompactionJobNotFound
	}
	if ledger.jobs[index].State != from {
		return ErrFrontierCompactionJobTransition
	}
	ledger.jobs[index].State = to
	return nil
}

// Recoverable returns pending and running jobs in ID order. A restored
// snapshot normalizes running jobs to pending because their task outcome was
// not durably known at the time of the crash.
func (ledger *FrontierCompactionJobLedger) Recoverable() []FrontierCompactionJob {
	if ledger == nil {
		return nil
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	jobs := make([]FrontierCompactionJob, 0, len(ledger.jobs))
	for _, job := range ledger.jobs {
		if job.State == FrontierCompactionJobPending || job.State == FrontierCompactionJobRunning {
			jobs = append(jobs, job)
		}
	}
	return jobs
}

// Snapshot returns all jobs in deterministic ID order.
func (ledger *FrontierCompactionJobLedger) Snapshot() []FrontierCompactionJob {
	if ledger == nil {
		return nil
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return append([]FrontierCompactionJob(nil), ledger.jobs...)
}

// PruneCompletedThrough removes completed and failed jobs whose IDs are at or
// below through. Pending and running jobs are never removed.
func (ledger *FrontierCompactionJobLedger) PruneCompletedThrough(through uint64) int {
	if ledger == nil || through == 0 {
		return 0
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	removed := 0
	write := 0
	for _, job := range ledger.jobs {
		if job.ID <= through && (job.State == FrontierCompactionJobCompleted || job.State == FrontierCompactionJobFailed) {
			delete(ledger.indexes, job.ID)
			removed++
			continue
		}
		ledger.jobs[write] = job
		ledger.indexes[job.ID] = write
		write++
	}
	for index := write; index < len(ledger.jobs); index++ {
		ledger.jobs[index] = FrontierCompactionJob{}
	}
	ledger.jobs = ledger.jobs[:write]
	return removed
}

// MarshalSnapshot encodes a deterministic CRC-protected HCJ1 snapshot.
func (ledger *FrontierCompactionJobLedger) MarshalSnapshot() ([]byte, error) {
	if ledger == nil {
		return nil, ErrFrontierCompactionJobLedgerNil
	}
	ledger.mu.Lock()
	nextID := ledger.nextID
	jobs := append([]FrontierCompactionJob(nil), ledger.jobs...)
	ledger.mu.Unlock()
	return encodeFrontierCompactionJobSnapshot(nextID, jobs)
}

// RestoreSnapshot restores a validated snapshot into an empty ledger. Running
// jobs are reset to pending so callers can safely resubmit them after restart.
func (ledger *FrontierCompactionJobLedger) RestoreSnapshot(payload []byte) error {
	if ledger == nil {
		return ErrFrontierCompactionJobLedgerNil
	}
	ledger.mu.Lock()
	maxJobs := ledger.maxJobs
	if len(ledger.jobs) != 0 {
		ledger.mu.Unlock()
		return ErrFrontierCompactionJobSnapshotNotEmpty
	}
	ledger.mu.Unlock()

	nextID, jobs, err := decodeFrontierCompactionJobSnapshot(payload, maxJobs)
	if err != nil {
		return err
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if len(ledger.jobs) != 0 {
		return ErrFrontierCompactionJobSnapshotNotEmpty
	}
	ledger.nextID = nextID
	ledger.jobs = append(ledger.jobs[:0], jobs...)
	ledger.indexes = make(map[uint64]int, len(jobs))
	for index, job := range jobs {
		ledger.indexes[job.ID] = index
	}
	return nil
}

// SaveDurableSnapshot writes the current job catalog through an existing
// atomic frontier snapshot store. Callers should save after each lifecycle
// transition when crash recovery must not lose the latest state.
func (ledger *FrontierCompactionJobLedger) SaveDurableSnapshot(ctx context.Context, store FrontierSnapshotStore) error {
	if ledger == nil {
		return ErrFrontierCompactionJobLedgerNil
	}
	if store == nil {
		return ErrFrontierCompactionJobStoreRequired
	}
	if err := frontierSnapshotContextErr(ctx); err != nil {
		return err
	}
	payload, err := ledger.MarshalSnapshot()
	if err != nil {
		return err
	}
	if err := frontierSnapshotContextErr(ctx); err != nil {
		return err
	}
	return store.Save(ctx, payload)
}

// RestoreDurableSnapshot loads one job catalog. It returns found=false when
// the store has no snapshot.
func (ledger *FrontierCompactionJobLedger) RestoreDurableSnapshot(ctx context.Context, store FrontierSnapshotStore) (bool, error) {
	if ledger == nil {
		return false, ErrFrontierCompactionJobLedgerNil
	}
	if store == nil {
		return false, ErrFrontierCompactionJobStoreRequired
	}
	if err := frontierSnapshotContextErr(ctx); err != nil {
		return false, err
	}
	payload, err := store.Load(ctx)
	if err != nil {
		return false, err
	}
	if payload == nil {
		return false, nil
	}
	if err := ledger.RestoreSnapshot(payload); err != nil {
		return false, err
	}
	return true, nil
}

func encodeFrontierCompactionJobSnapshot(nextID uint64, jobs []FrontierCompactionJob) ([]byte, error) {
	if len(jobs) > maxFrontierCompactionJobLimit {
		return nil, ErrFrontierCompactionJobSnapshotInvalid
	}
	payload := make([]byte, 0, frontierCompactionJobHeaderBytes+binary.MaxVarintLen64*2+len(jobs)*32+frontierCompactionJobChecksumBytes)
	payload = append(payload, frontierCompactionJobMagic[:]...)
	payload = append(payload, 1)
	payload = appendFrontierCompactionJobUvarint(payload, nextID)
	payload = appendFrontierCompactionJobUvarint(payload, uint64(len(jobs)))
	previousID := uint64(0)
	for _, job := range jobs {
		if !validFrontierCompactionJob(job) || job.ID <= previousID || job.ID > nextID {
			return nil, ErrFrontierCompactionJobSnapshotInvalid
		}
		previousID = job.ID
		payload = appendFrontierCompactionJobUvarint(payload, job.ID)
		payload = append(payload, byte(job.State))
		payload = appendFrontierCompactionJobUvarint(payload, job.Boundary)
		payload = appendFrontierCompactionJobUvarint(payload, uint64(len(job.FrontierID)))
		payload = append(payload, job.FrontierID...)
		if len(payload)+frontierCompactionJobChecksumBytes > maxFrontierCompactionJobBytes {
			return nil, ErrFrontierCompactionJobSnapshotInvalid
		}
	}
	var checksum [frontierCompactionJobChecksumBytes]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.Checksum(payload, frontierCompactionJobCRCTable))
	payload = append(payload, checksum[:]...)
	return payload, nil
}

func decodeFrontierCompactionJobSnapshot(payload []byte, maxJobs int) (uint64, []FrontierCompactionJob, error) {
	minimum := frontierCompactionJobHeaderBytes + 1 + 1 + frontierCompactionJobChecksumBytes
	if len(payload) < minimum || len(payload) > maxFrontierCompactionJobBytes || !bytes.Equal(payload[:4], frontierCompactionJobMagic[:]) || payload[4] != 1 {
		return 0, nil, ErrFrontierCompactionJobSnapshotInvalid
	}
	bodyEnd := len(payload) - frontierCompactionJobChecksumBytes
	if crc32.Checksum(payload[:bodyEnd], frontierCompactionJobCRCTable) != binary.BigEndian.Uint32(payload[bodyEnd:]) {
		return 0, nil, ErrFrontierCompactionJobSnapshotInvalid
	}
	offset := frontierCompactionJobHeaderBytes
	nextID, size := readFrontierCompactionJobUvarint(payload[offset:bodyEnd])
	if size <= 0 {
		return 0, nil, ErrFrontierCompactionJobSnapshotInvalid
	}
	offset += size
	count, size := readFrontierCompactionJobUvarint(payload[offset:bodyEnd])
	if size <= 0 || count > uint64(maxFrontierCompactionJobLimit) || count > uint64(maxJobs) {
		return 0, nil, ErrFrontierCompactionJobSnapshotInvalid
	}
	offset += size
	jobs := make([]FrontierCompactionJob, 0, int(count))
	previousID := uint64(0)
	for index := uint64(0); index < count; index++ {
		id, size := readFrontierCompactionJobUvarint(payload[offset:bodyEnd])
		if size <= 0 || id <= previousID || id > nextID {
			return 0, nil, ErrFrontierCompactionJobSnapshotInvalid
		}
		offset += size
		if offset >= bodyEnd {
			return 0, nil, ErrFrontierCompactionJobSnapshotInvalid
		}
		state := FrontierCompactionJobState(payload[offset])
		offset++
		boundary, size := readFrontierCompactionJobUvarint(payload[offset:bodyEnd])
		if size <= 0 {
			return 0, nil, ErrFrontierCompactionJobSnapshotInvalid
		}
		offset += size
		frontierLength, size := readFrontierCompactionJobUvarint(payload[offset:bodyEnd])
		if size <= 0 || frontierLength == 0 || frontierLength > maxFrontierCompactionJobIDBytes || frontierLength > uint64(bodyEnd-offset-size) {
			return 0, nil, ErrFrontierCompactionJobSnapshotInvalid
		}
		offset += size
		frontierEnd := offset + int(frontierLength)
		job := FrontierCompactionJob{
			ID:         id,
			FrontierID: string(payload[offset:frontierEnd]),
			Boundary:   boundary,
			State:      state,
		}
		if state == FrontierCompactionJobRunning {
			job.State = FrontierCompactionJobPending
		}
		if !validFrontierCompactionJob(job) {
			return 0, nil, ErrFrontierCompactionJobSnapshotInvalid
		}
		jobs = append(jobs, job)
		previousID = id
		offset = frontierEnd
	}
	if offset != bodyEnd || (count > 0 && nextID == 0) {
		return 0, nil, ErrFrontierCompactionJobSnapshotInvalid
	}
	return nextID, jobs, nil
}

func validFrontierCompactionJob(job FrontierCompactionJob) bool {
	return job.ID != 0 && job.FrontierID != "" && len(job.FrontierID) <= maxFrontierCompactionJobIDBytes && strings.IndexByte(job.FrontierID, 0) < 0 && job.State >= FrontierCompactionJobPending && job.State <= FrontierCompactionJobFailed
}

func appendFrontierCompactionJobUvarint(payload []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	size := binary.PutUvarint(encoded[:], value)
	return append(payload, encoded[:size]...)
}

func readFrontierCompactionJobUvarint(payload []byte) (uint64, int) {
	if len(payload) == 0 {
		return 0, 0
	}
	return binary.Uvarint(payload)
}
