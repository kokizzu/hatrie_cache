package hatriecache

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type ReplicationOutboxStore struct {
	mu                sync.Mutex
	path              string
	snapshot          replicationOutboxSnapshot
	db                *leveldb.DB
	levelDB           bool
	codec             ReplicationOutboxCodec
	batchWindow       time.Duration
	writeLeader       bool
	writePending      []*replicationOutboxWriteRequest
	writeCond         *sync.Cond
	closing           bool
	levelDBSyncWrites uint64
}

type ReplicationOutboxCodec string

const (
	ReplicationOutboxCodecBinary ReplicationOutboxCodec = "binary"
	ReplicationOutboxCodecJSON   ReplicationOutboxCodec = "json"
)

const DefaultReplicationOutboxBatchWindow = time.Millisecond

type ReplicationOutboxOptions struct {
	Codec       ReplicationOutboxCodec
	BatchWindow time.Duration
}

type replicationOutboxWriteRequest struct {
	puts    []replicationOutboxKeyValue
	deletes [][]byte
	done    chan error
}

type replicationOutboxKeyValue struct {
	key   []byte
	value []byte
}

var errReplicationOutboxClosed = errors.New("hatriecache: replication outbox is closed")

var (
	replicationOutboxLevelDBJobPrefix      = []byte("job:")
	replicationOutboxLevelDBDeadSeqKey     = []byte("meta:dead-seq")
	replicationOutboxLevelDBDeadLettersKey = []byte("meta:dead-letters")
)

type replicationOutboxSnapshot struct {
	Jobs        []replicationOutboxJob  `json:"jobs,omitempty"`
	DeadSeq     uint64                  `json:"dead_seq,omitempty"`
	DeadLetters []ReplicationDeadLetter `json:"dead_letters,omitempty"`
}

type replicationOutboxJob struct {
	ID         uint64                  `json:"id"`
	Result     ReplicationResult       `json:"result"`
	Tasks      []replicationOutboxTask `json:"tasks"`
	EnqueuedAt time.Time               `json:"enqueued_at"`
}

type replicationOutboxTask struct {
	Target      TopologyNode        `json:"target"`
	Payload     CacheCommandRequest `json:"payload"`
	BinaryValue []byte              `json:"binary_value,omitempty"`
}

func OpenReplicationOutbox(path string) (*ReplicationOutboxStore, error) {
	if path == "" {
		return nil, errors.New("hatriecache: replication outbox path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	store := &ReplicationOutboxStore{path: path}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return store, nil
	}
	if err := json.Unmarshal(data, &store.snapshot); err != nil {
		return nil, err
	}
	return store, nil
}

func OpenLevelDBReplicationOutbox(path string) (*ReplicationOutboxStore, error) {
	return OpenLevelDBReplicationOutboxWithOptions(path, ReplicationOutboxOptions{
		Codec:       ReplicationOutboxCodecBinary,
		BatchWindow: DefaultReplicationOutboxBatchWindow,
	})
}

func OpenLevelDBReplicationOutboxWithOptions(path string, options ReplicationOutboxOptions) (*ReplicationOutboxStore, error) {
	if path == "" {
		return nil, errors.New("hatriecache: replication outbox path is required")
	}
	codec, err := ParseReplicationOutboxCodec(string(options.Codec))
	if err != nil {
		return nil, err
	}
	if options.BatchWindow < 0 {
		return nil, errors.New("hatriecache: replication outbox batch window cannot be negative")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	db, err := leveldb.OpenFile(path, &opt.Options{
		Compression: opt.SnappyCompression,
	})
	if err != nil {
		return nil, err
	}
	store := &ReplicationOutboxStore{
		path:        path,
		db:          db,
		levelDB:     true,
		codec:       codec,
		batchWindow: options.BatchWindow,
	}
	store.writeCond = sync.NewCond(&store.mu)
	return store, nil
}

func ParseReplicationOutboxCodec(value string) (ReplicationOutboxCodec, error) {
	switch ReplicationOutboxCodec(strings.ToLower(strings.TrimSpace(value))) {
	case "", ReplicationOutboxCodecBinary:
		return ReplicationOutboxCodecBinary, nil
	case ReplicationOutboxCodecJSON:
		return ReplicationOutboxCodecJSON, nil
	default:
		return "", fmt.Errorf("hatriecache: unsupported replication outbox codec %q", value)
	}
}

func (store *ReplicationOutboxStore) Close() error {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if !store.levelDB || store.db == nil {
		return nil
	}
	store.closing = true
	for store.writeLeader {
		store.writeCond.Wait()
	}
	db := store.db
	store.db = nil
	return db.Close()
}

func (store *ReplicationOutboxStore) jobs() []replicationJob {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.levelDB {
		return store.levelDBJobsLocked()
	}
	jobs := make([]replicationJob, 0, len(store.snapshot.Jobs))
	for _, record := range store.snapshot.Jobs {
		jobs = append(jobs, record.replicationJob())
	}
	return jobs
}

func (store *ReplicationOutboxStore) deadLetters() (uint64, []ReplicationDeadLetter) {
	if store == nil {
		return 0, nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.levelDB {
		return store.levelDBDeadLettersLocked()
	}
	return store.snapshot.DeadSeq, cloneReplicationDeadLetters(store.snapshot.DeadLetters)
}

func (store *ReplicationOutboxStore) putJob(job replicationJob) error {
	if store == nil || job.id == 0 {
		return nil
	}
	record := newReplicationOutboxJob(job)
	if store.levelDB {
		data, err := store.marshalJob(record)
		if err != nil {
			return err
		}
		return store.writeLevelDB(replicationOutboxWriteRequest{
			puts: []replicationOutboxKeyValue{{key: replicationOutboxLevelDBJobKey(job.id), value: data}},
		})
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for idx, existing := range store.snapshot.Jobs {
		if existing.ID == job.id {
			store.snapshot.Jobs[idx] = record
			return store.saveLocked()
		}
	}
	store.snapshot.Jobs = append(store.snapshot.Jobs, record)
	return store.saveLocked()
}

func (store *ReplicationOutboxStore) deleteJob(id uint64) error {
	return store.completeJob(id, 0, nil, false)
}

func (store *ReplicationOutboxStore) completeJob(id uint64, deadSeq uint64, deadLetters []ReplicationDeadLetter, updateDeadLetters bool) error {
	if store == nil || id == 0 {
		return nil
	}
	if store.levelDB {
		request := replicationOutboxWriteRequest{deletes: [][]byte{replicationOutboxLevelDBJobKey(id)}}
		if updateDeadLetters {
			deadSeqData, deadLettersData, err := store.marshalDeadLetters(deadSeq, deadLetters)
			if err != nil {
				return err
			}
			request.puts = []replicationOutboxKeyValue{
				{key: replicationOutboxLevelDBDeadSeqKey, value: deadSeqData},
				{key: replicationOutboxLevelDBDeadLettersKey, value: deadLettersData},
			}
		}
		return store.writeLevelDB(request)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	changed := updateDeadLetters
	for idx, job := range store.snapshot.Jobs {
		if job.ID != id {
			continue
		}
		copy(store.snapshot.Jobs[idx:], store.snapshot.Jobs[idx+1:])
		store.snapshot.Jobs[len(store.snapshot.Jobs)-1] = replicationOutboxJob{}
		store.snapshot.Jobs = store.snapshot.Jobs[:len(store.snapshot.Jobs)-1]
		changed = true
		break
	}
	if updateDeadLetters {
		store.snapshot.DeadSeq = deadSeq
		store.snapshot.DeadLetters = cloneReplicationDeadLetters(deadLetters)
	}
	if !changed {
		return nil
	}
	return store.saveLocked()
}

func (store *ReplicationOutboxStore) setDeadLetters(deadSeq uint64, deadLetters []ReplicationDeadLetter) error {
	if store == nil {
		return nil
	}
	if store.levelDB {
		deadSeqData, deadLettersData, err := store.marshalDeadLetters(deadSeq, deadLetters)
		if err != nil {
			return err
		}
		return store.writeLevelDB(replicationOutboxWriteRequest{
			puts: []replicationOutboxKeyValue{
				{key: replicationOutboxLevelDBDeadSeqKey, value: deadSeqData},
				{key: replicationOutboxLevelDBDeadLettersKey, value: deadLettersData},
			},
		})
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.snapshot.DeadSeq = deadSeq
	store.snapshot.DeadLetters = cloneReplicationDeadLetters(deadLetters)
	return store.saveLocked()
}

func (store *ReplicationOutboxStore) marshalJob(record replicationOutboxJob) ([]byte, error) {
	if store != nil && store.codec == ReplicationOutboxCodecJSON {
		return json.Marshal(record)
	}
	return marshalReplicationOutboxJobBinary(record)
}

func (store *ReplicationOutboxStore) marshalDeadLetters(deadSeq uint64, deadLetters []ReplicationDeadLetter) ([]byte, []byte, error) {
	deadLetters = cloneReplicationDeadLetters(deadLetters)
	if store != nil && store.codec == ReplicationOutboxCodecJSON {
		data, err := json.Marshal(deadLetters)
		return []byte(strconv.FormatUint(deadSeq, 10)), data, err
	}
	data, err := marshalReplicationOutboxDeadLettersBinary(deadLetters)
	return marshalReplicationOutboxDeadSeqBinary(deadSeq), data, err
}

func (store *ReplicationOutboxStore) writeLevelDB(request replicationOutboxWriteRequest) error {
	if store == nil {
		return nil
	}
	if store.batchWindow == 0 {
		store.mu.Lock()
		defer store.mu.Unlock()
		if !store.levelDB || store.db == nil || store.closing {
			return errReplicationOutboxClosed
		}
		batch := new(leveldb.Batch)
		appendReplicationOutboxWriteRequest(batch, &request)
		err := store.db.Write(batch, &opt.WriteOptions{Sync: true})
		store.levelDBSyncWrites++
		return err
	}
	request.done = make(chan error, 1)
	store.mu.Lock()
	if !store.levelDB || store.db == nil || store.closing {
		store.mu.Unlock()
		return errReplicationOutboxClosed
	}
	store.writePending = append(store.writePending, &request)
	leader := !store.writeLeader
	if leader {
		store.writeLeader = true
	}
	store.mu.Unlock()

	if leader {
		store.flushLevelDBWrites()
	}
	return <-request.done
}

func (store *ReplicationOutboxStore) flushLevelDBWrites() {
	if store == nil {
		return
	}
	if store.batchWindow > 0 {
		time.Sleep(store.batchWindow)
	}
	store.mu.Lock()
	requests := store.writePending
	store.writePending = nil
	batch := new(leveldb.Batch)
	for _, request := range requests {
		appendReplicationOutboxWriteRequest(batch, request)
	}
	err := errReplicationOutboxClosed
	if store.db != nil {
		err = store.db.Write(batch, &opt.WriteOptions{Sync: true})
		store.levelDBSyncWrites++
	}
	for _, request := range requests {
		request.done <- err
		close(request.done)
	}
	store.writeLeader = false
	if store.writeCond != nil {
		store.writeCond.Broadcast()
	}
	store.mu.Unlock()
}

func appendReplicationOutboxWriteRequest(batch *leveldb.Batch, request *replicationOutboxWriteRequest) {
	if batch == nil || request == nil {
		return
	}
	for _, put := range request.puts {
		batch.Put(put.key, put.value)
	}
	for _, key := range request.deletes {
		batch.Delete(key)
	}
}

func (store *ReplicationOutboxStore) levelDBSyncWriteCount() uint64 {
	if store == nil {
		return 0
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.levelDBSyncWrites
}

func (store *ReplicationOutboxStore) saveLocked() error {
	if store == nil {
		return nil
	}
	data, err := json.Marshal(store.snapshot)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	dir := filepath.Dir(store.path)
	tmp, err := os.CreateTemp(dir, ".replication-outbox-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, store.path); err != nil {
		return err
	}
	removeTmp = false
	return syncDir(dir)
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (store *ReplicationOutboxStore) levelDBJobsLocked() []replicationJob {
	if store == nil || store.db == nil {
		return nil
	}
	iter := store.db.NewIterator(util.BytesPrefix(replicationOutboxLevelDBJobPrefix), nil)
	defer iter.Release()
	var jobs []replicationJob
	for iter.Next() {
		record, err := unmarshalReplicationOutboxJob(iter.Value())
		if err != nil {
			continue
		}
		jobs = append(jobs, record.replicationJob())
	}
	return jobs
}

func (store *ReplicationOutboxStore) levelDBDeadLettersLocked() (uint64, []ReplicationDeadLetter) {
	if store == nil || store.db == nil {
		return 0, nil
	}
	var deadSeq uint64
	if data, err := store.db.Get(replicationOutboxLevelDBDeadSeqKey, nil); err == nil {
		deadSeq, _ = unmarshalReplicationOutboxDeadSeq(data)
	}
	var deadLetters []ReplicationDeadLetter
	if data, err := store.db.Get(replicationOutboxLevelDBDeadLettersKey, nil); err == nil {
		deadLetters, _ = unmarshalReplicationOutboxDeadLetters(data)
	}
	return deadSeq, cloneReplicationDeadLetters(deadLetters)
}

func replicationOutboxLevelDBJobKey(id uint64) []byte {
	key := make([]byte, len(replicationOutboxLevelDBJobPrefix)+8)
	copy(key, replicationOutboxLevelDBJobPrefix)
	binary.BigEndian.PutUint64(key[len(replicationOutboxLevelDBJobPrefix):], id)
	return key
}

func newReplicationOutboxJob(job replicationJob) replicationOutboxJob {
	tasks := make([]replicationOutboxTask, 0, len(job.tasks))
	for _, task := range job.tasks {
		tasks = append(tasks, replicationOutboxTask{
			Target:      task.target,
			Payload:     task.payload,
			BinaryValue: task.payload.BinaryValue,
		})
	}
	return replicationOutboxJob{
		ID:         job.id,
		Result:     cloneReplicationResult(job.result),
		Tasks:      tasks,
		EnqueuedAt: job.enqueuedAt,
	}
}

func (record replicationOutboxJob) replicationJob() replicationJob {
	tasks := make([]replicationTask, 0, len(record.Tasks))
	for _, task := range record.Tasks {
		tasks = append(tasks, replicationTask{
			target:  task.Target,
			payload: task.Payload,
		})
		tasks[len(tasks)-1].payload.BinaryValue = task.BinaryValue
	}
	return replicationJob{
		id:         record.ID,
		result:     cloneReplicationResult(record.Result),
		tasks:      tasks,
		enqueuedAt: record.EnqueuedAt,
	}
}
