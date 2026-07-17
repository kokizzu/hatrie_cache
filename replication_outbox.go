package hatriecache

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type ReplicationOutboxStore struct {
	mu       sync.Mutex
	path     string
	snapshot replicationOutboxSnapshot
}

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
	Target  TopologyNode        `json:"target"`
	Payload CacheCommandRequest `json:"payload"`
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

func (store *ReplicationOutboxStore) jobs() []replicationJob {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
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
	return store.snapshot.DeadSeq, cloneReplicationDeadLetters(store.snapshot.DeadLetters)
}

func (store *ReplicationOutboxStore) putJob(job replicationJob) error {
	if store == nil || job.id == 0 {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	record := newReplicationOutboxJob(job)
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
	if store == nil || id == 0 {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for idx, job := range store.snapshot.Jobs {
		if job.ID != id {
			continue
		}
		copy(store.snapshot.Jobs[idx:], store.snapshot.Jobs[idx+1:])
		store.snapshot.Jobs[len(store.snapshot.Jobs)-1] = replicationOutboxJob{}
		store.snapshot.Jobs = store.snapshot.Jobs[:len(store.snapshot.Jobs)-1]
		return store.saveLocked()
	}
	return nil
}

func (store *ReplicationOutboxStore) setDeadLetters(deadSeq uint64, deadLetters []ReplicationDeadLetter) error {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.snapshot.DeadSeq = deadSeq
	store.snapshot.DeadLetters = cloneReplicationDeadLetters(deadLetters)
	return store.saveLocked()
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

func newReplicationOutboxJob(job replicationJob) replicationOutboxJob {
	tasks := make([]replicationOutboxTask, 0, len(job.tasks))
	for _, task := range job.tasks {
		tasks = append(tasks, replicationOutboxTask{
			Target:  task.target,
			Payload: task.payload,
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
	}
	return replicationJob{
		id:         record.ID,
		result:     cloneReplicationResult(record.Result),
		tasks:      tasks,
		enqueuedAt: record.EnqueuedAt,
	}
}
