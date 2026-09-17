package hatCache

import (
	"sync"
	"testing"
	"time"
)

func tr050ReplicationJob(id uint64, payloadBytes int) replicationJob {
	return replicationJob{
		id:         id,
		enqueuedAt: time.Unix(1700000000+int64(id), 0).UTC(),
		result:     ReplicationResult{Key: "tr050-key"},
		tasks: []replicationTask{{
			target:       TopologyNode{ID: "node-b", Address: "http://node-b"},
			payloadBytes: payloadBytes,
		}},
	}
}

func tr050LegacyPrepareAsyncJobForQueue(replicator *HTTPReplicator, job replicationJob) bool {
	if replicator == nil || replicator.queue == nil {
		return false
	}
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	replicator.appendPendingAsyncJobLocked(job)
	if replicator.outboxRestoreBacklog || job.id <= replicator.outboxRestoreCursor {
		return true
	}
	return false
}

func TestTR050ReplicationByteBudgetAdmission(t *testing.T) {
	first := tr050ReplicationJob(1, 128)
	firstEstimate := replicationJobEstimatedBytes(first)
	replicator := &HTTPReplicator{
		queue:         make(chan replicationJob, 2),
		maxQueueBytes: firstEstimate,
	}

	restore, admitted := replicator.prepareAsyncJobForQueue(first)
	if restore || !admitted {
		t.Fatalf("first admission = restore %v, admitted %v; want admitted without restore", restore, admitted)
	}

	second := tr050ReplicationJob(2, 128)
	_, admitted = replicator.prepareAsyncJobForQueue(second)
	if admitted {
		t.Fatal("second admission succeeded after the byte budget was consumed")
	}

	replicator.mu.RLock()
	pending := len(replicator.pending)
	queuedBytes := replicator.queueStats.EstimatedQueuedBytes
	replicator.mu.RUnlock()
	if pending != 1 {
		t.Fatalf("pending jobs = %d, want 1", pending)
	}
	if queuedBytes != firstEstimate {
		t.Fatalf("estimated queued bytes = %d, want %d", queuedBytes, firstEstimate)
	}
}

func TestTR050ReplicationByteBudgetIncludesInFlightBytes(t *testing.T) {
	first := tr050ReplicationJob(1, 128)
	firstEstimate := replicationJobEstimatedBytes(first)
	replicator := &HTTPReplicator{
		maxQueueBytes: firstEstimate + 1,
		queueStats: ReplicationQueueStats{
			EstimatedQueuedBytes:   firstEstimate,
			EstimatedInFlightBytes: 1,
		},
	}

	replicator.mu.Lock()
	available := replicator.asyncQueueBytesAvailableLocked(1)
	replicator.mu.Unlock()
	if available {
		t.Fatal("admission ignored in-flight bytes")
	}
}

func TestTR050ReplicationByteBudgetConcurrentAdmission(t *testing.T) {
	job := tr050ReplicationJob(1, 128)
	estimate := replicationJobEstimatedBytes(job)
	const producers = 16
	replicator := &HTTPReplicator{
		queue:         make(chan replicationJob, producers),
		maxQueueBytes: estimate * 4,
	}

	admitted := make(chan bool, producers)
	var waitGroup sync.WaitGroup
	waitGroup.Add(producers)
	for index := 0; index < producers; index++ {
		go func(index int) {
			defer waitGroup.Done()
			candidate := tr050ReplicationJob(uint64(index+1), 128)
			_, ok := replicator.prepareAsyncJobForQueue(candidate)
			admitted <- ok
		}(index)
	}
	waitGroup.Wait()
	close(admitted)

	count := 0
	for ok := range admitted {
		if ok {
			count++
		}
	}
	if count != 4 {
		t.Fatalf("concurrently admitted jobs = %d, want 4", count)
	}
	replicator.mu.RLock()
	queuedBytes := replicator.queueStats.EstimatedQueuedBytes
	replicator.mu.RUnlock()
	if queuedBytes != estimate*4 {
		t.Fatalf("concurrent estimated queued bytes = %d, want %d", queuedBytes, estimate*4)
	}
}

func TestTR050ReplicationByteBudgetDropsOversizedNonDurableJob(t *testing.T) {
	job := tr050ReplicationJob(1, 128)
	replicator := &HTTPReplicator{
		queue:         make(chan replicationJob, 1),
		maxQueueBytes: replicationJobEstimatedBytes(job) - 1,
		done:          make(chan struct{}),
	}

	result := replicator.enqueueReplicationJob(job)
	if result.Queued || !result.Skipped {
		t.Fatalf("oversized non-durable result = %#v, want skipped", result)
	}
	if result.Reason != "replication queue byte budget is full" {
		t.Fatalf("oversized non-durable reason = %q", result.Reason)
	}
	replicator.mu.RLock()
	pending := len(replicator.pending)
	replicator.mu.RUnlock()
	if pending != 0 {
		t.Fatalf("pending jobs after rejected oversized job = %d, want 0", pending)
	}
}

func TestTR050ReplicationByteBudgetRetainsDurableJob(t *testing.T) {
	job := tr050ReplicationJob(1, 128)
	job.journalSeq = 1
	replicator := &HTTPReplicator{
		queue:         make(chan replicationJob, 1),
		maxQueueBytes: replicationJobEstimatedBytes(job) - 1,
		done:          make(chan struct{}),
	}

	result := replicator.enqueueReplicationJob(job)
	if !result.Queued || result.Skipped {
		t.Fatalf("oversized durable result = %#v, want retained queued result", result)
	}
	if result.Reason != "replication queue byte budget is full; job retained in durable journal backlog" {
		t.Fatalf("oversized durable reason = %q", result.Reason)
	}
	replicator.mu.RLock()
	backlog := replicator.outboxRestoreBacklog
	pending := len(replicator.pending)
	replicator.mu.RUnlock()
	if !backlog {
		t.Fatal("durable oversized job did not mark outbox backlog")
	}
	if pending != 0 {
		t.Fatalf("pending jobs for durable oversized job = %d, want 0", pending)
	}
}

func TestTR050ReplicationByteBudgetDefaultsOff(t *testing.T) {
	legacy := NewHTTPReplicator(HTTPReplicatorOptions{AsyncQueueSize: 1})
	defer legacy.Close()
	if legacy.maxQueueBytes != 0 {
		t.Fatalf("default max queue bytes = %d, want disabled", legacy.maxQueueBytes)
	}

	negative := NewHTTPReplicator(HTTPReplicatorOptions{
		AsyncQueueSize:     1,
		AsyncQueueMaxBytes: -1,
	})
	defer negative.Close()
	if negative.maxQueueBytes != 0 {
		t.Fatalf("negative max queue bytes = %d, want disabled", negative.maxQueueBytes)
	}

	configured := NewHTTPReplicator(HTTPReplicatorOptions{
		AsyncQueueSize:     1,
		AsyncQueueMaxBytes: 4096,
	})
	defer configured.Close()
	if configured.maxQueueBytes != 4096 {
		t.Fatalf("configured max queue bytes = %d, want 4096", configured.maxQueueBytes)
	}
	configured.mu.RLock()
	maxBytes := configured.queueStats.MaxBytes
	configured.mu.RUnlock()
	if maxBytes != 4096 {
		t.Fatalf("configured queue stats max bytes = %d, want 4096", maxBytes)
	}
}

func BenchmarkTR050ReplicationByteBudgetAdmission(b *testing.B) {
	job := tr050ReplicationJob(1, 128)
	estimate := replicationJobEstimatedBytes(job)
	for _, benchmark := range []struct {
		name    string
		maxByte uint64
		legacy  bool
	}{
		{name: "legacy-control", legacy: true},
		{name: "disabled", maxByte: 0},
		{name: "enabled", maxByte: estimate * 2},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			replicator := &HTTPReplicator{
				queue:         make(chan replicationJob, 1),
				maxQueueBytes: benchmark.maxByte,
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				job.id = uint64(index + 1)
				if benchmark.legacy {
					tr050LegacyPrepareAsyncJobForQueue(replicator, job)
				} else {
					_, admitted := replicator.prepareAsyncJobForQueue(job)
					if !admitted {
						b.Fatal("admission rejected a job after the previous job was removed")
					}
				}
				replicator.unreserveAsyncJob(job.id)
			}
			b.StopTimer()
			b.ReportMetric(float64(estimate), "estimated_job_bytes/op")
		})
	}
}

func BenchmarkTR050ReplicationByteBudgetBurst(b *testing.B) {
	const burst = 1024
	job := tr050ReplicationJob(1, 128)
	estimate := replicationJobEstimatedBytes(job)
	for _, benchmark := range []struct {
		name    string
		maxByte uint64
	}{
		{name: "disabled", maxByte: 0},
		{name: "enabled", maxByte: estimate * 8},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			replicator := &HTTPReplicator{
				queue:         make(chan replicationJob, burst),
				maxQueueBytes: benchmark.maxByte,
			}
			admitted := 0
			queuedBytes := uint64(0)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				replicator.mu.Lock()
				replicator.pending = replicator.pending[:0]
				replicator.queueStats.EstimatedQueuedBytes = 0
				replicator.queueStats.EstimatedInFlightBytes = 0
				replicator.mu.Unlock()

				admitted = 0
				for index := 0; index < burst; index++ {
					job.id = uint64(index + 1)
					_, ok := replicator.prepareAsyncJobForQueue(job)
					if !ok {
						break
					}
					admitted++
				}

				replicator.mu.RLock()
				queuedBytes = replicator.queueStats.EstimatedQueuedBytes
				replicator.mu.RUnlock()
			}
			b.StopTimer()
			b.ReportMetric(float64(admitted), "admitted_jobs/burst")
			b.ReportMetric(float64(queuedBytes), "estimated_queued_bytes/burst")
		})
	}
}
