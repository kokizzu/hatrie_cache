package hatMonitoring

import (
	"runtime"
	"runtime/metrics"
	"sync"
	"time"
)

// SchedulerReport is an on-demand snapshot of Go scheduler state. It does
// not start a sampler or retain scheduler history.
type SchedulerReport struct {
	CollectedAt time.Time `json:"collected_at"`

	Goroutines uint64 `json:"goroutines"`
	GOMAXPROCS uint64 `json:"gomaxprocs"`
	NumCPU     uint64 `json:"num_cpu"`

	SchedulerMetricGoroutines uint64 `json:"scheduler_metric_goroutines"`
	SchedulerMetricGOMAXPROCS uint64 `json:"scheduler_metric_gomaxprocs"`
	SchedulerLatencySamples   uint64 `json:"scheduler_latency_samples"`
}

const (
	schedulerMetricGoroutines = iota
	schedulerMetricGOMAXPROCS
	schedulerMetricLatencies
)

var schedulerMetricNames = [...]string{
	"/sched/goroutines:goroutines",
	"/sched/gomaxprocs:threads",
	"/sched/latencies:seconds",
}

var schedulerMetricSamplesPool = sync.Pool{
	New: func() interface{} {
		return new([len(schedulerMetricNames)]metrics.Sample)
	},
}

// ReadSchedulerReport reads a bounded runtime scheduler snapshot. It does not
// trigger GC, change runtime settings, or retain process data.
func ReadSchedulerReport() SchedulerReport {
	samples := schedulerMetricSamplesPool.Get().(*[len(schedulerMetricNames)]metrics.Sample)
	defer schedulerMetricSamplesPool.Put(samples)
	for index, name := range schedulerMetricNames {
		samples[index].Name = name
	}
	metrics.Read(samples[:])

	report := SchedulerReport{
		CollectedAt:               time.Now().UTC(),
		Goroutines:                uint64(runtime.NumGoroutine()),
		GOMAXPROCS:                uint64(runtime.GOMAXPROCS(0)),
		NumCPU:                    uint64(runtime.NumCPU()),
		SchedulerMetricGoroutines: schedulerMetricUint64(samples[schedulerMetricGoroutines]),
		SchedulerMetricGOMAXPROCS: schedulerMetricUint64(samples[schedulerMetricGOMAXPROCS]),
	}
	if samples[schedulerMetricLatencies].Value.Kind() == metrics.KindFloat64Histogram {
		histogram := samples[schedulerMetricLatencies].Value.Float64Histogram()
		for _, count := range histogram.Counts {
			report.SchedulerLatencySamples += count
		}
	}
	return report
}

func schedulerMetricUint64(sample metrics.Sample) uint64 {
	if sample.Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return sample.Value.Uint64()
}
