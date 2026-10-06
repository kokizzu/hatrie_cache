package hatSql

import "runtime"

type sqlQueryAllocationSnapshot struct {
	bytes   uint64
	objects uint64
}

// readSQLQueryAllocationSnapshot reads monotonic process counters. It is
// called only for explicitly profiled observations because ReadMemStats is
// intentionally not part of ordinary query execution.
func readSQLQueryAllocationSnapshot() sqlQueryAllocationSnapshot {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return sqlQueryAllocationSnapshot{
		bytes:   stats.TotalAlloc,
		objects: stats.Mallocs,
	}
}

func sqlQueryAllocationDelta(end, start uint64) uint64 {
	if end < start {
		return 0
	}
	return end - start
}
