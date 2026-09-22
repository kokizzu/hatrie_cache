package hatSql

import (
	"errors"
	"fmt"
	"time"
)

// ErrSQLQueryCPUTimeExceeded identifies cooperative CPU-time budget rejection.
var ErrSQLQueryCPUTimeExceeded = errors.New("SQL query CPU time budget exceeded")

const sqlCPUTimeCheckEvery uint64 = 256

func (control *sqlExecutionControl) checkCPUTime() error {
	if control == nil || control.cpuTimeBudget <= 0 {
		return nil
	}
	if control.cpuTimeChecks.Add(1)%sqlCPUTimeCheckEvery != 0 {
		return nil
	}
	elapsed := time.Since(control.cpuWallStart)
	if control.cpuTimeThread {
		if current, ok := sqlCurrentThreadCPUTime(); ok && current >= control.cpuTimeStart {
			elapsed = current - control.cpuTimeStart
		}
	}
	if elapsed < control.cpuTimeBudget {
		return nil
	}
	return fmt.Errorf("%w: elapsed %s, maximum %s", ErrSQLQueryCPUTimeExceeded, elapsed, control.cpuTimeBudget)
}
