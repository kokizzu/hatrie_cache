//go:build !linux

package hatSql

import "time"

func currentSQLThreadCPUTime() (int64, time.Duration, error) {
	return 0, 0, ErrSQLCPUTimeUnsupported
}
