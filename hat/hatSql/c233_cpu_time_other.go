//go:build !linux

package hatSql

import "time"

func sqlCurrentThreadCPUTime() (time.Duration, bool) {
	return 0, false
}
