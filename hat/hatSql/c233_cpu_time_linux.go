//go:build linux

package hatSql

import (
	"syscall"
	"time"
)

func sqlCurrentThreadCPUTime() (time.Duration, bool) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_THREAD, &usage); err != nil {
		return 0, false
	}
	return sqlCPUTimevalDuration(usage.Utime) + sqlCPUTimevalDuration(usage.Stime), true
}

func sqlCPUTimevalDuration(value syscall.Timeval) time.Duration {
	return time.Duration(value.Sec)*time.Second + time.Duration(value.Usec)*time.Microsecond
}
