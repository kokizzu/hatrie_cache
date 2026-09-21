//go:build linux

package hatSql

import (
	"time"

	"golang.org/x/sys/unix"
)

func currentSQLThreadCPUTime() (int64, time.Duration, error) {
	var usage unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_THREAD, &usage); err != nil {
		return 0, 0, err
	}
	return int64(unix.Gettid()), sqlCPUTimevalDuration(usage.Utime) + sqlCPUTimevalDuration(usage.Stime), nil
}

func sqlCPUTimevalDuration(value unix.Timeval) time.Duration {
	seconds := time.Duration(value.Sec) * time.Second
	micros := time.Duration(value.Usec) * time.Microsecond
	if seconds > time.Duration(1<<63-1)-micros {
		return time.Duration(1<<63 - 1)
	}
	return seconds + micros
}
