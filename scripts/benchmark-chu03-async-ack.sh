#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench 'BenchmarkMonitoringAsyncCommandHTTP(Admission|Wait)$' -benchmem -benchtime=100ms -count=3
