#!/bin/sh
set -eu
exec go test -run '^$' -bench '^Benchmark(ConnectionPoolAcquireRelease|ConnectionDirectDialClose)$' -benchmem -count=5 ./hat/hatReplication
