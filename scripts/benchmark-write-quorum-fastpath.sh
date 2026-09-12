#!/bin/sh
set -eu

exec go test -run '^$' -bench '^BenchmarkExecuteWriteQuorum(ThreeTargets|UntilSatisfiedFastTargets|UntilSatisfiedSlowTarget|WaitAllWithSlowTarget)$' -benchmem -count=3 ./hat/hatReplication
