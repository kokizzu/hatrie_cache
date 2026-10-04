#!/bin/sh
set -eu

go test ./hat/hatReplication -run '^$' -bench '^BenchmarkConflictDecision' -benchmem -count=5 "$@"
