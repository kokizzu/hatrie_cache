#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench 'BenchmarkT047iClusterWriteCommit' -benchmem -benchtime="${BENCHTIME:-1s}" -count="${BENCHCOUNT:-3}"
