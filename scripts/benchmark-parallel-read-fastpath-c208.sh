#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench '^BenchmarkExecuteParallelReplicaReadFastPathC208$' -benchmem -count=3
