#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench 'BenchmarkSelectReadReplicaC213' -benchmem -count="${BENCH_COUNT:-5}"
