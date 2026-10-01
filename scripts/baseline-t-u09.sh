#!/usr/bin/env bash
set -euo pipefail

cache_dir=".t-u09-baseline-go-cache-$$"
trap 'rm -rf "$cache_dir"' EXIT
export GOTOOLCHAIN=auto
export GOCACHE="$PWD/$cache_dir"

go test ./hat/hatReplication -run '^TestReplicaPromotionBarrier' -count=1
go test ./hat/hatReplication -run '^$' -bench '^BenchmarkChangefeedCheckpointOperations$' -benchmem -benchtime=250ms -count=5
