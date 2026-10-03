#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench '^BenchmarkTU047ClusterWriteCommit$' -benchmem -benchtime=200ms -count=5
go test ./hat/hatReplication -run '^$' -bench '^BenchmarkTU047DurableClusterWriteCommit$' -benchmem -benchtime=1x -count=5
