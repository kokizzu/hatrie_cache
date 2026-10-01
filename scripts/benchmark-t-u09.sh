#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench 'BenchmarkJoinBootstrap' -benchtime=10000x -benchmem -cpu=1 -count=10
