#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkT212ReplicaRetention(Modes|Baseline)$' -benchmem -benchtime=20x -count=3
