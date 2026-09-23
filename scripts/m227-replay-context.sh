#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Journal replay code:'
sed -n '230,285p' hat/hatCache/journal.go
sed -n '1325,1470p' hat/hatCache/journal.go

printf '\n%s\n' 'Replay tests and benchmarks:'
rg -n 'ReplayWithProgress|\.Replay\(|Benchmark.*Replay|ReplayProgress' hat/hatCache/*test.go hat/hatCache/*benchmark_test.go || true
