#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Journal replay code:'
sed -n '230,285p' hat/hatCache/journal.go
sed -n '1325,1470p' hat/hatCache/journal.go

printf '\n%s\n' 'Replay tests and benchmarks:'
rg -n 'ReplayWithProgress|\.Replay\(|Benchmark.*Replay|ReplayProgress' hat/hatCache/*test.go hat/hatCache/*benchmark_test.go || true

printf '\n%s\n' 'Command request and replay dispatch definitions:'
request_files="$(rg -l '^type CacheCommandRequest|func executeCommandForReplay' hat/hatCache | sort)"
for file in $request_files; do
  printf '\n--- %s ---\n' "$file"
  rg -n '^type CacheCommandRequest|func \(.*\) ExecuteCommand|func executeCommandForReplay' "$file" || true
done
sed -n '1,220p' hat/hatCache/journal_replay_fastpath.go
sed -n '1,210p' hat/hatCache/command.go
