#!/usr/bin/env bash
set -euo pipefail

printf 'MZ-033 backlog row:\n'
rg -n 'MZ-033' ENGINE_IDEAS.md
printf 'Existing timestamp/oracle files:\n'
rg -l 'TimestampOracle|timestamp oracle' hat/hatReplication --glob '*.go' --glob '!**/*_test.go'
printf 'Adopted entry:\n'
sed -n '375,400p' ADOPTED_QUERY_ENGINE_IDEAS.md
printf 'README link context:\n'
sed -n '4040,4055p' README.md
printf 'Benchmark context:\n'
sed -n '24610,24655p' BENCHMARK.md
printf 'Global benchmark source:\n'
sed -n '1,140p' hat/hatReplication/global_timestamp_oracle_benchmark_test.go
printf 'MZ-033 Makefile targets:\n'
rg -n -C 4 'mz033|MZ-033' Makefile
printf 'MZ-033 Makefile diff:\n'
git diff --unified=3 -- Makefile | rg -n -C 12 'mz033|MZ-033'
printf 'Worktree status:\n'
git status --short
printf 'Staged summary:\n'
git diff --cached --stat
printf 'Unstaged summary:\n'
git diff --stat
