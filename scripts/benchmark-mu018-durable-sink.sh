#!/usr/bin/env bash
set -euo pipefail

GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkMU018AfterExactlyOnceLedger(File|Memory)Checkpoint$' -benchmem -benchtime=100ms -count=3
