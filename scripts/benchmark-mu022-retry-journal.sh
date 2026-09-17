#!/usr/bin/env bash
set -euo pipefail

exec env GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkMU022(BeforeSourceTransactionAdvance|AfterRetryJournalBeginComplete)$' -benchmem -benchtime=500ms -count=5
