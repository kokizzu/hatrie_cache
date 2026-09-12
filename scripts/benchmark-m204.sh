#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkCommandJournalSubscriptionNotify(Unbounded|Bounded)$' -benchtime=1s -benchmem -count=5
