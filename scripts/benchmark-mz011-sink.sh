#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache \
	-run '^$' \
	-bench '^BenchmarkMZ011(BaselineSubscriptionBatch100|CommandJournalSinkBatch100|CommandJournalSinkBatch100WithCheckpoint)$' \
	-benchmem \
	-count=5
