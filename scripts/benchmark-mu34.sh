#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkMU34(RawJournalRecordNext|HistoricalSubscriptionNext|LegacyTailCheckpointCommit|ExactCheckpointCommit)$' -benchmem -benchtime=1s -count=5
