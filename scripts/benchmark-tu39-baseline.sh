#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkTU39RawJournalRecordNext$' -benchmem -benchtime=1s -count=5
