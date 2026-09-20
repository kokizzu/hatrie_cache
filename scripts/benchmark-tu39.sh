#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkTU39(RawJournalRecordNext|SpaceChangefeedNext)$' -benchmem -benchtime=1s -count=5
