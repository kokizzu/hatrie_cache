#!/usr/bin/env bash
set -euo pipefail

GOMAXPROCS=1 go test ./hat/hatCache -run '^$' -bench '^Benchmark(CHU23AsyncCommandQueue|CommandJournalAsyncSubmission)' -benchmem -benchtime=1s -count=3 -cpu=1
