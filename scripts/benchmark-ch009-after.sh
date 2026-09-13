#!/usr/bin/env bash
set -eu

go test ./hat/hatCache -run '^$' -bench 'Benchmark(CommandJournalAsyncSubmission|CH009AsyncInsertBuffer)' -benchmem -benchtime=1000x -count=5
