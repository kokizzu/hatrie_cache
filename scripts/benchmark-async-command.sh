#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkCommandJournalAsyncSubmission$' -benchmem -count=5 -cpu=4
