#!/usr/bin/env bash
set -eu

go test ./hat/hatCache -run '^$' -bench 'BenchmarkCommandJournalAsyncSubmission/(sync_execute|async_submit_wait|async_admission)$' -benchmem -benchtime=1000x -count=5
