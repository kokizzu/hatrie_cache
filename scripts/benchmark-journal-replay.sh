#!/bin/sh
set -eu

exec go test -run '^$' -bench 'BenchmarkCommandJournalReplayProgress' -benchmem -count=3 ./hat/hatCache
