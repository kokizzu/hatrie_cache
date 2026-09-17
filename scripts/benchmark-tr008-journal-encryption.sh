#!/bin/sh
set -eu

go test ./hat/hatJournal -run '^$' -bench 'BenchmarkTR008' -benchmem -benchtime=200ms -count=3
