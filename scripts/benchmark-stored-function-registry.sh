#!/bin/sh
set -eu

go test ./hat/hatFunction -run '^$' -bench '^BenchmarkStoredFunction' -benchmem -count=5 "$@"
