#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkExecuteSQLMutation' -benchmem -count=5
