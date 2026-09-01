#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLJSONLowerIndexEquality$' -benchmem -count=5
