#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ045ExplainWorkload$' -benchmem -count=5
