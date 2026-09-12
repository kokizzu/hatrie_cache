#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench 'BenchmarkCH028' -benchmem -count=5
