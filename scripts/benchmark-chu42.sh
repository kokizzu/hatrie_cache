#!/bin/sh
set -eu
go test ./hat/hatSql -run '^$' -bench 'BenchmarkCH042' -benchmem -count=5
