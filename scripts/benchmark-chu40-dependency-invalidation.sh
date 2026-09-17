#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHU40ResultCacheDependencyModes$' -benchmem -count=5
