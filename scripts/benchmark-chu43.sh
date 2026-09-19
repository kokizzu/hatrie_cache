#!/bin/sh
set -eu
go test ./hat/hatSql -run '^$' -bench 'BenchmarkCHU43TDigest' -benchmem -count=5
