#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH024ExplainSegmentSkip$' -benchmem -benchtime=1000x -count=5
