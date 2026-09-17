#!/bin/sh
set -eu
go test ./hat/hatSql -run '^$' -bench 'BenchmarkCH017ProjectionAdvisor$' -benchmem -benchtime=200ms -count=5 -v
