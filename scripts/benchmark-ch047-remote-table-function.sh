#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH047RemoteTableFunctionSelectiveRange$' -benchmem -benchtime=250ms -count=5
