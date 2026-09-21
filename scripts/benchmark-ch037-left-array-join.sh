#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -list 'BenchmarkCH037'
go test -v ./hat/hatSql -run '^$' -bench '^BenchmarkCH037(ArrayJoinInner|LeftArrayJoinLegacy|LeftArrayJoin)$' -benchmem -count=5
