#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH037(ArrayJoinInner|LeftArrayJoin|LeftArrayJoinLegacy|NestedArrayJoin)$' -benchmem -count=5
