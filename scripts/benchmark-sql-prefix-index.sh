#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLJSONLikePrefix$' -benchmem -count=5
