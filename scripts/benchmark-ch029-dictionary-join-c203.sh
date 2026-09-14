#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH029DictionaryBackedJoin$' -benchmem -count=5
