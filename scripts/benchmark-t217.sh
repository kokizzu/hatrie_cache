#!/usr/bin/env bash
set -euo pipefail
trap 'make cleanup-hatrie-tmp-after-test' EXIT

go test ./hat/hatSql -run '^$' -bench '^BenchmarkT217TypedTable(RowUpsertBaseline|AppendColumnarBatch)$' -benchmem -benchtime=200ms -count=5
