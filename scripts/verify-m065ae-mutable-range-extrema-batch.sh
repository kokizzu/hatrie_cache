#!/usr/bin/env bash
set -eu

bash ./scripts/test-m065ae-mutable-range-extrema-batch.sh
go test -race ./hat/hatSql -run '^TestMutableIncrementalRangeWindow' -count=1
go vet ./hat/hatSql
