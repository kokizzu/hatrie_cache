#!/usr/bin/env bash
set -eu

bash ./scripts/test-m065ad-mutable-range-nth-value-batch.sh
go test -race ./hat/hatSql -run '^TestMutableIncrementalRangeNthValueWindow' -count=1
go vet ./hat/hatSql
