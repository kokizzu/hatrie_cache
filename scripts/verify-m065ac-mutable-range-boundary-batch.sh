#!/usr/bin/env bash
set -eu

bash ./scripts/test-m065ac-mutable-range-boundary-batch.sh
go test -race ./hat/hatSql -run '^TestMutableIncrementalRangeBoundaryWindow' -count=1
go vet ./hat/hatSql
