#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestMutableIncrementalRangeWindow' -count=1
go vet ./hat/hatSql
