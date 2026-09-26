#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^Test(GroupMinMax(String|Int64)|GroupCountDistinctString)DifferentialRows' -count=1
go vet ./hat/hatSql
git diff --check
