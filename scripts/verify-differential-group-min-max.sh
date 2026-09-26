#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestGroupMinMax(String|Int64)DifferentialRows' -count=1
go vet ./hat/hatSql
git diff --check
