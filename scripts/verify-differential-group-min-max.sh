#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestGroupMinMaxInt64DifferentialRows' -count=1
go vet ./hat/hatSql
git diff --check
