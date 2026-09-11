#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^(TestDifferentialIntersect|ExampleDifferentialIntersect$)' -count=1
go vet ./hat/hatSql
git diff --check
