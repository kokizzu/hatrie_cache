#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^(Test(Negate|Except)DifferentialRows|ExampleExceptDifferentialRows$)' -count=1
go vet ./hat/hatSql
git diff --check
