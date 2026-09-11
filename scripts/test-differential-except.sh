#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(Test(Negate|Except)DifferentialRows|ExampleExceptDifferentialRows$)' -count=1
