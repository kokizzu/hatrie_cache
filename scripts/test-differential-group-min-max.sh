#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^Test(GroupMinMax(String|Int64)|GroupCountDistinctString)DifferentialRows' -count=1
