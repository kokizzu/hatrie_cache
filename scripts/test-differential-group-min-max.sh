#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestGroupMinMax(String|Int64)DifferentialRows' -count=1
