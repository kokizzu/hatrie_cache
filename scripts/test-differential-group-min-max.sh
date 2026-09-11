#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestGroupMinMaxInt64DifferentialRows' -count=1
