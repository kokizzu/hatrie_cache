#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCH009TTLRollup' -count=1 -v
