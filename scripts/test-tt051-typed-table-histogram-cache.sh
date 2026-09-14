#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestTypedTableHistogram' -count=1
