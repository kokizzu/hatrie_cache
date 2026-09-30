#!/usr/bin/env bash
set -euo pipefail

GOCACHE="$PWD/.gocache" go test ./hat/hatSql -run 'TestCHU61NullablePredicates' -count=1
