#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'Test(CHU59|CHU60|Columnar|SQLColumnar|PackedNumeric)' -count=1
