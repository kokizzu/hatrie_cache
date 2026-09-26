#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCH037ColumnarArrayJoin' -count=1
