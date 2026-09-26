#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestCH037ColumnarArrayJoin' -count=1
