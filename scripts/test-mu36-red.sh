#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^TestMU036' -count=1
