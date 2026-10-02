#!/usr/bin/env bash
set -euo pipefail
go test -race ./hat/hatSql -run '^TestMU036' -count=1
