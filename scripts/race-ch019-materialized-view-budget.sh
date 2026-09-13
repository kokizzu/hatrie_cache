#!/usr/bin/env bash
set -euo pipefail
go test -race ./hat/hatSql -run '^TestCH019MaterializedViews' -count=1
