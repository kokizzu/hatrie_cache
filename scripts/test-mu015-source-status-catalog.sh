#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^TestMU015Catalog' -count=1
