#!/usr/bin/env bash
set -euo pipefail
go test -race ./hat/hatSql -run '^TestMZ034' -count=1
