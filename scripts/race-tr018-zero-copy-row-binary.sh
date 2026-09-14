#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestTR018' -count=1
