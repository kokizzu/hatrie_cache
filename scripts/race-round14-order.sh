#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestRound14' -count=1
