#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestRound15' -count=1
