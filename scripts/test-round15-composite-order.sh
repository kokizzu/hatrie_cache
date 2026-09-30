#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestRound15' -count=1
