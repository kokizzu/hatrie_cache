#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestSQL(LeftArrayJoin|ArrayJoin)' -count=1
