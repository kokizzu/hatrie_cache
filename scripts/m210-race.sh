#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestM210' -count=1
