#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestTU26' -count=1
