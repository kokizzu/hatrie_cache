#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestTypedTableStats' -count=1
