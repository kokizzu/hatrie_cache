#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM211' -count=1
