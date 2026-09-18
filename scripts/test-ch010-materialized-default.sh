#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCH010' -count=1
