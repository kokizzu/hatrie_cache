#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCH055' -count=1
