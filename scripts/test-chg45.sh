#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCH045' -count=1
