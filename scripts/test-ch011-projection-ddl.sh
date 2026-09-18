#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCH011' -count=1
