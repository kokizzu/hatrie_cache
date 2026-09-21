#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCH235' -count=1
