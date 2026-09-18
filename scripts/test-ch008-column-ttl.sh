#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCH008' -count=1
