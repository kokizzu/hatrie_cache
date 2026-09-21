#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestCH235' -count=1
