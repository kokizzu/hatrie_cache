#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestCH038OrNull' -count=1
