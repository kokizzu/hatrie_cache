#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestCH0(30|45)' -count=1
