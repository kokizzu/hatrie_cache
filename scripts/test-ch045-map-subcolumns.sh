#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestCH0(30|45)' -count=1
