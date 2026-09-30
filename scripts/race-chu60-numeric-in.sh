#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestCHU60NumericIN' -count=1
