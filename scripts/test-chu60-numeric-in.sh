#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestCHU60NumericIN' -count=1
