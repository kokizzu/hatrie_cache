#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestCHG42' -count=1
