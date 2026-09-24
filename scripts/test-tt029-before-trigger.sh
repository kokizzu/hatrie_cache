#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestTT029' -count=1
