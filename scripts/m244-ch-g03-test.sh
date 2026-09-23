#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestCH003' -count=1
