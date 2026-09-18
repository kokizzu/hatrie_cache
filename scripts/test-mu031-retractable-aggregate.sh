#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run 'TestMU031' -count=1
