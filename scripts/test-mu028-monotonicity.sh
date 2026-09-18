#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run 'TestMU028' -count=1
