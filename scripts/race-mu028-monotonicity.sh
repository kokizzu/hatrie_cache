#!/usr/bin/env bash
set -euo pipefail
go test -race ./hat/hatSql -run 'TestMU028' -count=1
