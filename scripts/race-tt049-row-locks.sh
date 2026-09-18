#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestTT049' -count=1
