#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestMZ020' -race -count=1
