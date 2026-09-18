#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestMZ028' -race -count=1
