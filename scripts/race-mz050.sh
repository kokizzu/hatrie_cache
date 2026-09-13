#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestMZ050' -count=1
