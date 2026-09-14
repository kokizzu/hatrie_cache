#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestCH030' -count=1
