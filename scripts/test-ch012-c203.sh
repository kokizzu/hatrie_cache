#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestCH012' -count=1
