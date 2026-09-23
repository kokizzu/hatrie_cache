#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestM219' -race -count=1
