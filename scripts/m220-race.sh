#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestM220' -race -count=1
