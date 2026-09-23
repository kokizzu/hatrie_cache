#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestM218' -race -count=1
