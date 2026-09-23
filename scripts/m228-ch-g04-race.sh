#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'Join' -count=1
