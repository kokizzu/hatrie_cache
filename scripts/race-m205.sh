#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -race -count=1
