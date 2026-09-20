#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestM090b' -count=1
