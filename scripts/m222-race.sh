#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestM222' -count=1
