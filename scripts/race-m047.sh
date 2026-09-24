#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestM047' -count=1
