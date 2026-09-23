#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestM212' -count=1
