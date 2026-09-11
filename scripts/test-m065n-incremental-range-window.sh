#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM065n' -count=1
