#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM250' -count=1
