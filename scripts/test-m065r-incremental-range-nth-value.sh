#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM065r' -count=1
