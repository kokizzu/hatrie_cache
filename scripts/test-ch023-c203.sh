#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCH023' -count=1
