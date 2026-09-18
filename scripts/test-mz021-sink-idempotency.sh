#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestMZ021' -count=1
