#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestMZ020' -count=1
