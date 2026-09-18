#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestMZ024' -count=1
