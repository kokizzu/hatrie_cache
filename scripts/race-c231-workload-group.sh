#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^(TestC231|TestCHU39)' -count=1
