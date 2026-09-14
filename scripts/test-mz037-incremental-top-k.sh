#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestC213|TestIncrementalTopK)' -count=1
