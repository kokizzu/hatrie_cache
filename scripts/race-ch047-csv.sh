#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestCH047ParallelCSV' -count=1
