#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run 'TestC213IncrementalTopK' -count=1
