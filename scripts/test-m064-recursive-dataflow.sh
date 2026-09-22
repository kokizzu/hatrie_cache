#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run 'TestM064RecursiveDataflow' -count=1
