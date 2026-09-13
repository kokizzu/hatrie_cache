#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run 'TestTDigest' -count=1
go test ./hat/hatSql -run 'TestSQLTDigestPercentile' -count=1
