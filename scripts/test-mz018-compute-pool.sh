#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestSQLQueryManagerComputePool|TestSQLQueryManagerRejectsInvalidComputePoolOptions' -count=1
